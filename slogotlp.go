// Purpose: slogotlp is a slog.Handler that sends logs to an OpenTelemetry collector.
// Currently, only grpc is supported.
package slogotlp

import (
	"context"
	"encoding"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
	collectorLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/bakins/slogotlp/internal/transform"
)

const (
	otlpEndpointEnv     = "OTEL_EXPORTER_OTLP_ENDPOINT"
	otlpLogsEndpointEnv = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"
	otlpInsecureEnv     = "OTEL_EXPORTER_OTLP_INSECURE"
	otlpLogsInsecureEnv = "OTEL_EXPORTER_OTLP_LOGS_INSECURE"
)

type handlerOptions struct {
	resource             *resource.Resource
	insecure             *bool
	errorHandler         func(error)
	endpoint             string
	dialOptions          []grpc.DialOption
	level                slog.Level
	bundleDelayThreshold time.Duration
	bundleCountThreshold int
}

// HandlerOption applies an option when creating a Handler.
type HandlerOption interface {
	apply(*handlerOptions)
}

type handlerOptionFunc func(*handlerOptions)

func (o handlerOptionFunc) apply(opts *handlerOptions) {
	o(opts)
}

// WithLevel sets the minimum level to log. The default is slog.LevelInfo.
func WithLevel(level slog.Level) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.level = level
	})
}

// WithResource sets the resource. There is no default.
func WithResource(resource *resource.Resource) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.resource = resource
	})
}

// WithEndpoint sets the endpoint. If not set, this will be read from the environment
func WithEndpoint(endpoint string) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.endpoint = endpoint
	})
}

// WithInsecure sets the insecure flag. If not set, this will be read from the environment
func WithInsecure(insecure bool) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.insecure = &insecure
	})
}

// WithBundleDelayThreshold sets the bundle delay threshold. The default is bundler.DefaultDelayThreshold.
func WithBundleDelayThreshold(delay time.Duration) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.bundleDelayThreshold = delay
	})
}

// WithBundleCountThreshold sets the bundle count threshold. The default is bundler.DefaultBundleCountThreshold.
func WithBundleCountThreshold(count int) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.bundleCountThreshold = count
	})
}

// WithErrorHandler sets the error handler. If not set, errors are logged to stderr.
func WithErrorHandler(handler func(error)) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.errorHandler = handler
	})
}

// WithDialOptions sets the grpc dial options.
func WithDialOptions(opts ...grpc.DialOption) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.dialOptions = opts
	})
}

// EnvSet returns if the OTLP environment variables are set.
func EnvSet() bool {
	return os.Getenv(otlpLogsEndpointEnv) != "" || os.Getenv(otlpEndpointEnv) != ""
}

type exporter interface {
	UploadLogs(ctx context.Context, logs []*logspb.ResourceLogs) error
	Shutdown(ctx context.Context) error
}

type grpcExporter struct {
	client collectorLogs.LogsServiceClient
	conn   *grpc.ClientConn
}

func newGrpcExporter(ctx context.Context, opts handlerOptions) (*grpcExporter, error) {
	if opts.endpoint == "" {
		exporterTarget := os.Getenv(otlpLogsEndpointEnv)
		if exporterTarget == "" {
			exporterTarget = os.Getenv(otlpEndpointEnv)
		}
		if exporterTarget == "" {
			return nil, fmt.Errorf("no endpoint provided")
		}

		opts.endpoint = exporterTarget
	}

	u, err := url.Parse(opts.endpoint)
	if err != nil {
		return nil, fmt.Errorf("unabel to parse endpoint %v", err)
	}

	if opts.insecure == nil {
		val := os.Getenv(otlpLogsInsecureEnv)
		if val == "" {
			val = os.Getenv(otlpInsecureEnv)
		}

		if val != "" {
			if rc, err := strconv.ParseBool(val); err != nil {
				opts.insecure = &rc
			}
		}
	}

	var dialOptions []grpc.DialOption

	scheme := strings.ToLower(u.Scheme)
	if scheme == "http" || (opts.insecure != nil && *opts.insecure) {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	dialOptions = append(dialOptions, opts.dialOptions...)

	conn, err := grpc.DialContext(ctx, u.Host, dialOptions...)
	if err != nil {
		return nil, err
	}

	return &grpcExporter{
		client: collectorLogs.NewLogsServiceClient(conn),
		conn:   conn,
	}, nil
}

func (e *grpcExporter) UploadLogs(ctx context.Context, logs []*logspb.ResourceLogs) error {
	request := collectorLogs.ExportLogsServiceRequest{
		ResourceLogs: logs,
	}

	_, err := e.client.Export(ctx, &request)
	if err != nil {
		return fmt.Errorf("error exporting logs %w", err)
	}

	return nil
}

func (e *grpcExporter) Shutdown(ctx context.Context) error {
	return e.conn.Close()
}

type bundlerExporter struct {
	bundler  *bundler.Bundler
	exporter exporter
}

func newBundlerExporter(exporter exporter, opts handlerOptions) *bundlerExporter {
	b := bundler.NewBundler((*logspb.ResourceLogs)(nil), func(bundle any) {
		// just to be sure
		bundleLogs, ok := bundle.([]*logspb.ResourceLogs)
		if !ok {
			return
		}

		// How to handle errors here?
		err := exporter.UploadLogs(context.Background(), bundleLogs)
		if err != nil && opts.errorHandler != nil {
			opts.errorHandler(err)
		}
	})

	b.BundleCountThreshold = opts.bundleCountThreshold
	b.DelayThreshold = opts.bundleDelayThreshold

	return &bundlerExporter{
		bundler:  b,
		exporter: exporter,
	}
}

func (b *bundlerExporter) UploadLogs(ctx context.Context, logs []*logspb.ResourceLogs) error {
	var lastError error
	for _, l := range logs {
		if err := b.bundler.AddWait(ctx, l, 1); err != nil {
			lastError = err
		}
	}

	return lastError
}

func (b *bundlerExporter) Shutdown(ctx context.Context) error {
	b.bundler.Flush()
	return b.exporter.Shutdown(ctx)
}

// NewHandler creates a new slog.Handler.
func NewHandler(ctx context.Context, options ...HandlerOption) (*Handler, error) {
	opts := handlerOptions{
		level:                slog.LevelInfo,
		bundleCountThreshold: bundler.DefaultBundleCountThreshold,
		bundleDelayThreshold: bundler.DefaultDelayThreshold,
		errorHandler: func(err error) {
			_, _ = fmt.Fprintln(os.Stderr, "[slogotlp] error", err)
		},
	}

	for _, o := range options {
		o.apply(&opts)
	}

	g, err := newGrpcExporter(ctx, opts)
	if err != nil {
		return nil, err
	}

	var exporter exporter = g

	if opts.bundleCountThreshold > 0 {
		exporter = newBundlerExporter(exporter, opts)
	}

	h := Handler{
		exporter: exporter,
		level:    opts.level,
	}

	if opts.resource != nil {
		h.resource = transform.Resource(opts.resource)
	}

	return &h, nil
}

// Handler implements the slog.Handler interface.
type Handler struct {
	exporter exporter
	resource *resourcepb.Resource
	group    *group
	attrs    []slog.Attr
	level    slog.Level
}

var _ slog.Handler = &Handler{}

// Shutdown shutsdown the handler. This should be called before the process exits
// to flush buffers and close conenctions.
func (h *Handler) Shutdown(ctx context.Context) error {
	return h.exporter.Shutdown(ctx)
}

func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

// WithGroup is a no-op, as we do not support groups yet.
func (h *Handler) WithGroup(name string) slog.Handler {
	h2 := *h

	h2.group = &group{
		next: h.group,
		name: name,
	}

	return &h2
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := *h
	if h2.group != nil {
		h2.group = h2.group.Clone()
		h2.group.AddAttrs(attrs)
	} else {
		newAttrs := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
		newAttrs = append(newAttrs, h.attrs...)
		newAttrs = append(newAttrs, attrs...)
		h2.attrs = newAttrs
	}
	return &h2
}

func (h *Handler) Handle(ctx context.Context, record slog.Record) error {
	logRecord := h.convertRecord(record)

	if spanContext := trace.SpanContextFromContext(ctx); spanContext.HasTraceID() {
		traceID := spanContext.TraceID()
		logRecord.TraceId = traceID[:]

		logRecord.Flags = uint32(spanContext.TraceFlags())

		if spanContext.HasSpanID() {
			spanID := spanContext.SpanID()
			logRecord.SpanId = spanID[:]
		}
	}

	rl := logspb.ResourceLogs{
		Resource: h.resource,
		ScopeLogs: []*logspb.ScopeLogs{
			{
				LogRecords: []*logspb.LogRecord{
					logRecord,
				},
			},
		},
	}

	// we do not want to use any timeout that may be set in the passed in context,
	// so we use Background. This will block until it is able to push item to the bundle.
	return h.exporter.UploadLogs(ctx, []*logspb.ResourceLogs{&rl})
}

func (h *Handler) convertRecord(r slog.Record) *logspb.LogRecord {
	severityNumber := logspb.SeverityNumber(r.Level + 9)

	timeNow := uint64(r.Time.UnixNano())

	record := logspb.LogRecord{
		TimeUnixNano:         timeNow,
		ObservedTimeUnixNano: timeNow,
		SeverityNumber:       severityNumber,
		SeverityText:         severityNumber.String(),
		Body: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: r.Message,
			},
		},
	}

	n := r.NumAttrs()
	if h.group != nil {
		if n > 0 {
			buf := newKVBuffer(n)
			r.Attrs(buf.AddAttr)
			record.Attributes = append(record.Attributes, h.group.KeyValue(buf.KeyValues()...))
		} else {
			// A Handler should not output groups if there are no attributes.
			g := h.group.NextNonEmpty()
			if g != nil {
				record.Attributes = append(record.Attributes, g.KeyValue())
			}
		}
	} else if n > 0 {
		buf := newKVBuffer(n)
		r.Attrs(buf.AddAttr)
		record.Attributes = append(record.Attributes, buf.data...)
	}

	return &record
}

// group support is based on https://github.com/open-telemetry/opentelemetry-go-contrib/blob/bridges/otelslog/v0.0.1/bridges/otelslog/handler.go
// which is Apache 2.0 Licensed

type kvBuffer struct {
	data []*commonpb.KeyValue
}

func newKVBuffer(n int) *kvBuffer {
	return &kvBuffer{data: make([]*commonpb.KeyValue, 0, n)}
}

func (b *kvBuffer) Clone() *kvBuffer {
	if b == nil {
		return b
	}
	b2 := *b
	b2.data = slices.Clone(b.data)
	return &b2
}

func (b *kvBuffer) Len() int {
	if b == nil {
		return 0
	}
	return len(b.data)
}

func (b *kvBuffer) AddAttrs(attrs []slog.Attr) {
	b.data = slices.Grow(b.data, len(attrs))
	for _, a := range attrs {
		_ = b.AddAttr(a)
	}
}

func (b *kvBuffer) AddAttr(attr slog.Attr) bool {
	if attr.Key == "" {
		if attr.Value.Kind() == slog.KindGroup {
			// A Handler should inline the Attrs of a group with an empty key.
			for _, a := range attr.Value.Group() {
				b.data = append(b.data, &commonpb.KeyValue{
					Key:   a.Key,
					Value: convertValue(a.Value),
				})
			}
			return true
		}

		if attr.Value.Any() == nil {
			// A Handler should ignore an empty Attr.
			return true
		}
	}
	b.data = append(b.data, &commonpb.KeyValue{
		Key:   attr.Key,
		Value: convertValue(attr.Value),
	})
	return true
}

// group represents a group received from slog.
type group struct {
	next  *group
	attrs *kvBuffer
	name  string
}

func (g *group) NextNonEmpty() *group {
	if g == nil || g.attrs.Len() > 0 {
		return g
	}
	return g.next.NextNonEmpty()
}

func (g *group) Clone() *group {
	if g == nil {
		return g
	}
	g2 := *g
	g2.attrs = g2.attrs.Clone()
	return &g2
}

func (g *group) AddAttrs(attrs []slog.Attr) {
	if g.attrs == nil {
		g.attrs = newKVBuffer(len(attrs))
	}
	g.attrs.AddAttrs(attrs)
}

func convertValue(v slog.Value) *commonpb.AnyValue {
	switch v.Kind() {
	case slog.KindAny:
		return convertAny(v.Any())

	case slog.KindBool:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_BoolValue{
				BoolValue: v.Bool(),
			},
		}

	case slog.KindDuration:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{
				IntValue: v.Duration().Nanoseconds(),
			},
		}

	case slog.KindFloat64:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_DoubleValue{
				DoubleValue: v.Float64(),
			},
		}

	case slog.KindInt64:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{
				IntValue: v.Int64(),
			},
		}

	case slog.KindString:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: v.String(),
			},
		}

	case slog.KindTime:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{
				IntValue: v.Time().UnixNano(),
			},
		}

	case slog.KindUint64:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{
				IntValue: int64(v.Uint64()),
			},
		}

	case slog.KindGroup:
		buf := newKVBuffer(5)
		buf.AddAttrs(v.Group())
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_KvlistValue{
				KvlistValue: &commonpb.KeyValueList{
					Values: buf.data,
				},
			},
		}

	case slog.KindLogValuer:
		return convertValue(v.Resolve())

	default:
		_, _ = fmt.Fprintf(os.Stderr, "[slogotlp] unhandled kind %T %v %v\n", v.Any(), v.Kind(), v.Any())

		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: fmt.Sprintf("unhandled: (%s) %+v", v.Kind(), v.Any()),
			},
		}
	}
}

func convertAny(value any) *commonpb.AnyValue {
	if tm, ok := value.(encoding.TextMarshaler); ok {
		data, err := tm.MarshalText()
		if err != nil {
			return nil
		}

		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: string(data),
			},
		}
	}

	if str, ok := value.(fmt.Stringer); ok {
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: str.String(),
			},
		}
	}

	if err, ok := value.(error); ok {
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: err.Error(),
			},
		}
	}

	if bs, ok := value.([]byte); ok {
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: strconv.Quote(string(bs)),
			},
		}
	}

	rt := reflect.TypeOf(value)
	kind := rt.Kind()

	switch kind {
	case reflect.Slice, reflect.Array:
		// special case for byte slices
		if rt.Elem().Kind() == reflect.Uint8 {
			return &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{
					StringValue: strconv.Quote(string(reflect.ValueOf(value).Bytes())),
				},
			}
		}

		s := reflect.ValueOf(value)

		arrayValue := commonpb.ArrayValue{
			Values: make([]*commonpb.AnyValue, 0, s.Len()),
		}

		for i := 0; i < s.Len(); i++ {
			item := s.Index(i).Interface()
			if val := convertValue(slog.AnyValue(item)); val != nil {
				arrayValue.Values = append(arrayValue.Values, val)
			}
		}

		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_ArrayValue{
				ArrayValue: &arrayValue,
			},
		}

	default:
		// fallthrough to below
	}

	return &commonpb.AnyValue{
		Value: &commonpb.AnyValue_StringValue{
			StringValue: fmt.Sprintf("%+v", value),
		},
	}
}

func (g *group) KeyValue(kvs ...*commonpb.KeyValue) *commonpb.KeyValue {
	// Assumes checking of group g already performed (i.e. non-empty).
	out := &commonpb.KeyValue{
		Key: g.name,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_KvlistValue{
				KvlistValue: &commonpb.KeyValueList{
					Values: g.attrs.data,
				},
			},
		},
	}

	g = g.next
	for g != nil {
		// A Handler should not output groups if there are no attributes.
		if g.attrs.Len() > 0 {
			out = &commonpb.KeyValue{
				Key: g.name,
				Value: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_KvlistValue{
						KvlistValue: &commonpb.KeyValueList{
							Values: []*commonpb.KeyValue{
								out,
							},
						},
					},
				},
			}
		}
		g = g.next
	}
	return out
}

func (b *kvBuffer) KeyValues(kvs ...*commonpb.KeyValue) []*commonpb.KeyValue {
	if b == nil {
		return kvs
	}
	return append(b.data, kvs...)
}
