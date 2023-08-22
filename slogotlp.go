package slogotlp

import (
	"context"
	"encoding"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"reflect"
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

func WithLevel(level slog.Level) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.level = level
	})
}

func WithResource(resource *resource.Resource) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.resource = resource
	})
}

func WithEndpoint(endpoint string) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.endpoint = endpoint
	})
}

func WithInsecure(insecure bool) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.insecure = &insecure
	})
}

func WithBundleDelayThreshold(delay time.Duration) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.bundleDelayThreshold = delay
	})
}

func WithBundleCountThreshold(count int) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.bundleCountThreshold = count
	})
}

func WithErrorHandler(handler func(error)) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.errorHandler = handler
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
	if name == "" {
		return h
	}

	n := *h

	return &n
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	n := *h

	cp := make([]slog.Attr, 0, len(n.attrs)+len(attrs))
	cp = append(cp, n.attrs...)
	cp = append(cp, attrs...)
	n.attrs = cp

	return &n
}

func (h *Handler) Handle(ctx context.Context, record slog.Record) error {
	severityNumber := logspb.SeverityNumber(record.Level + 9)

	timeNow := uint64(record.Time.UnixNano())

	logRecord := logspb.LogRecord{
		TimeUnixNano:         timeNow,
		ObservedTimeUnixNano: timeNow,
		SeverityNumber:       severityNumber,
		SeverityText:         severityNumber.String(),
		Body: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: record.Message,
			},
		},
		Attributes: make([]*commonpb.KeyValue, 0, len(h.attrs)),
	}

	for _, a := range h.attrs {
		kv := convertAttribute(a)
		if kv != nil {
			logRecord.Attributes = append(logRecord.Attributes, kv)
		}
	}

	record.Attrs(func(a slog.Attr) bool {
		kv := convertAttribute(a)
		if kv != nil {
			logRecord.Attributes = append(logRecord.Attributes, kv)
		}

		return true
	})

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
					&logRecord,
				},
			},
		},
	}

	// we do not want to use any timeout that may be set in the passed in context,
	// so we use Background. This will block until it is able to push item to the bundle.
	return h.exporter.UploadLogs(ctx, []*logspb.ResourceLogs{&rl})
}

func convertAttribute(attr slog.Attr) *commonpb.KeyValue {
	value := convertAttributeValue(attr.Value)
	if value == nil {
		return nil
	}

	return &commonpb.KeyValue{
		Key:   attr.Key,
		Value: value,
	}
}

func convertAttributeValue(v slog.Value) *commonpb.AnyValue {
	switch v.Kind() {
	case slog.KindBool:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_BoolValue{
				BoolValue: v.Bool(),
			},
		}

	case slog.KindDuration:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_DoubleValue{
				DoubleValue: v.Duration().Seconds(),
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
			Value: &commonpb.AnyValue_StringValue{
				StringValue: v.Time().Format(time.RFC3339Nano),
			},
		}

	case slog.KindUint64:
		// otel doesn't support uint64, so this may get truncated...
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{
				IntValue: int64(v.Uint64()),
			},
		}

	case slog.KindLogValuer:
		val := v.LogValuer().LogValue()
		return convertAttributeValue(val)

	case slog.KindAny:
		value := v.Any()

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
				if val := convertAttributeValue(slog.AnyValue(item)); val != nil {
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
	default:
		// unhandled kind
		_, _ = fmt.Fprintf(os.Stderr, "[proxylogger] unhandled kind %T %v %v\n", v.Any(), v.Kind(), v.Any())
		return nil
	}
}
