package slogotlp

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bakins/slogotlp/internal/transform"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
	collectorLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	otlpEndpointEnv = "OTEL_EXPORTER_OTLP_ENDPOINT"
	otlpInsecureEnv = "OTEL_EXPORTER_OTLP_INSECURE"
)

type handlerOptions struct {
	exporter        Exporter
	resource        *resource.Resource
	exporterOptions []ExporterOption
	level           slog.Level
}

type HandlerOption interface {
	apply(*handlerOptions)
}

type handlerOptionFunc func(*handlerOptions)

func (o handlerOptionFunc) apply(opts *handlerOptions) {
	o(opts)
}

func WithExporter(exporter Exporter) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.exporter = exporter
	})
}

func WithExporterOptions(options ...ExporterOption) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.exporterOptions = options
	})
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

// EnvSet returns if the OTLP environment variables are set.
func EnvSet() bool {
	return os.Getenv(otlpEndpointEnv) != ""
}

type Exporter interface {
	UploadLogs(ctx context.Context, logs []*logspb.ResourceLogs) error
	Shutdown(ctx context.Context) error
}

type GrpcExporter struct {
	client collectorLogs.LogsServiceClient
	conn   *grpc.ClientConn
}

type exporterOptions struct {
	insecure *bool
	endpoint string
}

type ExporterOption interface {
	apply(*exporterOptions)
}

type exporterOptionFunc func(*exporterOptions)

func (o exporterOptionFunc) apply(opts *exporterOptions) {
	o(opts)
}

func WithEndpoint(endpoint string) ExporterOption {
	return exporterOptionFunc(func(o *exporterOptions) {
		o.endpoint = endpoint
	})
}

func WithInsecure(insecure bool) ExporterOption {
	return exporterOptionFunc(func(o *exporterOptions) {
		o.insecure = &insecure
	})
}

func NewGrpcExporter(ctx context.Context, options ...ExporterOption) (*GrpcExporter, error) {
	opts := exporterOptions{}

	for _, o := range options {
		o.apply(&opts)
	}

	if opts.endpoint == "" {
		exporterTarget := os.Getenv(otlpEndpointEnv)
		if exporterTarget == "" {
			return nil, fmt.Errorf("no endpoint provided")
		}

		opts.endpoint = exporterTarget
	}

	if opts.insecure == nil {
		if val := os.Getenv(otlpInsecureEnv); val != "" {
			if rc, err := strconv.ParseBool(val); err != nil {
				opts.insecure = &rc
			}
		}
	}

	var dialOptions []grpc.DialOption
	if strings.HasPrefix(opts.endpoint, "http://") || (opts.insecure != nil && *opts.insecure) {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.DialContext(ctx, opts.endpoint, dialOptions...)
	if err != nil {
		return nil, err
	}

	return &GrpcExporter{
		client: collectorLogs.NewLogsServiceClient(conn),
		conn:   conn,
	}, nil
}

func (e *GrpcExporter) UploadLogs(ctx context.Context, logs []*logspb.ResourceLogs) error {
	request := collectorLogs.ExportLogsServiceRequest{
		ResourceLogs: logs,
	}

	_, err := e.client.Export(ctx, &request)
	if err != nil {
		return fmt.Errorf("error exporting logs %w", err)
	}

	return nil
}

func (e *GrpcExporter) Shutdown(ctx context.Context) error {
	return e.conn.Close()
}

func NewHandler(options ...HandlerOption) (*Handler, error) {
	opts := handlerOptions{
		level: slog.LevelInfo,
	}

	for _, o := range options {
		o.apply(&opts)
	}

	if opts.exporter == nil {
		exporter, err := NewGrpcExporter(context.Background(), opts.exporterOptions...)
		if err != nil {
			return nil, err
		}

		opts.exporter = exporter
	}

	h := Handler{
		exporter: opts.exporter,
		level:    opts.level,
	}

	if opts.resource != nil {
		h.resource = transform.Resource(opts.resource)
	}

	return &h, nil
}

type Handler struct {
	exporter Exporter
	attrs    []slog.Attr
	level    slog.Level
	resource *resourcepb.Resource
}

var _ slog.Handler = &Handler{}

func (h *Handler) Close() error {
	return h.exporter.Shutdown(context.Background())
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
		if val, ok := v.Any().(error); ok {
			return &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{
					StringValue: val.Error(),
				},
			}
		}

		if val, ok := v.Any().(fmt.Stringer); ok {
			return &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{
					StringValue: val.String(),
				},
			}
		}

		if val, ok := v.Any().(fmt.Stringer); ok {
			return &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{
					StringValue: val.String(),
				},
			}
		}

		// TODO: handle other types.

		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: fmt.Sprintf("%v", v.Any()),
			},
		}
	default:
		// unhandled kind
		_, _ = fmt.Fprintf(os.Stderr, "[proxylogger] unhandled kind %T %v %v\n", v.Any(), v.Kind(), v.Any())
		return nil
	}
}
