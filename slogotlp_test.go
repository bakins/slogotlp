package slogotlp_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
	collectorLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"

	"github.com/bakins/slogotlp"
)

func TestHandler(t *testing.T) {
	is := is.NewRelaxed(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	is.NoErr(err)

	t.Cleanup(func() {
		_ = listener.Close()
	})

	var collector testCollector

	g := grpc.NewServer()
	collectorLogs.RegisterLogsServiceServer(g, &collector)

	go func() {
		is.NoErr(g.Serve(listener))
	}()

	t.Cleanup(func() {
		g.Stop()
	})

	handler, err := slogotlp.NewHandler(
		context.Background(),
		slogotlp.WithEndpoint("http://"+listener.Addr().String()),
		slogotlp.WithDialOptions(grpc.WithBlock()),
	)
	is.NoErr(err)

	t.Cleanup(func() {
		// we shutdown the handler below, so ignore this error
		_ = handler.Shutdown(context.Background())
	})

	logger := slog.New(handler)
	for i := 0; i < 10; i++ {
		logger.Info("test", "index", i)
	}

	is.NoErr(handler.Shutdown(context.Background()))

	is.Equal(len(collector.logRecords), 10)
}

type testCollector struct {
	collectorLogs.UnimplementedLogsServiceServer
	logRecords []*logspb.LogRecord
	mu         sync.Mutex
}

func (t *testCollector) Export(_ context.Context, request *collectorLogs.ExportLogsServiceRequest) (*collectorLogs.ExportLogsServiceResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, resourceLogs := range request.ResourceLogs {
		for _, scopeLogs := range resourceLogs.ScopeLogs {
			t.logRecords = append(t.logRecords, scopeLogs.LogRecords...)
		}
	}

	return &collectorLogs.ExportLogsServiceResponse{}, nil
}

func newTestHandler(t *testing.T) (*slogotlp.Handler, *testCollector) {
	t.Helper()
	is := is.New(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	is.NoErr(err)

	t.Cleanup(func() {
		_ = listener.Close()
	})

	collector := &testCollector{}

	g := grpc.NewServer()
	collectorLogs.RegisterLogsServiceServer(g, collector)

	go func() {
		_ = g.Serve(listener)
	}()

	t.Cleanup(func() {
		g.Stop()
	})

	handler, err := slogotlp.NewHandler(
		context.Background(),
		slogotlp.WithEndpoint("http://"+listener.Addr().String()),
		slogotlp.WithDialOptions(grpc.WithBlock()),
	)
	is.NoErr(err)

	t.Cleanup(func() {
		_ = handler.Shutdown(context.Background())
	})

	return handler, collector
}

func findAttr(attrs []*commonpb.KeyValue, key string) *commonpb.AnyValue {
	for _, a := range attrs {
		if a.Key == key {
			return a.Value
		}
	}
	return nil
}

func kvList(av *commonpb.AnyValue) []*commonpb.KeyValue {
	if av == nil {
		return nil
	}
	return av.GetKvlistValue().GetValues()
}

func TestInsecureEnv(t *testing.T) {
	tests := map[string]struct {
		envKey   string
		envValue string
	}{
		"OTEL_EXPORTER_OTLP_INSECURE true":      {envKey: "OTEL_EXPORTER_OTLP_INSECURE", envValue: "true"},
		"OTEL_EXPORTER_OTLP_LOGS_INSECURE true": {envKey: "OTEL_EXPORTER_OTLP_LOGS_INSECURE", envValue: "true"},
		"OTEL_EXPORTER_OTLP_INSECURE 1":         {envKey: "OTEL_EXPORTER_OTLP_INSECURE", envValue: "1"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			is.NoErr(err)
			t.Cleanup(func() { _ = listener.Close() })

			collector := &testCollector{}
			g := grpc.NewServer()
			collectorLogs.RegisterLogsServiceServer(g, collector)
			go func() { _ = g.Serve(listener) }()
			t.Cleanup(func() { g.Stop() })

			t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
			t.Setenv("OTEL_EXPORTER_OTLP_LOGS_INSECURE", "")
			t.Setenv(test.envKey, test.envValue)

			// Endpoint without "http" scheme so the insecure decision must come
			// from the env var path, not the scheme shortcut.
			handler, err := slogotlp.NewHandler(
				context.Background(),
				slogotlp.WithEndpoint("//"+listener.Addr().String()),
				slogotlp.WithDialOptions(grpc.WithBlock()),
			)
			is.NoErr(err)
			t.Cleanup(func() { _ = handler.Shutdown(context.Background()) })

			logger := slog.New(handler)
			logger.Info("hello")

			is.NoErr(handler.Shutdown(context.Background()))
			is.Equal(1, len(collector.logRecords))
		})
	}
}

func TestInsecureEnvInvalid(t *testing.T) {
	// Invalid bool values should be ignored (insecure stays unset). With no
	// "http" scheme and no insecure flag, grpc.DialContext returns an error
	// because no transport credentials are configured.
	is := is.New(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	is.NoErr(err)
	t.Cleanup(func() { _ = listener.Close() })

	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "not-a-bool")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_INSECURE", "")

	_, err = slogotlp.NewHandler(
		context.Background(),
		slogotlp.WithEndpoint("//"+listener.Addr().String()),
	)
	is.True(err != nil)
}

func TestGroups(t *testing.T) {
	tests := map[string]struct {
		log      func(*slog.Logger)
		validate func(*is.I, *logspb.LogRecord)
	}{
		"WithGroup single": {
			log: func(l *slog.Logger) {
				l.WithGroup("g").Info("m", "k", "v")
			},
			validate: func(is *is.I, lr *logspb.LogRecord) {
				is.Equal(1, len(lr.Attributes))
				g := findAttr(lr.Attributes, "g")
				is.True(g != nil)
				inner := kvList(g)
				is.Equal(1, len(inner))
				is.Equal("k", inner[0].Key)
				is.Equal("v", inner[0].Value.GetStringValue())
			},
		},
		"WithGroup nested": {
			log: func(l *slog.Logger) {
				l.WithGroup("a").WithGroup("b").Info("m", "k", int64(1))
			},
			validate: func(is *is.I, lr *logspb.LogRecord) {
				a := findAttr(lr.Attributes, "a")
				is.True(a != nil)
				aKvs := kvList(a)
				is.Equal(1, len(aKvs))
				is.Equal("b", aKvs[0].Key)
				bKvs := kvList(aKvs[0].Value)
				is.Equal(1, len(bKvs))
				is.Equal("k", bKvs[0].Key)
				is.Equal(int64(1), bKvs[0].Value.GetIntValue())
			},
		},
		"WithAttrs inside group": {
			log: func(l *slog.Logger) {
				l.WithGroup("g").With("k1", "v1").Info("m", "k2", "v2")
			},
			validate: func(is *is.I, lr *logspb.LogRecord) {
				g := findAttr(lr.Attributes, "g")
				is.True(g != nil)
				inner := kvList(g)
				is.Equal(2, len(inner))
				is.Equal("v1", findAttr(inner, "k1").GetStringValue())
				is.Equal("v2", findAttr(inner, "k2").GetStringValue())
			},
		},
		"inline slog.Group": {
			log: func(l *slog.Logger) {
				l.Info("m", slog.Group("g", "k", "v"))
			},
			validate: func(is *is.I, lr *logspb.LogRecord) {
				g := findAttr(lr.Attributes, "g")
				is.True(g != nil)
				inner := kvList(g)
				is.Equal(1, len(inner))
				is.Equal("k", inner[0].Key)
				is.Equal("v", inner[0].Value.GetStringValue())
			},
		},
		"inline group inside WithGroup": {
			log: func(l *slog.Logger) {
				l.WithGroup("outer").Info("m", slog.Group("inner", "k", "v"))
			},
			validate: func(is *is.I, lr *logspb.LogRecord) {
				outer := findAttr(lr.Attributes, "outer")
				is.True(outer != nil)
				outerKvs := kvList(outer)
				is.Equal(1, len(outerKvs))
				is.Equal("inner", outerKvs[0].Key)
				innerKvs := kvList(outerKvs[0].Value)
				is.Equal(1, len(innerKvs))
				is.Equal("k", innerKvs[0].Key)
				is.Equal("v", innerKvs[0].Value.GetStringValue())
			},
		},
		"empty-key group inlined": {
			log: func(l *slog.Logger) {
				l.Info("m", slog.Group("", "k", "v"))
			},
			validate: func(is *is.I, lr *logspb.LogRecord) {
				is.Equal(1, len(lr.Attributes))
				is.Equal("k", lr.Attributes[0].Key)
				is.Equal("v", lr.Attributes[0].Value.GetStringValue())
			},
		},
		"empty group skipped": {
			log: func(l *slog.Logger) {
				l.WithGroup("g").Info("m")
			},
			validate: func(is *is.I, lr *logspb.LogRecord) {
				is.Equal(0, len(lr.Attributes))
			},
		},
		"WithAttrs no group": {
			log: func(l *slog.Logger) {
				l.With("top", "x").Info("m", "k", "v")
			},
			validate: func(is *is.I, lr *logspb.LogRecord) {
				is.Equal(2, len(lr.Attributes))
				is.Equal("x", findAttr(lr.Attributes, "top").GetStringValue())
				is.Equal("v", findAttr(lr.Attributes, "k").GetStringValue())
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			handler, collector := newTestHandler(t)

			logger := slog.New(handler)
			test.log(logger)

			is.NoErr(handler.Shutdown(context.Background()))
			is.Equal(1, len(collector.logRecords))
			is.Equal("m", collector.logRecords[0].Body.GetStringValue())
			test.validate(is, collector.logRecords[0])
		})
	}
}

func TestTypes(t *testing.T) {
	getAttribute := func(is *is.I, lr *logspb.LogRecord, key string) *commonpb.AnyValue {
		is.Helper()
		for _, a := range lr.Attributes {
			if a.Key == key {
				return a.Value
			}
		}

		is.Fail()

		return nil
	}

	tests := map[string]struct {
		validator  func(*is.I, *logspb.LogRecord)
		attributes []any
	}{
		"simple types": {
			attributes: []any{
				"float", 42.0,
				"string", "hello",
				"bool", true,
				"time", time.Date(2023, time.August, 22, 0, 0, 0, 0, time.UTC),
			},
			validator: func(is *is.I, lr *logspb.LogRecord) {
				is.Equal(42.0, getAttribute(is, lr, "float").GetDoubleValue())
			},
		},
		"error": {
			attributes: []any{
				"error", errors.New("bad things"),
			},
			validator: func(is *is.I, lr *logspb.LogRecord) {
				is.Equal("bad things", getAttribute(is, lr, "error").GetStringValue())
			},
		},
		"array of int64": {
			attributes: []any{
				"array_of_int64", []int64{42, 100},
			},
			validator: func(is *is.I, lr *logspb.LogRecord) {
				array := getAttribute(is, lr, "array_of_int64").GetArrayValue()
				is.True(array != nil)
				is.Equal(2, len(array.Values))
				is.Equal(int64(100), array.Values[1].GetIntValue())
			},
		},
		"array of any": {
			attributes: []any{
				"array_of_any", []any{
					42.0,
					"testing1234",
					bytes.NewBuffer([]byte("127.0.0.1:8080")),
				},
			},
			validator: func(is *is.I, lr *logspb.LogRecord) {
				array := getAttribute(is, lr, "array_of_any").GetArrayValue()
				is.True(array != nil)
				is.Equal(3, len(array.Values))
				is.Equal(42.0, array.Values[0].GetDoubleValue())
				is.Equal("testing1234", array.Values[1].GetStringValue())
				is.Equal("127.0.0.1:8080", array.Values[2].GetStringValue())
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			is.NoErr(err)

			t.Cleanup(func() {
				_ = listener.Close()
			})

			var collector testCollector

			g := grpc.NewServer()
			collectorLogs.RegisterLogsServiceServer(g, &collector)

			go func() {
				is.NoErr(g.Serve(listener))
			}()

			t.Cleanup(func() {
				g.Stop()
			})

			handler, err := slogotlp.NewHandler(context.Background(), slogotlp.WithEndpoint("http://"+listener.Addr().String()))
			is.NoErr(err)

			t.Cleanup(func() {
				// we shutdown the handler below, so ignore this error
				_ = handler.Shutdown(context.Background())
			})

			logger := slog.New(handler)

			logger.Info("testing", test.attributes...)

			is.NoErr(handler.Shutdown(context.Background()))

			is.Equal(1, len(collector.logRecords))
			is.Equal("testing", collector.logRecords[0].Body.GetStringValue())

			test.validator(is, collector.logRecords[0])
		})
	}
}
