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
