package slogotlp_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"testing"

	"github.com/bakins/slogotlp"
	"github.com/matryer/is"
	collectorLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
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

	handler, err := slogotlp.NewHandler(context.Background(), slogotlp.WithEndpoint("http://"+listener.Addr().String()))
	is.NoErr(err)

	t.Cleanup(func() {
		// we shutdown the handler below, so ignore this error
		_ = handler.Shutdown(context.Background())
	})

	logger := slog.New(handler)
	for i := 0; i < 10; i++ {
		fmt.Println("logging")
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

	fmt.Println(len(t.logRecords))

	return &collectorLogs.ExportLogsServiceResponse{}, nil
}
