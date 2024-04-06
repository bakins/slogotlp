# slogoltp

[![Go Reference](https://pkg.go.dev/badge/go.dev/github.com/bakins/slogotlp.svg)](https://pkg.go.dev/github.com/bakins/slogotlp)

OpenTelemetry oltp exporter support for [slog](https://pkg.go.dev/log/slog).

* Go 1.21+ support as we use the new stdlib [slog](https://pkg.go.dev/log/slog).
* Currently , only grpc otlp is supported.

## Usage

```go
import (
    "context"
    "log/slog"

    "github.com/bakins/slogotlp"
)

func main() {
    handler, err := slogotlp.NewHandler(context.Background())
    // handle err

    defer handler.Shutdown(context.Background())

    logger := slog.New(handler)

    // use logger
}
```
By default, `slogotlp` uses [environment variables](https://opentelemetry.io/docs/specs/otel/protocol/exporter/) for configuration.

* checks `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` then `OTEL_EXPORTER_OTLP_ENDPOINT`` for the exporter endpoint. This should be a url like "http://localhost:4317". There is no default.
* If the scheme in the endpoint url is "http" or the environment variable `OTEL_EXPORTER_OTLP_LOGS_INSECURE` or `OTEL_EXPORTER_OTLP_INSECURE` is set to "true", then insecure communication is used.

Currently, no other OpenTelemetry [environment variables](https://opentelemetry.io/docs/specs/otel/protocol/exporter/) are supported.

Options may also be set when creating the log handler.

## LICENSE

see [LICENSE](./LICENSE)
