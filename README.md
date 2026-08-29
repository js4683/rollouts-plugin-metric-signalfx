# SignalFx Metric Plugin for Argo Rollouts

An out-of-tree RPC metric provider that evaluates bounded SignalFlow programs against Splunk Observability Cloud (formerly SignalFx).

- **Status:** Planning / initial implementation. Metric plugins are still alpha in Argo Rollouts.
- **Compatibility:** Argo Rollouts v1.10.0. Pin the dependency exactly for the first release.
- **Provider key:** `js4683/rollouts-plugin-metric-signalfx`
- **Module:** `github.com/js4683/rollouts-plugin-metric-signalfx`

## Behavior

- Each `Run` parses one metric configuration, creates one SignalFlow client, executes the configured program, consumes `Computation.Data()` for the configured `duration` in seconds, stops the computation, drains buffered messages (up to 2 seconds), reduces numeric payloads to one value using `aggregator`, and evaluates the value through Argo Rollouts `evaluate.EvaluateResult`.
- Empty data, unsupported value types, non-finite values, stream errors, and deadline expiry return `AnalysisPhaseError`.
- A failed SignalFlow computation propagates its error without transparent retries. The controller's analysis retry policy remains authoritative.
- `Resume`, `Terminate`, and `GarbageCollect` are idempotent no-ops. Measurements are finite and not persisted by the plugin.
- `Type()` returns `RPCPlugin`.

## Configuration

The JSON object under `metric.provider.plugin["js4683/rollouts-plugin-metric-signalfx"]`:

| Field | Required | Description |
| --- | --- | --- |
| `query` | yes | SignalFlow program containing the published result to measure |
| `realm` | yes unless `streamURL` is set | Splunk Observability realm, e.g. `us0` |
| `accessToken` | yes unless `SIGNALFX_ACCESS_TOKEN` is set | Access token for the realm |
| `duration` | yes | Positive integer seconds for the measurement window |
| `aggregator` | yes | One of `max`, `min`, `avg`, `sum`, `count`, `latest` |
| `streamURL` | no | Full WebSocket endpoint override for a tested non-default deployment. When set, `realm` is not required. Must be `ws://` or `wss://` with a host. |

`aggregator` semantics across the window:

- `max`, `min`, `avg`, `sum` over all numeric payloads
- `count` is the number of numeric payloads
- `latest` is the last numeric payload received

All payload value types `double`, `long` (`int64`), and `int` (`int32`) are converted to `float64`. Non-finite values and unknown aggregators return an error.

## Installation

Argo Rollouts loads metric provider plugins through `argo-rollouts-config`.

### HTTPS location (release)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argo-rollouts-config
data:
  metricProviderPlugins: |-
    - name: "js4683/rollouts-plugin-metric-signalfx"
      location: "https://github.com/js4683/rollouts-plugin-metric-signalfx/releases/download/v0.1.0/metric-plugin-linux-amd64"
```

Add the exact `sha256` checksum published with the release to the entry when a release exists. For unreleased or development use, omit `sha256`.

### File location

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argo-rollouts-config
data:
  metricProviderPlugins: |-
    - name: "js4683/rollouts-plugin-metric-signalfx"
      location: "file:///opt/argo/rollouts/plugins/metric-plugin-signalfx"
```

`file://` requires the binary to be mounted into the controller container (init container, shared volume, or baked image). The controller does not start if the plugin is not available at the configured location.

## Authentication

Production use must inject `SIGNALFX_ACCESS_TOKEN` from a Kubernetes Secret into the Rollouts controller. The plugin subprocess inherits the controller's environment.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: signalfx
stringData:
  token: <splunk-access-token>
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: argo-rollouts
spec:
  template:
    spec:
      containers:
        - name: argo-rollouts
          env:
            - name: SIGNALFX_ACCESS_TOKEN
              valueFrom:
                secretKeyRef:
                  name: signalfx
                  key: token
```

Inline `accessToken` in an `AnalysisTemplate` is supported for local testing only. Do not commit tokens to Git. The inline value, when present, takes precedence over the environment variable. The plugin never logs the token or returns it in metadata. `GetMetadata` only returns `ResolvedSignalFlowQuery`.

## Example

See `examples/analysis-template.yaml` for a complete `AnalysisTemplate`:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: signalfx-latency
spec:
  metrics:
    - name: p95-latency
      interval: 5m
      successCondition: result < 200
      failureLimit: 1
      provider:
        plugin:
          js4683/rollouts-plugin-metric-signalfx:
            realm: us0
            query: |
              data('demo.trans.latency').max().publish()
            duration: 60
            aggregator: latest
```

The example query and thresholds are illustrative. Substitute a SignalFlow program that publishes one result and SLO thresholds that match the service under analysis. The provider key, field names, duration units, and `aggregator` values are part of the contract.

## Building and verifying locally

```bash
make fmt
go test -race ./...
go vet ./...
make build
```

Artifacts:

- `dist/metric-plugin-linux-amd64`
- `dist/metric-plugin-linux-arm64`

Release binaries are not committed. Each release publishes SHA-256 checksums alongside the binaries.

## References

- Issue: https://github.com/argoproj/argo-rollouts/issues/1046
- Plugin docs: https://argo-rollouts.readthedocs.io/en/stable/analysis/plugins/
- Sample plugin: https://github.com/argoproj-labs/rollouts-plugin-metric-sample-prometheus
- SignalFlow client: https://github.com/signalfx/signalflow-client-go
