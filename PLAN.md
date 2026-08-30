# SignalFx Metric Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and locally verify an Argo Rollouts RPC metric plugin that evaluates bounded SignalFlow measurements from Splunk Observability Cloud.

**Architecture:** The executable serves one HashiCorp go-plugin implementation of Argo Rollouts' `MetricProviderPlugin`. Each `Run` parses per-metric configuration, creates one SignalFlow client, collects and drains a bounded computation, reduces numeric payloads to one value, and delegates success/failure condition evaluation to Argo Rollouts. The first release has no shared client cache, retry worker, or asynchronous job registry.

**Tech Stack:** Go 1.26.1+, Argo Rollouts v1.10.0, HashiCorp go-plugin v1.7.0, SignalFlow client v2.3.0, logrus v1.9.3, SignalFlow FakeBackend, GitHub Actions.

**Spec:** `../argo-rollouts-signalfx-plugin.md`

## Global Constraints

- Use module path `github.com/js4683/rollouts-plugin-metric-signalfx`.
- Use provider key `js4683/rollouts-plugin-metric-signalfx` and return `RPCPlugin` from `Type()`.
- Pin Argo Rollouts to v1.10.0 and use its compatible go-plugin v1.7.0 and logrus v1.9.3 dependencies.
- Pin `github.com/signalfx/signalflow-client-go/v2` to v2.3.0.
- Accept positive integer `duration` values in seconds.
- Accept exactly `max`, `min`, `avg`, `sum`, `count`, or `latest` as aggregators.
- Resolve `accessToken` from metric configuration first and `SIGNALFX_ACCESS_TOKEN` second.
- Never log, return in metadata, or commit an access token.
- Use `streamURL` when supplied; otherwise derive the endpoint from `realm`.
- Create and close one SignalFlow client per `Run`.
- Return stream failures to Argo Rollouts without transparent retries.
- Keep `Resume`, `Terminate`, and `GarbageCollect` idempotent and non-persistent.
- Run all automated tests without live Splunk credentials.
- Build Linux amd64 and arm64 binaries but keep `dist/` untracked.
- Do not commit, push, create a GitHub repository, open a pull request, publish a release, or comment on issue #1046 without an explicit request.

## File Map

- `main.go`: RPC handshake, provider construction, and `go-plugin.Serve`.
- `rpc_test.go`: root-package handshake and process-level RPC contract tests.
- `internal/plugin/config.go`: provider-key lookup, JSON parsing, token precedence, and endpoint validation.
- `internal/plugin/config_test.go`: table tests for every configuration rule.
- `internal/plugin/aggregation.go`: pure numeric reduction.
- `internal/plugin/aggregation_test.go`: aggregator and invalid-number tests.
- `internal/plugin/stream.go`: SignalFlow client construction, payload conversion, collection, stop, and drain.
- `internal/plugin/stream_test.go`: FakeBackend and resource-lifecycle tests.
- `internal/plugin/plugin.go`: Argo metric-provider lifecycle and measurement mapping.
- `internal/plugin/plugin_test.go`: phase, metadata, timestamp, and lifecycle tests.
- `examples/analysis-template.yaml`: complete provider configuration example.
- `README.md`: installation, authentication, behavior, compatibility, and release use.
- `Makefile`: format, race-test, vet, and cross-build targets.
- `.github/workflows/build.yml`: CI equivalents of local quality gates.
- `.gitignore`: generated binaries, coverage, and editor state.
- `LICENSE`: Apache-2.0.

---

### Task 1: Repository and RPC Scaffold

**Files:**
- Create: `main.go`
- Create: `rpc_test.go`
- Modify: `internal/plugin/plugin.go`
- Modify: `internal/plugin/plugin_test.go`
- Modify: `go.mod`
- Modify: `go.sum`
- Create: `Makefile`
- Create: `.github/workflows/build.yml`
- Create: `.gitignore`
- Create: `LICENSE`

**Interfaces:**
- Consumes: Argo `rpc.MetricProviderPlugin` and HashiCorp `go-plugin` protocol version 1.
- Produces: `handshakeConfig`, `pluginMap(log.Entry) map[string]goPlugin.Plugin`, and a compiling `RpcPlugin` implementation.

- [x] Initialize a local Git repository on `feat/signalfx-metric-plugin` with no remote.
- [x] Initialize module `github.com/js4683/rollouts-plugin-metric-signalfx`.
- [x] Write provider contract tests before `RpcPlugin` exists and observe the expected undefined-type failure.
- [x] Add the minimal provider shell and observe `TestRpcPluginType` and `TestRpcPluginScaffoldLifecycle` pass.
- [x] Write `rpc_test.go` first, asserting handshake cookie values and the `RpcMetricProviderPlugin` map entry; run `go test . -run TestEntrypointContract -v` and observe undefined entry-point symbols.
- [x] Add `main.go` with `ARGO_ROLLOUTS_RPC_PLUGIN=metricprovider`, protocol version 1, and map key `RpcMetricProviderPlugin`; rerun the focused test to green.
- [x] Set `go 1.26.1` and direct requirements for Argo v1.10.0, go-plugin v1.7.0, and logrus v1.9.3; run `go mod tidy` and confirm there is no unrelated fork replacement.
- [x] Add `fmt`, `test`, `vet`, and `build` Make targets. `test` runs `go test -race ./...`; `build` writes Linux amd64 and arm64 binaries under `dist/`.
- [x] Add CI that runs dependency download, `gofmt -d`, race tests, vet, and release builds.
- [x] Add `.gitignore` and Apache-2.0 `LICENSE`.
- [x] Run `gofmt -d main.go rpc_test.go internal/plugin/*.go`, `go test ./...`, and `go vet ./...`; all must exit cleanly.

### Task 2: Configuration Contract

**Files:**
- Create: `internal/plugin/config.go`
- Create: `internal/plugin/config_test.go`

**Interfaces:**
- Produces: `Config`, `parseConfig(metric v1alpha1.Metric) (Config, error)`, `parseConfigJSON(raw json.RawMessage, envToken string) (Config, error)`, and `validateStreamURL(string) error`.

```go
type Config struct {
	Query       string `json:"query"`
	Realm       string `json:"realm"`
	AccessToken string `json:"accessToken"`
	Duration    int    `json:"duration"`
	Aggregator  string `json:"aggregator"`
	StreamURL   string `json:"streamURL"`
}
```

- [x] Write table tests for inline-token success, environment-token fallback, inline-token precedence, `streamURL` without realm, malformed JSON, missing provider map, missing provider key, blank query, blank token, non-positive duration, unsupported aggregator, missing realm, valid `ws://`, valid `wss://`, rejected `http://`, and URL without host.
- [x] Run `go test ./internal/plugin -run 'TestParseConfig|TestValidateStreamURL' -v`; observe failures because parser symbols are absent.
- [x] Implement constants `PluginName`, `AccessTokenEnv`, and `ResolvedQueryKey`, then add minimal parsing and validation to satisfy the table.
- [x] Ensure validation errors identify fields but never include query text or token values.
- [x] Run the focused tests and `go test ./internal/plugin`; both must pass.

### Task 3: Numeric Aggregation

**Files:**
- Create: `internal/plugin/aggregation.go`
- Create: `internal/plugin/aggregation_test.go`

**Interfaces:**
- Produces: `aggregate(values []float64, aggregator string) (float64, error)`.

- [x] Write tests proving `[10,20,30]` produces max 30, min 10, avg 20, sum 60, count 3, and latest 30.
- [x] Write tests proving empty input, unknown aggregator, NaN, positive infinity, negative infinity, and a sum that overflows to infinity return errors.
- [x] Run `go test ./internal/plugin -run TestAggregate -v`; observe the missing-function failure.
- [x] Implement a one-pass reducer initialized from the first value, with finite checks before and after summation.
- [x] Run the focused test and `go test ./internal/plugin`; both must pass.

### Task 4: SignalFlow Collection

**Files:**
- Create: `internal/plugin/stream.go`
- Create: `internal/plugin/stream_test.go`

**Interfaces:**
- Produces: `newSignalFlowClient(Config, log.Entry) (*signalflow.Client, error)`.
- Produces: `payloadValues(*messages.DataMessage) ([]float64, error)`.
- Produces: `collectSignalFlow(context.Context, *signalflow.Client, Config, log.Entry) (float64, error)`.
- Produces: `stopAndDrain(*signalflow.Computation, func(*messages.DataMessage) error, log.Entry) error`.

- [x] Write direct payload tests for double, int64, int32, unsupported type, and non-finite values.
- [x] Write FakeBackend tests for a three-series stream, empty data, a synthetic backend computation error, parent-context cancellation, and zero running jobs after normal stop.
- [x] Run `go test ./internal/plugin -run 'TestPayloadValues|TestCollectSignalFlow|TestStopAndDrain' -v`; observe missing-function failures.
- [x] Add direct requirement `github.com/signalfx/signalflow-client-go/v2 v2.3.0` when the stream package first imports it.
- [x] Implement client construction with realm or `streamURL`, access token, and an error callback that never logs configuration.
- [x] Implement bounded collection with a duration timer, parent/hard deadline priority checks, computation error propagation, payload processing, and aggregation.
- [x] Implement stop/drain with a two-second independent cleanup budget and no untracked goroutine.
- [x] Run focused tests, `go test -race ./internal/plugin`, and `go vet ./internal/plugin`; all must pass.

### Task 5: Argo Measurement Provider

**Files:**
- Modify: `internal/plugin/plugin.go`
- Modify: `internal/plugin/plugin_test.go`

**Interfaces:**
- Consumes: configuration, client, collector, Argo `evaluate.EvaluateResult`, and `metricutil.MarkMeasurementError`.
- Produces: complete `InitPlugin`, `Run`, `Resume`, `Terminate`, `GarbageCollect`, `Type`, and `GetMetadata` behavior.

- [x] Add tests that use FakeBackend value 42 to prove Successful, Failed, and Inconclusive phases.
- [x] Add tests for config error, empty data, backend error, expression error, start/finish timestamps, formatted value `42`, resolved-query metadata, metadata parse error, and absence of token metadata.
- [x] Run `go test ./internal/plugin -run 'TestRun|TestGetMetadata|TestProviderLifecycle' -v`; observe failures against the provider shell.
- [x] Replace the shell `Run` with parse, client-create, collect, format, evaluate, and timestamp flow. Preserve the collected value when condition evaluation fails.
- [x] Implement query-only metadata and retain pass-through lifecycle methods.
- [x] Run focused tests, `go test -race ./internal/plugin`, and `go vet ./internal/plugin`; all must pass.

### Task 6: Process-Level RPC Contract

**Files:**
- Modify: `rpc_test.go`

**Interfaces:**
- Consumes: `handshakeConfig`, `pluginMap`, and complete `RpcPlugin`.
- Produces: in-process RPC proof using `goPlugin.ServeTestConfig` and `ReattachConfig`.

- [x] Extend the root test to start the plugin server, dispense `RpcMetricProviderPlugin`, call every provider method, and close within two seconds.
- [x] Run `go test . -run TestRPCProviderContract -v`; diagnose any handshake or serialization error from its exact output.
- [x] Run `go test -race ./...` and `go vet ./...`; both must pass.

### Task 7: User and Release Documentation

**Files:**
- Create: `README.md`
- Create: `examples/analysis-template.yaml`

**Interfaces:**
- Documents: provider key, six config fields, token precedence, installation locations, measurement semantics, compatibility, and build commands.

- [x] Add an `AnalysisTemplate` using realm `us0`, duration 60, aggregator `latest`, and `data('demo.trans.latency').max().publish()` with `result < 200`.
- [x] Document `metricProviderPlugins` installation by HTTPS and `file://`; omit `sha256` from unreleased examples and require the exact published checksum once a release exists.
- [x] Document `SIGNALFX_ACCESS_TOKEN` injection from a Kubernetes Secret and mark inline `accessToken` as local-testing only.
- [x] Document all aggregators, bounded stream behavior, error/no-retry behavior, lifecycle no-ops, `streamURL`, Argo plugin alpha status, and v1.10.0 compatibility.
- [x] Document `make fmt`, `go test -race ./...`, `go vet ./...`, and `make build`.
- [x] Search README, example, and code for provider/config-name consistency.

## Final Verification

- [x] `gofmt -d main.go rpc_test.go internal/plugin/*.go` prints nothing.
- [x] `go test -race ./...` passes.
- [x] `go vet ./...` passes.
- [x] `make build` creates `dist/metric-plugin-linux-amd64` and `dist/metric-plugin-linux-arm64`.
- [x] `file dist/metric-plugin-linux-amd64 dist/metric-plugin-linux-arm64` reports Linux executables with matching architectures.
- [x] `git status --short` shows source and documentation only; `dist/` is ignored.
- [x] Searches for token-like values find only field and environment-variable names, never a credential.
- [x] The final diff contains no unrelated files, debug output, fork replacement, or generated binary.
