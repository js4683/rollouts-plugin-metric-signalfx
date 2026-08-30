# Local Fake SignalFlow and Argo Smoke Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Strengthen the released SignalFx metric plugin with executable-level RPC coverage, protocol-valid SignalFlow computation-error coverage, and a disposable kind smoke test that exercises the released binary through an Argo Rollouts `AnalysisRun` without a Splunk account.

**Architecture:** Keep production behavior and the published v0.1.0 binary unchanged. Use the pinned client library's `FakeBackend` for deterministic data streams and a tiny test-only WebSocket handler for the terminal computation-error protocol case, package the data fake into a temporary Linux image, and let Argo Rollouts download and launch the already published plugin binary. The smoke test uses a fake token and `streamURL`, verifies the observed `42` measurement, checks the controller does not log the token, and deletes its kind cluster on exit. Test log noise is suppressed only in test logger setup.

**Tech Stack:** Go 1.26.1+, Argo Rollouts v1.10.0, HashiCorp go-plugin v1.7.0, SignalFlow client v2.3.0 `FakeBackend`, Docker/Colima, kind, kubectl, GitHub Actions.

**Spec:** `PLAN.md` and `README.md`

## Global Constraints

- Keep module path `github.com/js4683/rollouts-plugin-metric-signalfx`.
- Keep provider key `js4683/rollouts-plugin-metric-signalfx` and `Type()` value `RPCPlugin`.
- Keep Argo Rollouts pinned to v1.10.0 and SignalFlow client v2.3.0.
- Keep the production plugin's one-client-per-`Run`, bounded synchronous collection, and no-transparent-retry behavior.
- Use only the fake token `abcd` in the test harness; never accept or print a real token.
- Keep `dist/` and generated images/binaries out of Git.
- The kind smoke test must use the published v0.1.0 binary and its exact SHA-256 checksum.
- The kind smoke test must delete only the cluster it created, even when a command fails.
- The test-only fake SignalFlow server must publish exactly `data('demo').publish()` with value `42`.
- No live Splunk account, token, or network-side metric ingestion is required for automated tests.

---

### Task 1: Executable-Level RPC Smoke Test

**Files:**
- Create: `rpc_binary_test.go`
- Read: `main.go`
- Read: `internal/plugin/plugin.go`
- Read: `internal/plugin/stream_test.go`

**Interfaces:**
- Consumes: `handshakeConfig`, `RpcMetricProviderPlugin`, `signalflow.NewRunningFakeBackend`, and the released plugin's `streamURL` configuration.
- Produces: `TestBuiltPluginBinaryUsesFakeSignalFlow`, which builds the current executable in `t.TempDir()`, launches it through `go-plugin.ClientConfig.Cmd`, dispenses `RpcMetricProviderPlugin`, and runs one metric over the fake WebSocket endpoint.

- [x] **Step 1: Write the failing process test**

Create `rpc_binary_test.go` with the following observable contract:

```go
func TestBuiltPluginBinaryUsesFakeSignalFlow(t *testing.T) {
	const program = "data('demo').publish()"
	fake := signalflow.NewRunningFakeBackend()
	defer fake.Stop()
	fake.AddProgramTSIDs(program, []idtool.ID{idtool.ID(1)})
	fake.SetTSIDFloatData(idtool.ID(1), 42)

	binaryPath := buildPluginBinary(t)
	client, provider := startBinaryProvider(t, binaryPath)
	defer client.Kill()

	metric := v1alpha1.Metric{
		SuccessCondition: "result >= 42",
		Provider: v1alpha1.MetricProvider{Plugin: map[string]json.RawMessage{
				signalfxPlugin.PluginName: json.RawMessage(fmt.Sprintf(
					`{"query":%q,"streamURL":%q,"accessToken":"abcd","duration":2,"aggregator":"latest"}`,
				program, fake.URL())),
		}},
	}

	measurement := provider.Run(nil, metric)
	if measurement.Phase != v1alpha1.AnalysisPhaseSuccessful {
		t.Fatalf("phase = %q, message = %q", measurement.Phase, measurement.Message)
	}
	if measurement.Value != "42" {
		t.Fatalf("value = %q, want 42", measurement.Value)
	}
	if measurement.StartedAt == nil || measurement.FinishedAt == nil {
		t.Fatal("measurement timestamps missing")
	}
}
```

`buildPluginBinary(t)` must set `binaryPath := filepath.Join(t.TempDir(), "metric-plugin")`, run `go build -trimpath -o "$binaryPath" .` with the repository root as its working directory, and return `binaryPath`. `startBinaryProvider(t, binaryPath)` must launch `exec.Command(binaryPath)`, use `handshakeConfig`, register a client-side `RpcMetricProviderPlugin`, call `client.Client()`, dispense `RpcMetricProviderPlugin`, and assert the dispensed value implements `rolloutsPlugin.MetricProviderPlugin`.

- [x] **Step 2: Run the new test to establish the missing-helper failure**

Run: `go test ./... -run TestBuiltPluginBinaryUsesFakeSignalFlow -count=1 -v`

Expected: FAIL with undefined-symbol errors for `buildPluginBinary` and `startBinaryProvider`.

- [x] **Step 3: Implement only the test helpers**

Implement `buildPluginBinary(t *testing.T) string` and `startBinaryProvider(t *testing.T, binaryPath string) (*goPlugin.Client, rolloutsPlugin.MetricProviderPlugin)` in `rpc_binary_test.go`. Resolve the repository root from the test file with `runtime.Caller`, use `t.TempDir()` for the output, call `cmd.Run()` with `CombinedOutput()`, and fail with the command output if the build fails. Use `t.Cleanup` or the test's caller-owned `client.Kill()` to ensure the child process is terminated.

- [x] **Step 4: Run the process test and the existing RPC test**

Run: `go test ./... -run 'Test(BuiltPluginBinaryUsesFakeSignalFlow|RPCProviderContract)$' -count=1 -v`

Expected: both tests PASS, the measurement value is `42`, and the fake backend has received and completed a real client computation from the separately launched plugin process.

- [x] **Step 5: Commit the isolated test**

```bash
git add rpc_binary_test.go
git commit -s -m "test: exercise built plugin binary over RPC"
```

### Task 2: SignalFlow Error and Cleanup Coverage

**Files:**
- Modify: `internal/plugin/stream_test.go:3-16,101-115,162-164`
- Modify: `internal/plugin/plugin_provider_test.go:3-13,47,63,85`
- Modify: `go.mod:5-11`
- Modify: `go.sum`

**Interfaces:**
- Consumes: a test-only WebSocket error handler, `collectSignalFlow`, and the existing `stopAndDrain` cleanup boundary.
- Produces: a test that observes a protocol-valid computation error and quiet test output without changing production logging, error propagation, or the two-second drain budget.

- [x] **Step 1: Replace the cancellation substitute with a protocol-valid computation error**

Change `TestCollectSignalFlowReturnsComputationError` to use a test-only `httptest.Server` upgraded with the already transitive `github.com/gorilla/websocket` module. Promote that module to a direct test dependency with `go mod tidy`. The handler must reply `{"type":"authenticated"}` to the client's authenticate message, reply with a JSON error containing `"synthetic SignalFlow failure"` and the channel value from the execute request, and close the WebSocket immediately after that error. Configure a client with `signalflow.StreamURL(testServerURL)` and `signalflow.AccessToken("abcd")`, call `collectSignalFlow` with a live context, and assert the returned error can be unwrapped as `*signalflow.ComputationError` with `Message == "synthetic SignalFlow failure"`. Close the client and HTTP server in deferred cleanup after the handler has stopped sending. Also use a two-second fake-backed window in `TestRunMapsPhases`, because the fake backend's default one-second resolution otherwise races the first sample.

- [x] **Step 2: Run the focused error test before production changes**

Run: `go test ./internal/plugin -run TestCollectSignalFlowReturnsComputationError -count=1 -v`

Expected: PASS if the current collector propagates `comp.Err()` correctly and the server closes cleanly; a failure must identify protocol or cleanup behavior rather than relying on `FakeBackend.AddProgramError`, whose continued post-error stream is intentionally avoided.

- [x] **Step 3: Suppress expected fake-backend cleanup output in test logger setup**

Import `io` in `internal/plugin/stream_test.go` and change `testLogger()` to discard test-only log output. Update provider tests in `internal/plugin/plugin_provider_test.go` to use `testLogger()` instead of the default logrus logger:

```go
func testLogger() log.Entry {
	logger := log.New()
	logger.SetOutput(io.Discard)
	return *logger.WithField("test", "signalflow")
}
```

Do not change `stopAndDrain`; it must retain its existing callback-error behavior, independent `drainTimeout`, and `nil` return when the backend does not close before the cleanup budget expires. The test output change must not alter the published binary.

- [x] **Step 4: Run stream tests under the race detector**

Run: `go test -race ./internal/plugin -run 'Test(PayloadValues|CollectSignalFlow|Run)' -count=1 -v`

Expected: PASS with no fake-backend drain messages emitted by the test logger, no goroutine leak, and no race report.

- [x] **Step 5: Commit the coverage and logging change**

```bash
git add internal/plugin/stream.go internal/plugin/stream_test.go
git commit -s -m "test: cover SignalFlow computation errors"
```

### Task 3: Disposable Fake SignalFlow Service

**Files:**
- Create: `test/fake-signalflow/main.go`
- Create: `test/fake-signalflow/Dockerfile`
- Create: `.dockerignore`

**Interfaces:**
- Consumes: SignalFlow client v2.3.0 `FakeBackend`, the fixed program `data('demo').publish()`, and `idtool.ID`.
- Produces: a Linux HTTP/WebSocket service listening on `:8080`, accepting the fake token `abcd`, and streaming one `42` value for the smoke `AnalysisRun`.

- [x] **Step 1: Write the fake service entrypoint**

Create `test/fake-signalflow/main.go` with this behavior:

```go
const program = "data('demo').publish()"

func main() {
	fake := signalflow.NewRunningFakeBackend()
	defer fake.Stop()
	fake.AddProgramTSIDs(program, []idtool.ID{idtool.ID(1)})
	fake.SetTSIDFloatData(idtool.ID(1), 42)

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	log.Fatal(http.ListenAndServe(addr, fake))
}
```

Use no real credentials and no configurable program/value; fixed behavior makes the smoke test deterministic.

- [x] **Step 2: Add a minimal static test image**

Create `test/fake-signalflow/Dockerfile` that uses `golang:1.26` to compile `./test/fake-signalflow` with `CGO_ENABLED=0 GOOS=linux`, then copies the resulting binary into a `scratch` final image with entrypoint `/fake-signalflow`. Add `.dockerignore` entries for `.git`, `dist`, documentation, examples, and other non-build inputs so local release artifacts and notes never enter the Docker build context. Do not add this image to `make build` or release artifacts.

- [x] **Step 3: Build the test image**

Run: `docker build -f test/fake-signalflow/Dockerfile -t signalfx-fake:local .`

Expected: the image builds without changing `go.mod` or `go.sum`.

- [x] **Step 4: Run the fake service locally**

Run: `docker run --rm -d --name signalfx-fake-local -p 18080:8080 signalfx-fake:local`, then stop it with `docker rm -f signalfx-fake-local` after confirming the container is running. The service is only a WebSocket test double; it is not a SignalFx-compatible cloud service for production use.

- [x] **Step 5: Commit the disposable test service source**

```bash
git add test/fake-signalflow
git commit -s -m "test: add local fake SignalFlow service"
```

### Task 4: Argo Rollouts kind Smoke Test

**Files:**
- Create: `test/kind-smoke.sh`
- Modify: `README.md:5,43-56,131-145`
- Modify: `Makefile:4`
- Modify: `.github/workflows/build.yml:23-24`

**Interfaces:**
- Consumes: `kind`, Docker, the public v0.1.0 plugin assets, the fake service image, and the Argo Rollouts v1.10.0 install manifest.
- Produces: a shell smoke test that creates and deletes only `argo-sfx-smoke`, loads the fake service, makes Argo download the correct architecture-specific plugin with SHA-256 verification, injects the fake token through a Secret, and waits for a successful one-shot `AnalysisRun`.

- [x] **Step 1: Write the smoke script with cleanup first**

Create `test/kind-smoke.sh` with `set -euo pipefail`, a fixed cluster name `argo-sfx-smoke`, and a trap that runs `kind delete cluster --name argo-sfx-smoke` on every exit after creation. Use context `kind-argo-sfx-smoke` for every kubectl command.

The script must:

```text
1. Require docker, kind, kubectl, and a running Docker daemon.
2. Build signalfx-fake:local from test/fake-signalflow/Dockerfile.
3. Create kindest/node:v1.31.4 cluster argo-sfx-smoke and load signalfx-fake:local.
4. Create namespace signalfx-test and deploy the fake service at port 8080.
5. Apply the Argo Rollouts v1.10.0 manifest with --server-side --force-conflicts.
6. Detect the node architecture and select the published amd64 or arm64 asset/hash.
7. Apply argo-rollouts-config with the plugin URL, exact sha256, and no realm requirement.
8. Apply Secret signalfx with fake value abcd and patch the controller env from that Secret.
9. Restart the controller and wait for its rollout.
10. Apply an AnalysisRun using streamURL ws://fake-signalflow.signalfx-test.svc.cluster.local:8080,
    query data('demo').publish(), duration 2, aggregator latest, and successCondition result >= 42.
11. Poll status.metricResults[0].measurements[0].value until phase Successful or fail on Failed/Error/timeout.
12. Assert the measurement value is exactly 42 and controller logs do not contain abcd.
13. Print the AnalysisRun status and let the trap delete the cluster.
```

Use these release assets and hashes:

```text
amd64: https://github.com/js4683/rollouts-plugin-metric-signalfx/releases/download/v0.1.0/metric-plugin-linux-amd64
       cefb4120ee5d29e11ed2fa5efde4389be09e59f990bc14166c447a43e5a40442
arm64: https://github.com/js4683/rollouts-plugin-metric-signalfx/releases/download/v0.1.0/metric-plugin-linux-arm64
       70aaff2cbedf61ae5d03074d974776a14b527273cb39dc16b66d522ef064cfaa
```

- [x] **Step 2: Update release documentation**

Change `README.md` status to `v0.1.0 initial release; metric plugins remain alpha in Argo Rollouts`. Add the amd64 `sha256` to the HTTPS ConfigMap example and state the arm64 hash next to the release asset list. Add a `Testing without Splunk` section documenting `go test -race ./...`, the in-process FakeBackend tests, `go test ./... -run TestBuiltPluginBinaryUsesFakeSignalFlow`, and `bash test/kind-smoke.sh` prerequisites and cleanup behavior. State explicitly that the kind test validates the released binary and Argo wiring against a fake SignalFlow service, not live cloud authentication. Extend the `Makefile` `fmt` target and CI formatting command to include `rpc_binary_test.go` and `test/fake-signalflow/*.go`.

- [x] **Step 3: Run the smoke test on the local arm64 Docker runtime**

Run: `bash test/kind-smoke.sh`

Expected: the controller reaches `Running`, the plugin downloads the arm64 v0.1.0 asset with the matching checksum, the `AnalysisRun` reaches `Successful`, its first measurement is `42`, no `abcd` token appears in controller logs, and `kind get clusters` no longer lists `argo-sfx-smoke` after exit.

- [x] **Step 4: Commit the smoke harness and documentation**

```bash
git add test/kind-smoke.sh README.md Makefile .github/workflows/build.yml .dockerignore
git commit -s -m "test: add Argo Rollouts fake SignalFlow smoke test"
```

### Task 5: Final Verification and Release Follow-Up

**Files:**
- Read: `go.mod`
- Read: `go.sum`
- Read: `README.md`
- Read: `test/kind-smoke.sh`
- Read: `test/fake-signalflow/main.go`

**Interfaces:**
- Consumes: all production and test targets from Tasks 1-4.
- Produces: a clean, documented, no-account validation result without changing v0.1.0 assets.

- [x] **Step 1: Format and run the full test suite**

Run:

```bash
gofmt -w main.go rpc_test.go rpc_binary_test.go internal/plugin/*.go test/fake-signalflow/main.go
go test -race ./...
go vet ./...
```

Expected: formatting produces no diff, all tests pass under the race detector, and vet is clean.

- [x] **Step 2: Build current binaries and verify the published release assets independently**

Run:

```bash
make build
shasum -a 256 dist/metric-plugin-linux-amd64 dist/metric-plugin-linux-arm64
```

The current source-build hashes are allowed to differ from v0.1.0 because Go embeds the current VCS revision and dirty state in the executable. Verify that the already-published v0.1.0 assets still have their release digests by downloading the public assets and hashing those downloads. Expected public-release hashes are:

```text
cefb4120ee5d29e11ed2fa5efde4389be09e59f990bc14166c447a43e5a40442  dist/metric-plugin-linux-amd64
70aaff2cbedf61ae5d03074d974776a14b527273cb39dc16b66d522ef064cfaa  dist/metric-plugin-linux-arm64
```

- [x] **Step 3: Scan tracked content for secrets and local paths**

Run: `git grep -n -i -E 'BEGIN .*PRIVATE KEY|password[[:space:]]*:|api[_-]?key[[:space:]]*:|/Users/|SIGNALFX_ACCESS_TOKEN|accessToken' -- ':!go.sum'`

Expected: only configuration field names, environment-variable names, fake test value `abcd`, documentation example values, and Secret references appear. No real credential or absolute local path may appear.

- [x] **Step 4: Inspect final repository state**

Run: `git diff --check && git status --short -uall && git log --oneline -5`

Expected: no whitespace errors, only intended source/docs changes, generated `dist/` and local Docker state ignored, and no release asset changes.

- [ ] **Step 5: Commit final fixes and push the feature branch through the configured gate**

Use `no-mistakes axi run --intent "Strengthen the no-account validation of the public Argo Rollouts SignalFx metric plugin with executable RPC coverage, FakeBackend computation-error coverage, a disposable kind smoke test using the released binary, and accurate release documentation."` on a non-default branch. Resolve only findings that do not alter the deliberate out-of-tree alpha-plugin scope, then push the intended commits to `origin` after the gate passes.
