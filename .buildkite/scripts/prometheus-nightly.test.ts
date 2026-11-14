import { spawnSync } from "child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { parse } from "yaml";

const PROMCHECK_ENV = parse(readFileSync(resolve(import.meta.dirname, "../pipelines/pull-request-validate-prometheus-compliance.yml"), "utf8")).env;
const SCRIPT = resolve(import.meta.dirname, "validate-prometheus-compliance.sh");
const COUNTS = { type: "summary", total: 6, inspected: 6, match: 1, mismatch: 1, es_4xx: 1, es_5xx: 1, error: 1, request_filtered: 0, response_filtered: 0, flaky: 1 };
const VERDICT = JSON.stringify(COUNTS);

describe("validate-prometheus-compliance.sh nightly", () => {
  let dir: string;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "prometheus-nightly-"));
    mkdirSync(join(dir, "bin"));
    // Only external services and the downloaded executable are replaced. Real jq parses the verdict
    // and builds the indexed document; no Elasticsearch build or remote writes occur in these tests.
    writeFileSync(join(dir, "promcheck"), '#!/bin/bash\nprintf "%s\\n" "$*" > "$TEST_DIR/args.log"\nprintf "nightly diagnostics\\n" >&2\nprintf "%s\\n" "$TEST_LOG"\nexit "$TEST_RC"\n', { mode: 0o755 });
    // The runner sizes Linux CI nodes via getconf; these tests run on developer machines too.
    writeFileSync(join(dir, "bin", "getconf"), '#!/bin/bash\ncase "$1" in _NPROCESSORS_ONLN) echo 8;; _PHYS_PAGES) echo 2097152;; PAGESIZE) echo 4096;; *) exit 1;; esac\n', { mode: 0o755 });
    writeFileSync(join(dir, "bin", "vault"), `#!/bin/bash\nprintf '%s' '{"data":{"url":"https://vault-results.example","api_key":"vault-key"}}'\n`, { mode: 0o755 });
    writeFileSync(join(dir, "bin", "curl"), `#!/bin/bash
out= url=
while (($#)); do
  case "$1" in
    -H) [[ $2 == @- ]] && cat >/dev/null; shift ;;
    -o) out=$2; shift ;;
    --data-binary) cp "\${2#@}" "$TEST_DIR/upload.json"; shift ;;
    https://*) url=$1 ;;
  esac
  shift
done
case "$url" in
  */releases/tags/*) printf '{"assets":[{"name":"promcheck-linux-amd64","url":"https://assets/binary"},{"name":"promcheck-linux-arm64","url":"https://assets/binary"},{"name":"grafanalabs-top100.tar.gz","url":"https://assets/corpus"}]}' ;;
  https://assets/binary) cp "$TEST_DIR/promcheck" "$out" ;;
  https://assets/corpus) touch "$out" ;;
  */_create/*) printf '%s' "$url" > "$TEST_DIR/upload.url"; printf '%s' "$TEST_RESPONSE" > "$out"; printf '%s' "$TEST_HTTP_STATUS"; exit "$TEST_UPLOAD_RC" ;;
  *) exit 99 ;;
esac
`, { mode: 0o755 });
  });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  function run(overrides: Record<string, string> = {}) {
    return spawnSync("bash", [SCRIPT, "nightly"], {
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${join(dir, "bin")}:${process.env.PATH}`,
        ...PROMCHECK_ENV,
        GH_TOKEN: "fake-github-token",
        GRADLEW: "./gradlew --build-cache --scan",
        PROMCHECK_RESULTS_NAMESPACE: "experimental",
        PROMCHECK_RESULTS_URL: "https://results.example/",
        PROMCHECK_RESULTS_API_KEY: "fake-results-key",
        PROMCHECK_OUTPUT_DIR: join(dir, "output"),
        BUILDKITE_BUILD_ID: "build-123",
        BUILDKITE_BRANCH: "main",
        BUILDKITE_BUILD_URL: "https://buildkite.com/elastic/promcheck/builds/123",
        TEST_DIR: dir,
        TEST_LOG: VERDICT,
        TEST_RC: "1",
        TEST_RESPONSE: '{"result":"created"}',
        TEST_UPLOAD_RC: "0",
        TEST_HTTP_STATUS: "201",
        BUILDKITE_BUILD_NUMBER: "123",
        BUILDKITE_PIPELINE_SLUG: "elasticsearch-pull-request-validate-prometheus-compliance",
        ...overrides,
      },
    });
  }

  test("indexes mismatches and errors as results, with provenance", () => {
    const r = run();
    expect(r.status, r.stdout + r.stderr).toBe(0);
    const result = JSON.parse(readFileSync(join(dir, "upload.json"), "utf8"));
    expect(JSON.parse(readFileSync(join(dir, "output", "counts.json"), "utf8"))).toEqual({ ...COUNTS, errors: 3 });
    expect(result).toMatchObject({
      status: "ok", dataset: "grafanalabs-top100",
      data_stream: { type: "synthetics", dataset: "promql.support", namespace: "experimental" },
      git: { branch: "main" }, promcheck: { version: PROMCHECK_ENV.PROMCHECK_VER },
      buildkite: { build_id: "build-123", build_number: 123, pipeline: "elasticsearch-pull-request-validate-prometheus-compliance" },
      compliance: { input_total: 6, executed_total: 6, executed_success: 1, executed_failure: 1, executed_error: 3, executed_skip: 1, success_pct: 16.7 },
    });
    expect(result.git.commit).toMatch(/^[a-f0-9]{40}$/);
    expect(readFileSync(join(dir, "upload.url"), "utf8")).toBe("https://results.example/synthetics-promql.support-experimental/_create/build-123-grafanalabs-top100");
    expect(r.stdout + r.stderr).not.toContain("fake-results-key");
  });

  test("uses the shared JSON runner, agent sizing, and CI Gradle options", () => {
    const r = run();
    expect(r.status, r.stderr).toBe(0);
    const args = readFileSync(join(dir, "args.log"), "utf8");
    expect(args).toContain("--output json --concurrency 8 -- --build-cache --scan --configuration-cache");
    expect(args).toContain("-Dtests.es.node.processors=8 -Dtests.heap.size=2048m");
    expect(readFileSync(join(dir, "output", "promcheck.stderr.log"), "utf8")).toBe("nightly diagnostics\n");
    expect(readFileSync(join(dir, "output", "promcheck.log"), "utf8")).not.toContain("diagnostics");
  });

  test("uses input totals for coverage when queries were request-filtered", () => {
    const r = run({ TEST_LOG: JSON.stringify({ ...COUNTS, total: 8, request_filtered: 2 }) });
    expect(r.status, r.stderr).toBe(0);
    expect(JSON.parse(readFileSync(join(dir, "upload.json"), "utf8")).compliance).toMatchObject({
      input_total: 8, executed_total: 6, success_pct: 12.5,
    });
  });

  test("keeps the destination and namespace tag consistent when changed", () => {
    const r = run({ PROMCHECK_RESULTS_NAMESPACE: "validation" });
    expect(r.status, r.stderr).toBe(0);
    expect(readFileSync(join(dir, "upload.url"), "utf8")).toContain("/synthetics-promql.support-validation/_create/");
    expect(JSON.parse(readFileSync(join(dir, "upload.json"), "utf8")).data_stream.namespace).toBe("validation");
  });

  test("accepts an already-created document on upload retry", () => {
    const r = run({ TEST_HTTP_STATUS: "409", TEST_RESPONSE: '{"error":{"type":"version_conflict_engine_exception"}}' });
    expect(r.status, r.stderr).toBe(0);
  });

  test("loads the destination from Vault", () => {
    const r = run({ PROMCHECK_RESULTS_URL: "", PROMCHECK_RESULTS_API_KEY: "", PROMCHECK_RESULTS_SECRET: "secret/test" });
    expect(r.status, r.stderr).toBe(0);
    expect(readFileSync(join(dir, "upload.url"), "utf8")).toContain("https://vault-results.example/");
    expect(r.stdout + r.stderr).not.toContain("vault-key");
  });

  test("does not upload an incomplete run", () => {
    expect(run({ TEST_RC: "2" }).status).not.toBe(0);
    expect(existsSync(join(dir, "upload.json"))).toBe(false);
  });

  test("supports successful JSON runs and response filters", () => {
    const r = run({ TEST_RC: "0", TEST_LOG: JSON.stringify({ ...COUNTS, total: 4, inspected: 4, match: 2, mismatch: 0, es_4xx: 0, es_5xx: 0, error: 0, response_filtered: 1 }) });
    expect(r.status, r.stderr).toBe(0);
    expect(JSON.parse(readFileSync(join(dir, "output", "counts.json"), "utf8")).response_filtered).toBe(1);
  });

  test.each([
    { TEST_RC: "2" },
    { TEST_LOG: "starting..." },
    { TEST_LOG: JSON.stringify({ ...COUNTS, total: 7 }) },
    { TEST_LOG: VERDICT + "\n" + VERDICT },
    { TEST_LOG: VERDICT + "\n{broken" },
    { TEST_UPLOAD_RC: "22" },
    { TEST_RESPONSE: '{"error":"rejected"}' },
    { TEST_HTTP_STATUS: "403" },
    { TEST_HTTP_STATUS: "409", TEST_RESPONSE: '{"error":{"type":"security_exception"}}' },
    { PROMCHECK_TEST_INSTANCE_TIMEOUT: "soon" },
    { PROMCHECK_RESULTS_NAMESPACE: "" },
    { PROMCHECK_RESULTS_NAMESPACE: "../default" },
  ])("fails on incomplete runs, invalid counts, configuration, or upload errors: %j", (env) => {
    expect(run(env).status).not.toBe(0);
  });
});
