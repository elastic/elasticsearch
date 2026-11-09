import { spawnSync } from "child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

const SCRIPT = resolve(import.meta.dirname, "periodic-promcheck.sh");
const VERDICT = "6 inspected: 1 match, 1 mismatch, 1 4xx, 1 5xx, 1 error, 1 flaky";

describe("periodic-promcheck.sh", () => {
  let dir: string;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "periodic-promcheck-"));
    mkdirSync(join(dir, "bin"));
    // Only external services and the downloaded executable are replaced. Real jq parses the verdict
    // and builds the indexed document; no Elasticsearch build or remote writes occur in these tests.
    writeFileSync(join(dir, "promcheck"), '#!/bin/bash\nprintf "%s\\n" "$TEST_LOG"\nexit "$TEST_RC"\n', { mode: 0o755 });
    writeFileSync(join(dir, "bin", "vault"), `#!/bin/bash\nprintf '%s' '{"data":{"url":"https://vault-results.example","api_key":"vault-key"}}'\n`, { mode: 0o755 });
    writeFileSync(join(dir, "bin", "curl"), `#!/bin/bash
out= url=
while (($#)); do
  case "$1" in
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
  */_doc/*) printf '%s' "$url" > "$TEST_DIR/upload.url"; printf '%s' "$TEST_RESPONSE"; exit "$TEST_UPLOAD_RC" ;;
  *) exit 99 ;;
esac
`, { mode: 0o755 });
  });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  function run(overrides: Record<string, string> = {}) {
    return spawnSync("bash", [SCRIPT], {
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${join(dir, "bin")}:${process.env.PATH}`,
        GH_TOKEN: "fake-github-token",
        PROMCHECK_VER: "0.9.5+prometheus-3.13.1",
        PROMCHECK_TEST_INSTANCE_TIMEOUT: "900",
        PROMCHECK_CORPUS: "grafanalabs-top100",
        PROMCHECK_RESULTS_INDEX: "promcheck-results",
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
        ...overrides,
      },
    });
  }

  test("indexes mismatches and errors as results, with provenance", () => {
    const r = run();
    expect(r.status, r.stdout + r.stderr).toBe(0);
    const result = JSON.parse(readFileSync(join(dir, "upload.json"), "utf8"));
    expect(result.counts).toEqual({ total: 6, match: 1, mismatch: 1, http4xx: 1, http5xx: 1, error: 1, response_filtered: 0, flaky: 1 });
    expect(result).toMatchObject({ branch: "main", exit_code: 1, build_id: "build-123", corpus: "grafanalabs-top100" });
    expect(result.commit).toMatch(/^[a-f0-9]{40}$/);
    expect(readFileSync(join(dir, "upload.url"), "utf8")).toBe("https://results.example/promcheck-results/_doc/build-123-grafanalabs-top100");
    expect(r.stdout + r.stderr).not.toContain("fake-results-key");
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

  test("supports successful runs, bias memos, and response filters", () => {
    const r = run({ TEST_RC: "0", TEST_LOG: "4 inspected: 2 match (1 bias-forgiven), 0 mismatch, 0 4xx, 0 5xx, 1 response-filtered, 1 flaky" });
    expect(r.status, r.stderr).toBe(0);
    expect(JSON.parse(readFileSync(join(dir, "upload.json"), "utf8")).counts.response_filtered).toBe(1);
  });

  test.each([
    { TEST_RC: "2" },
    { TEST_LOG: "starting..." },
    { TEST_LOG: "7 inspected: 1 match, 1 mismatch, 1 4xx, 1 5xx, 1 error, 1 flaky" },
    { TEST_UPLOAD_RC: "22" },
    { TEST_RESPONSE: '{"error":"rejected"}' },
    { PROMCHECK_TEST_INSTANCE_TIMEOUT: "soon" },
  ])("fails on incomplete runs, invalid counts, configuration, or upload errors: %j", (env) => {
    expect(run(env).status).not.toBe(0);
  });
});
