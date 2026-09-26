import { execFileSync } from "child_process";
import { existsSync, mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

import { analyzeReports } from "../analyzer/analyze.ts";

const REPEAT_SCRIPT = resolve(`${import.meta.dirname}/repeat-rest-test.sh`);

// A single passing JUnit testcase. The runner relocates whatever TEST-*.xml a
// "gradle" invocation leaves behind, so the fake gradle below just has to write
// this to the conventional results path on every call.
const PASSING_XML = `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.FooIT" tests="1" failures="0" errors="0">
  <testcase classname="org.example.FooIT" name="testThing"/>
</testsuite>
`;

// Where the real gradle yamlRestTest task writes its report, relative to cwd.
const LIVE_XML = "p/build/test-results/yamlRestTest/TEST-org.example.FooIT.xml";

describe("repeat-rest-test.sh", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "repeat-rest-test-"));
    // Fake "gradle": overwrites the same TEST-*.xml on every call, exactly like a
    // real re-run does. Mirrors the overwrite behaviour that made the analyzer see
    // only the final iteration before per-iteration snapshots were added.
    writeFileSync(
      join(dir, "fake-gradle.sh"),
      `#!/bin/bash\nmkdir -p "$(dirname "${LIVE_XML}")"\ncat > "${LIVE_XML}" <<'EOF'\n${PASSING_XML}EOF\n`
    );
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  test("preserves every iteration's JUnit XML instead of overwriting", () => {
    const iters = 3;
    execFileSync("bash", [REPEAT_SCRIPT, String(iters), "bash", "fake-gradle.sh"], { cwd: dir });

    for (let i = 1; i <= iters; i++) {
      expect(existsSync(join(dir, `flakiness-iters/iter-${i}/${LIVE_XML}`))).toBe(true);
    }
    // The final iteration must have been moved out of its live location, so it is
    // not double-counted alongside the snapshots at upload/analyze time.
    expect(existsSync(join(dir, LIVE_XML))).toBe(false);
  });

  test("analyzer counts one case per iteration across the preserved snapshots", async () => {
    const iters = 4;
    execFileSync("bash", [REPEAT_SCRIPT, String(iters), "bash", "fake-gradle.sh"], { cwd: dir });

    const report = await analyzeReports([dir]);
    expect(report.totals.iterations).toBe(iters);
    expect(report.totals.successfulCases).toBe(iters);
    expect(report.totals.realFailures).toBe(0);
    // The seed-less yaml case name aggregates into a single per-test row that
    // records the pass on each iteration.
    expect(report.perTest).toHaveLength(1);
    expect(report.perTest[0].passes).toBe(iters);
  });

  test("analyzer joins a flaky iteration with its passing siblings", async () => {
    // The whole point of preserving every iteration: a test that fails on one
    // iteration but passes on the others must surface as a mixed pass/fail row,
    // which is impossible if only the final iteration's XML survives. A fake
    // gradle that fails on its 2nd of 3 calls exercises the cross-file join.
    const failingCase = `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.FooIT" tests="1" failures="1" errors="0">
  <testcase classname="org.example.FooIT" name="testThing">
    <failure type="java.lang.AssertionError" message="boom"/>
  </testcase>
</testsuite>
`;
    writeFileSync(
      join(dir, "fake-gradle.sh"),
      `#!/bin/bash\n` +
        `mkdir -p "$(dirname "${LIVE_XML}")"\n` +
        `n=$(cat .callcount 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > .callcount\n` +
        `if [ "$n" -eq 2 ]; then\n` +
        `  cat > "${LIVE_XML}" <<'EOF'\n${failingCase}EOF\n  exit 1\n` +
        `else\n` +
        `  cat > "${LIVE_XML}" <<'EOF'\n${PASSING_XML}EOF\n` +
        `fi\n`
    );

    // Fails on iteration 2, so repeat-rest-test.sh exits non-zero overall; the
    // per-iteration XML is still relocated regardless of the run's exit code.
    expect(() =>
      execFileSync("bash", [REPEAT_SCRIPT, "3", "bash", "fake-gradle.sh"], { cwd: dir })
    ).toThrow();

    const report = await analyzeReports([dir]);
    expect(report.totals.iterations).toBe(3);
    expect(report.totals.successfulCases).toBe(2);
    expect(report.totals.realFailures).toBe(1);
    expect(report.perTest).toHaveLength(1);
    expect(report.perTest[0].passes).toBe(2);
    expect(report.perTest[0].failures).toBe(1);
    expect(report.perTest[0].failureKinds).toEqual(["assertion"]);
  });

  test("leaves pre-existing unrelated reports in place", () => {
    // On a developer's checkout the cwd is the repo root, which may already hold
    // build/test-results reports from other modules. The `-newer` guard must keep
    // the loop from sweeping those out from under the developer.
    const strayXml = "other-module/build/test-results/test/TEST-org.other.StrayTests.xml";
    mkdirSync(join(dir, "other-module/build/test-results/test"), { recursive: true });
    writeFileSync(join(dir, strayXml), PASSING_XML);
    // Backdate it well before the run so it is unambiguously older than the marker.
    const longAgo = new Date("2020-01-01T00:00:00Z");
    utimesSync(join(dir, strayXml), longAgo, longAgo);

    execFileSync("bash", [REPEAT_SCRIPT, "2", "bash", "fake-gradle.sh"], { cwd: dir });

    // The stray report stays where it was and was not relocated into a snapshot.
    expect(existsSync(join(dir, strayXml))).toBe(true);
    expect(existsSync(join(dir, `flakiness-iters/iter-1/${strayXml}`))).toBe(false);
    expect(existsSync(join(dir, `flakiness-iters/iter-2/${strayXml}`))).toBe(false);
    // The run's own report is still preserved per iteration.
    expect(existsSync(join(dir, `flakiness-iters/iter-1/${LIVE_XML}`))).toBe(true);
    expect(existsSync(join(dir, `flakiness-iters/iter-2/${LIVE_XML}`))).toBe(true);
  });

  test("does not re-move already-preserved snapshots on later iterations", () => {
    // Guards the `-prune` of the snapshot root: without it, iteration 2 would find
    // iteration 1's snapshot (its path still contains build/test-results/) and move
    // it under iter-2, collapsing the count again.
    execFileSync("bash", [REPEAT_SCRIPT, "2", "bash", "fake-gradle.sh"], { cwd: dir });

    expect(existsSync(join(dir, `flakiness-iters/iter-1/${LIVE_XML}`))).toBe(true);
    expect(existsSync(join(dir, `flakiness-iters/iter-2/${LIVE_XML}`))).toBe(true);
    // iter-1's report must not have been relocated into iter-2's tree.
    expect(existsSync(join(dir, `flakiness-iters/iter-2/flakiness-iters/iter-1/${LIVE_XML}`))).toBe(false);
  });
});
