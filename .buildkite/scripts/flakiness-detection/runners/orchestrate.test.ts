import { execFileSync } from "child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

const SCRIPT = resolve(`${import.meta.dirname}/orchestrate.sh`);

const ENV = {
  FLAKINESS_REFS_ARTIFACT: "flakiness-refs.json",
  FLAKINESS_PLAN_ARTIFACT: "flakiness-plan.json",
  FLAKINESS_PRECOMPILE_ARTIFACT: "flakiness-precompile.json",
  FLAKINESS_TARGETS_DIR: "build/flakiness/project-targets",
  FLAKINESS_TARGETS_ARCHIVE: "flakiness-project-targets.tgz",
  FLAKINESS_COMPILE_TASKS: "compileTestJava compileInternalClusterTestJava",
  FLAKINESS_RESOLVE_INNER_TIMEOUT: "1",
  FLAKINESS_COMPILE_INNER_TIMEOUT: "1",
  FLAKINESS_SCAN_INNER_TIMEOUT: "1",
};

describe("orchestrate.sh", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "orchestrate-"));
    mkdirSync(join(dir, "bin"));
    writeFileSync(join(dir, "bin", "buildkite-agent"), "#!/bin/bash\nexit 0\n", { mode: 0o755 });
    // The script invokes gradle by this relative path, so a fake here intercepts every phase. It logs its
    // argv so the assertions can see which tasks each phase asked for, and fails a phase named in
    // FAIL_PHASE so the failure classification is observable.
    mkdirSync(join(dir, ".ci", "scripts"), { recursive: true });
    writeFileSync(
      join(dir, ".ci", "scripts", "run-gradle.sh"),
      `#!/bin/bash
printf '%s\\n' "$*" >> gradle.log
case "$FAIL_PHASE" in
  resolve) [[ "$*" == *flakinessResolveProject* ]] && exit 3 ;;
  compile) [[ "$*" == *compileTestJava* ]] && exit 4 ;;
  scan) [[ "$*" == *flakinessScan* ]] && exit 5 ;;
esac
exit 0
`,
      { mode: 0o755 }
    );
  });

  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  /** @param resolved whether the resolve phase left a per-project file carrying a refIndex */
  function run(opts: { failPhase?: string; resolved?: boolean } = {}) {
    if (opts.resolved !== false) {
      mkdirSync(join(dir, ENV.FLAKINESS_TARGETS_DIR), { recursive: true });
      writeFileSync(join(dir, ENV.FLAKINESS_TARGETS_DIR, "p.json"), '{"resolved":[{"refIndex":0}]}');
    } else {
      mkdirSync(join(dir, ENV.FLAKINESS_TARGETS_DIR), { recursive: true });
      writeFileSync(join(dir, ENV.FLAKINESS_TARGETS_DIR, "p.json"), '{"resolved":[]}');
    }
    let status = 0;
    try {
      execFileSync("bash", [SCRIPT], {
        cwd: dir,
        env: { ...process.env, ...ENV, PATH: `${join(dir, "bin")}:${process.env.PATH}`, FAIL_PHASE: opts.failPhase ?? "" },
        stdio: "pipe",
      });
    } catch (e) {
      status = (e as { status: number }).status;
    }
    const read = (f: string) => (existsSync(join(dir, f)) ? readFileSync(join(dir, f), "utf8") : undefined);
    return {
      status,
      gradle: read("gradle.log") ?? "",
      plan: read(ENV.FLAKINESS_PLAN_ARTIFACT),
      precompile: read(ENV.FLAKINESS_PRECOMPILE_ARTIFACT),
      archived: existsSync(join(dir, ENV.FLAKINESS_TARGETS_ARCHIVE)),
    };
  }

  test("the happy path runs all three phases in order and exits 0", () => {
    const r = run();
    expect(r.status).toBe(0);
    const phases = r.gradle.trim().split("\n");
    expect(phases[0]).toContain("-Pflakiness.resolve flakinessResolveProject");
    expect(phases[1]).toContain("compileTestJava");
    expect(phases[2]).toContain("-Pflakiness.resolve flakinessScan");
    // The resolve task is UNQUALIFIED and the configuration cache stays on.
    expect(r.gradle).not.toContain("--no-configuration-cache");
    expect(r.archived).toBe(true);
  });

  /** Only compile means "the PR did not build". The other two are our own defects. */
  test("compile failure is the only build_failed: it writes both markers, then exits rc", () => {
    const r = run({ failPhase: "compile" });
    expect(r.status).toBe(4);
    expect(JSON.parse(r.plan!)).toEqual({ buildFailed: true, reason: "precompile", entries: [] });
    expect(JSON.parse(r.precompile!)).toEqual({ outcome: "build_failed", reason: "precompile" });
    // Markers first, red exit second, or generate has nothing to record.
    expect(r.gradle).not.toContain("flakinessScan");
  });

  test("resolve failure is not build_failed: no markers, exits rc, never reaches compile", () => {
    const r = run({ failPhase: "resolve" });
    expect(r.status).toBe(3);
    expect(r.plan).toBeUndefined();
    expect(r.precompile).toBeUndefined();
    expect(r.gradle).not.toContain("compileTestJava");
    expect(r.gradle).not.toContain("flakinessScan");
  });

  test("scan failure is not build_failed: no markers, exits rc", () => {
    const r = run({ failPhase: "scan" });
    expect(r.status).toBe(5);
    expect(r.plan).toBeUndefined();
    expect(r.precompile).toBeUndefined();
  });

  /**
   * The second of two gates. pr.ts already declines to upload this step when nothing changed under a
   * source directory, so a docs-only PR never gets here; this guard catches what that coarse filter lets
   * through, such as a change confined to src/main/java, which would otherwise pay the whole-repo test
   * compile to produce an empty plan.
   */
  test("compile is skipped when no project resolved a target, but scan still runs", () => {
    const r = run({ resolved: false });
    expect(r.status).toBe(0);
    expect(r.gradle).not.toContain("compileTestJava");
    // scan is what reports refs no project could claim at all, so it must not be skipped with compile.
    expect(r.gradle).toContain("flakinessScan");
  });

  test("a missing configuration value fails loudly instead of expanding to an empty path", () => {
    let status = 0;
    try {
      const { FLAKINESS_TARGETS_DIR: _omitted, ...partial } = ENV;
      execFileSync("bash", [SCRIPT], {
        cwd: dir,
        env: { ...process.env, ...partial, PATH: `${join(dir, "bin")}:${process.env.PATH}` },
        stdio: "pipe",
      });
    } catch (e) {
      status = (e as { status: number }).status;
    }
    expect(status).toBe(2);
  });
});
