import { execFileSync } from "child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

const SCRIPT = resolve(`${import.meta.dirname}/never-fail.sh`);

const LAYOUT = {
  FLAKINESS_STATUS_DIR: "flakiness-status",
  FLAKINESS_JOB_STATUS_PREFIX: "status-",
  FLAKINESS_TASK_STATUS_PREFIX: "tasks-",
  FLAKINESS_TASK_STATUS_FILE: "build/task-status.json",
};

describe("never-fail.sh", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "never-fail-"));
    // Stub buildkite-agent: records its argv so annotation behaviour is observable without an agent.
    mkdirSync(join(dir, "bin"));
    writeFileSync(
      join(dir, "bin", "buildkite-agent"),
      `#!/bin/bash\nprintf '%s\\n' "$*" >> "${join(dir, "annotations.log")}"\n`,
      { mode: 0o755 }
    );
  });

  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  /** Runs the script with a wrapped command, returning what an observer could see afterwards. */
  function run(
    command: string,
    opts: { kind?: string; timeout?: number; taskPaths?: string; parallelJob?: string; hardFailRc?: string } = {}
  ) {
    const args = ["--context", "flakiness-detection:unit", "--inner-timeout-minutes", String(opts.timeout ?? 1)];
    if (opts.kind) args.push("--kind", opts.kind);
    if (opts.hardFailRc) args.push("--hard-fail-rc", opts.hardFailRc);
    let status = 0;
    try {
      execFileSync("bash", [SCRIPT, ...args], {
        cwd: dir,
        env: {
          ...process.env,
          ...LAYOUT,
          PATH: `${join(dir, "bin")}:${process.env.PATH}`,
          BUILDKITE_JOB_ID: "job-1",
          BUILDKITE_LABEL: "the-label",
          FLAKINESS_CMD_0: command,
          FLAKINESS_CMD_1: command,
          ...(opts.taskPaths ? { FLAKINESS_TASK_PATHS_0: opts.taskPaths } : {}),
          ...(opts.parallelJob ? { BUILDKITE_PARALLEL_JOB: opts.parallelJob } : {}),
        },
        stdio: "pipe",
      });
    } catch (e) {
      status = (e as { status: number }).status;
    }
    const statusFile = join(dir, "flakiness-status", "status-job-1.json");
    return {
      status,
      annotations: existsSync(join(dir, "annotations.log")) ? readFileSync(join(dir, "annotations.log"), "utf8") : "",
      outcome: existsSync(statusFile) ? JSON.parse(readFileSync(statusFile, "utf8")) : undefined,
      taskStatusCopied: existsSync(join(dir, "flakiness-status", "tasks-job-1.json")),
    };
  }

  test("a passing command runs, the step exits 0, and rc 0 is recorded", () => {
    const r = run("echo ran > marker.txt", { kind: "test" });
    expect(r.status).toBe(0);
    expect(readFileSync(join(dir, "marker.txt"), "utf8").trim()).toBe("ran");
    expect(r.annotations).toBe("");
    expect(r.outcome.rc).toBe(0);
    expect(r.outcome.kind).toBe("test");
    expect(r.outcome.stepKey).toBe("flakiness-detection:unit");
  });

  /** The whole point: a red command must not make the step red, or the PR shows a spurious failure. */
  test("a failing command still exits 0, is annotated, and keeps its original rc", () => {
    const r = run("exit 7", { kind: "test" });
    expect(r.status).toBe(0);
    expect(r.annotations).toContain("--style warning");
    expect(r.annotations).toContain("exited with 7");
    expect(r.outcome.rc).toBe(7);
  });

  /**
   * rc 124 is timeout's SIGTERM exit; it must read as a timeout, not a generic failure.
   *
   * `exec` matters: with `--foreground` timeout does not put the command in its own process group, so it
   * signals only its direct child. Without `exec`, bash dies and the `sleep` is orphaned still holding the
   * inherited stdout pipe, so this test would block for the full 120s even though the script has already
   * returned. `exec` makes bash become the sleep, so there is one process to signal.
   */
  test("a command that overruns is killed, annotated as a timeout, and still exits 0", () => {
    const r = run("exec sleep 120", { kind: "test", timeout: 1 });
    expect(r.status).toBe(0);
    expect(r.annotations).toContain("timed out after 1m (rc=124)");
    expect(r.outcome.rc).toBe(124);
  }, 90_000);

  test("without --kind the command runs but no batch outcome is written", () => {
    const r = run("echo ran > marker.txt");
    expect(r.status).toBe(0);
    expect(existsSync(join(dir, "marker.txt"))).toBe(true);
    expect(r.outcome).toBeUndefined();
    expect(r.taskStatusCopied).toBe(false);
  });

  test("task paths are copied into the outcome unchanged, as JSON", () => {
    const r = run("true", { kind: "test", taskPaths: '[":a:test",":b:test"]' });
    expect(r.outcome.taskPaths).toEqual([":a:test", ":b:test"]);
  });

  /** Unset must not produce `"taskPaths":` and invalid JSON. */
  test("a missing task-paths var still yields valid JSON", () => {
    const r = run("true", { kind: "test" });
    expect(r.outcome.taskPaths).toEqual([]);
  });

  test("a heap dump left behind is reported as the oom subtype", () => {
    const r = run("mkdir -p p/build/heapdump && touch p/build/heapdump/x.hprof && exit 1", { kind: "test" });
    expect(r.outcome.infraSubtype).toBe("oom");
    expect(r.outcome.rc).toBe(1);
  });

  test("no heap dump leaves the subtype empty", () => {
    const r = run("exit 1", { kind: "test" });
    expect(r.outcome.infraSubtype).toBe("");
  });

  test("gradle-runner's task report is copied when present", () => {
    mkdirSync(join(dir, "build"));
    writeFileSync(join(dir, "build", "task-status.json"), '[{"path":":a:test","outcome":"SUCCESS"}]');
    const r = run("true", { kind: "test" });
    expect(r.taskStatusCopied).toBe(true);
    expect(JSON.parse(readFileSync(join(dir, "flakiness-status", "tasks-job-1.json"), "utf8"))).toEqual([
      { path: ":a:test", outcome: "SUCCESS" },
    ]);
  });

  /** Observability is best-effort: a missing report must not fail the batch. */
  test("a missing task report is tolerated", () => {
    const r = run("true", { kind: "test" });
    expect(r.status).toBe(0);
    expect(r.taskStatusCopied).toBe(false);
    expect(r.outcome.rc).toBe(0);
  });

  test("BUILDKITE_PARALLEL_JOB selects that batch's command", () => {
    // FLAKINESS_CMD_1 is set to the same command; index 1 must be the one read.
    const r = run("echo picked > marker.txt", { kind: "test", parallelJob: "1" });
    expect(r.status).toBe(0);
    expect(readFileSync(join(dir, "marker.txt"), "utf8").trim()).toBe("picked");
  });

  /**
   * The analyze step's opt-in: on a PR whose team added its label to BLOCKING_LABELS, the verdict has to
   * reach Buildkite. Only the designated code does - everything else still honours the never-fail
   * contract, which is what keeps a crash or an overrun of the analyze step off the PR.
   */
  test("with --hard-fail-rc the designated code is propagated", () => {
    const r = run("exit 42", { hardFailRc: "42" });
    expect(r.status).toBe(42);
    // The wrapped command annotated the report itself, so the wrapper adds no "exited with 42" noise.
    expect(r.annotations).toBe("");
  });

  test("with --hard-fail-rc every other non-zero code still exits 0 and is annotated", () => {
    const failed = run("exit 1", { hardFailRc: "42" });
    expect(failed.status).toBe(0);
    expect(failed.annotations).toContain("exited with 1");

    const timedOut = run("exit 124", { hardFailRc: "42" });
    expect(timedOut.status).toBe(0);
    expect(timedOut.annotations).toContain("timed out");
  });

  test("without --hard-fail-rc even the designated code exits 0", () => {
    // An unlabelled PR: analyze.ts reports the same verdict regardless of labels, and this is what makes
    // it a no-op there.
    const r = run("exit 42");
    expect(r.status).toBe(0);
  });

  test("a hard-fail code that is not a positive integer is rejected before anything runs", () => {
    // Checked up front on purpose: a bad value would otherwise only break the comparison at the very end
    // of the step, after an hour of tests.
    for (const bad of ["abc", "0", "-1"]) {
      const r = run("echo ran > marker.txt", { hardFailRc: bad });
      expect(r.status).toBe(2);
      expect(existsSync(join(dir, "marker.txt"))).toBe(false);
    }
  });

  test("a malformed direct invocation fails loudly instead of silently exiting 0", () => {
    let status = 0;
    try {
      execFileSync("bash", [SCRIPT, "--context", "k"], { cwd: dir, stdio: "pipe" });
    } catch (e) {
      status = (e as { status: number }).status;
    }
    // Before the wrapped command starts there is no never-fail contract to honour.
    expect(status).toBe(2);
  });
});
