import { describe, expect, test } from "vitest";
import { toBuildkitePipeline, toResolvePipeline } from "./buildkite.ts";
import { planCommandsToRunnable } from "../commands.ts";
import type { PlanCommand, RunnableCommand } from "../domain.ts";

import { DEFAULT_AGENT_CONFIG } from "../domain.ts";

// The plan's batch commands are now produced by the Java scan task; TS only substitutes the gradle binary
// (planCommandsToRunnable) and shapes the BK pipeline. These helpers stand in for the Java output so the
// end-to-end shaping is exercised against realistic __GRADLE__-token command strings.
function unit(fqcn: string): PlanCommand {
  return {
    kind: "test",
    label: "unit tests",
    key: "flakiness-detection:unit",
    command: `__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests ${fqcn}`,
  };
}

function javaRest(project: string, fqcn: string): PlanCommand {
  return {
    kind: "javaRestTest",
    label: "java rest tests",
    key: "flakiness-detection:java-rest",
    command: `.buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 __GRADLE__ ${project}:javaRestTest --tests ${fqcn} --rerun`,
  };
}

function pipelineFromPlanCommands(commands: PlanCommand[]) {
  return toBuildkitePipeline(planCommandsToRunnable(commands, "buildkite"), DEFAULT_AGENT_CONFIG);
}

describe("toBuildkitePipeline end-to-end", () => {
  test("single batch has no parallelism", () => {
    const pipeline = pipelineFromPlanCommands([unit("org.elasticsearch.index.IndexTests")]);
    expect(pipeline.steps).toHaveLength(1);
    expect(pipeline.steps[0].group).toBe("flakiness-detection");

    const step = pipeline.steps[0].steps[0];
    expect(step.label).toBe("unit tests");
    expect(step.key).toBe("flakiness-detection:unit");
    expect(step.parallelism).toBeUndefined();
    expect(step.env).toBeUndefined();
    // The __GRADLE__ token was substituted with the run-gradle.sh wrapper for the buildkite target.
    expect(step.command).toContain(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.elasticsearch.index.IndexTests"
    );
    expect(step.command).not.toContain("__GRADLE__");
    expect(step.command).toContain("exit 0");
    // Inner timeout fires 2m before outer timeout_in_minutes so the wrapper
    // still gets to annotate + exit 0 even on a stuck command.
    expect(step.command).toContain("timeout --foreground --signal=TERM --kill-after=30s 58m bash");
    expect(step.timeout_in_minutes).toBe(60);
    expect(step.agents.provider).toBe("gcp");
    expect(step.agents.machineType).toBe("n4-custom-32-98304");
    // Smart retry must stay off for flakiness steps even if wrapNeverFail is removed.
    expect(step.retry).toEqual({ automatic: false });
  });

  test("multiple batches sharing a key use parallelism with env dispatch", () => {
    const commands: PlanCommand[] = [];
    for (let i = 0; i < 5; i++) {
      commands.push(javaRest(`:mod:${i}`, `org.elasticsearch.Rest${i}IT`));
    }

    const pipeline = pipelineFromPlanCommands(commands);
    expect(pipeline.steps).toHaveLength(1);

    const group = pipeline.steps[0];
    expect(group.group).toBe("flakiness-detection");
    // 1 batch step + 1 trailing analyze step.
    expect(group.steps).toHaveLength(2);

    const step = group.steps[0];
    expect(step.label).toBe("java rest tests");
    expect(step.key).toBe("flakiness-detection:java-rest");
    expect(step.parallelism).toBe(5);
    expect(step.env).toBeDefined();
    expect(step.env!["BATCH_COMMAND_0"]).toContain("repeat-rest-test.sh");
    expect(step.env!["BATCH_COMMAND_4"]).toContain("repeat-rest-test.sh");
    expect(step.env!["BATCH_COMMAND_0"]).toContain("exit 0");
    expect(step.env!["BATCH_COMMAND_4"]).toContain("exit 0");
    // Each parallel batch is independently wrapped under the inner timeout.
    expect(step.env!["BATCH_COMMAND_0"]).toContain("timeout --foreground --signal=TERM --kill-after=30s 58m bash");
    expect(step.env!["BATCH_COMMAND_4"]).toContain("timeout --foreground --signal=TERM --kill-after=30s 58m bash");
    // Both `$$` escapes defer interpolation past BK's pipeline-upload pass:
    //   * BUILDKITE_PARALLEL_JOB is a per-job runtime var; if not escaped, BK
    //     substitutes empty at upload time and the indirect lookup becomes a
    //     no-op (the bug observed on build 150689).
    //   * `${!VARNAME}` (bash indirect expansion) can't be parsed by BK as a
    //     variable identifier because of the leading `!`.
    expect(step.command).toContain('$${BUILDKITE_PARALLEL_JOB}');
    expect(step.command).not.toMatch(/[^$]\$\{BUILDKITE_PARALLEL_JOB\}/);
    expect(step.command).toContain('$${!VARNAME}');
    expect(step.command).not.toMatch(/[^$]\$\{!VARNAME\}/);

    const analyze = group.steps[1];
    expect(analyze.key).toBe("flakiness-detection:analyze");
    expect(analyze.depends_on).toEqual([{ step: "flakiness-detection:java-rest", allow_failure: true }]);
    // Both batch and analyze steps opt out of automatic (smart) retries.
    expect(step.retry).toEqual({ automatic: false });
    expect(analyze.retry).toEqual({ automatic: false });
  });

  test("batch steps write a status file; analyze step does not", () => {
    const pipeline = pipelineFromPlanCommands([unit("org.elasticsearch.SomeTests")]);
    const [batch, analyze] = pipeline.steps[0].steps;

    // Single-batch step captures the start epoch and writes a per-job status
    // file tagged with the kind + step key, carrying the runtime rc + duration
    // + OOM subtype (from the heap-dump probe below).
    expect(batch.command).toContain("_fd_start=$(date +%s)");
    expect(batch.command).toContain(
      'printf \'{"jobId":"%s","stepKey":"%s","kind":"%s","rc":%s,"durationSec":%s,"infraSubtype":"%s","taskPaths":%s}\' "$$BUILDKITE_JOB_ID" "flakiness-detection:unit" "test" "$$rc" "$(( _fd_end - _fd_start ))" "$$_fd_oom" \'[]\' > "flakiness-status/status-$$BUILDKITE_JOB_ID.json" || true'
    );
    // gradle-runner's task report is COPIED, not parsed here: analyze.ts does the matching with real JSON
    // parsing, so this shell stays uncoupled from the report's exact spacing.
    expect(batch.command).toContain('cp build/task-status.json "flakiness-status/tasks-$$BUILDKITE_JOB_ID.json"');
    // OOM is detected from a heap-dump file (TTY-safe), not the log; the probe
    // stops at the first match.
    expect(batch.command).toContain("find . -type f -path '*/build/heapdump/*.hprof' -print -quit");

    // The analyze step is not a test batch and must not write a status file or
    // probe for OOM.
    expect(analyze.key).toBe("flakiness-detection:analyze");
    expect(analyze.command).not.toContain("flakiness-status/status-");
    expect(analyze.command).not.toContain("_fd_start=");
    expect(analyze.command).not.toContain("heapdump");
  });

  test("each parallel batch writes a status file with the correct kind", () => {
    const commands: PlanCommand[] = [];
    for (let i = 0; i < 5; i++) {
      commands.push(javaRest(`:mod:${i}`, `org.elasticsearch.Rest${i}IT`));
    }

    const step = pipelineFromPlanCommands(commands).steps[0].steps[0];
    expect(step.env!["BATCH_COMMAND_0"]).toContain('"flakiness-detection:java-rest" "javaRestTest"');
    expect(step.env!["BATCH_COMMAND_0"]).toContain('> "flakiness-status/status-$$BUILDKITE_JOB_ID.json" || true');
    expect(step.env!["BATCH_COMMAND_4"]).toContain('"flakiness-detection:java-rest" "javaRestTest"');
  });

  test("all test kinds appear in single group with unique keys", () => {
    const commands: PlanCommand[] = [
      unit("org.elasticsearch.SomeTests"),
      {
        kind: "internalClusterTest",
        label: "integ tests",
        key: "flakiness-detection:integ",
        command: "__GRADLE__ -Dtests.iters=20 :server:internalClusterTest --tests org.elasticsearch.ClusterIT",
      },
    ];

    const pipeline = pipelineFromPlanCommands(commands);
    expect(pipeline.steps).toHaveLength(1);
    expect(pipeline.steps[0].group).toBe("flakiness-detection");
    // 2 batch steps + 1 trailing analyze step.
    expect(pipeline.steps[0].steps).toHaveLength(3);
    expect(pipeline.steps[0].steps[0].label).toBe("unit tests");
    expect(pipeline.steps[0].steps[0].key).toBe("flakiness-detection:unit");
    expect(pipeline.steps[0].steps[1].label).toBe("integ tests");
    expect(pipeline.steps[0].steps[1].key).toBe("flakiness-detection:integ");
    expect(pipeline.steps[0].steps[2].key).toBe("flakiness-detection:analyze");
  });

  test("yaml runners and suites get separate labels", () => {
    const commands: PlanCommand[] = [
      {
        kind: "yamlRestTestRunner",
        label: "yaml rest test runner",
        key: "flakiness-detection:yaml-runner",
        command:
          ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 __GRADLE__ :x-pack:plugin:ml:yamlRestTest --rerun",
      },
      {
        kind: "yamlRestTestSuite",
        label: "yaml rest tests",
        key: "flakiness-detection:yaml-suite",
        command:
          ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 __GRADLE__ :x-pack:plugin:ml:yamlRestTest --rerun -Dtests.rest.suite.:x-pack:plugin:ml:yamlRestTest=ml/test",
      },
    ];

    const pipeline = pipelineFromPlanCommands(commands);
    expect(pipeline.steps).toHaveLength(1);
    // 2 batch steps + 1 trailing analyze step.
    expect(pipeline.steps[0].steps).toHaveLength(3);
    expect(pipeline.steps[0].steps[0].label).toBe("yaml rest test runner");
    expect(pipeline.steps[0].steps[1].label).toBe("yaml rest tests");
    expect(pipeline.steps[0].steps[2].key).toBe("flakiness-detection:analyze");
  });

  test("returns empty group for empty input", () => {
    const pipeline = pipelineFromPlanCommands([]);
    expect(pipeline.steps).toHaveLength(1);
    expect(pipeline.steps[0].group).toBe("flakiness-detection");
    expect(pipeline.steps[0].steps).toEqual([]);
  });
});

describe("toBuildkitePipeline", () => {
  test("collapses multiple batches sharing a key into a single parallel step", () => {
    const cmds: RunnableCommand[] = [
      { kind: "test", label: "unit tests", key: "flakiness-detection:unit", command: "cmd1" },
      { kind: "test", label: "unit tests", key: "flakiness-detection:unit", command: "cmd2" },
      { kind: "test", label: "unit tests", key: "flakiness-detection:unit", command: "cmd3" },
    ];
    const pipeline = toBuildkitePipeline(cmds, DEFAULT_AGENT_CONFIG);
    const step = pipeline.steps[0].steps[0];
    expect(step.parallelism).toBe(3);
    expect(step.env?.BATCH_COMMAND_0).toContain("cmd1");
    expect(step.env?.BATCH_COMMAND_2).toContain("cmd3");
  });

  test("does not set parallelism for a single batch", () => {
    const cmds: RunnableCommand[] = [
      { kind: "test", label: "unit tests", key: "flakiness-detection:unit", command: "only" },
    ];
    const pipeline = toBuildkitePipeline(cmds, DEFAULT_AGENT_CONFIG);
    const step = pipeline.steps[0].steps[0];
    expect(step.parallelism).toBeUndefined();
    expect(step.command).toContain("only");
  });

  test("batch steps upload JUnit XML + status artifacts; analyze step downloads statuses", () => {
    const cmds: RunnableCommand[] = [
      { kind: "test", label: "unit tests", key: "flakiness-detection:unit", command: "cmd" },
    ];
    const pipeline = toBuildkitePipeline(cmds, DEFAULT_AGENT_CONFIG);
    const [batch, analyze] = pipeline.steps[0].steps;

    // Batch step uploads both the JUnit XML and the per-job status file —
    // auto-uploaded by BK when artifact_paths is set.
    expect(batch.artifact_paths).toEqual(["**/build/test-results/**/TEST-*.xml", "flakiness-status/*.json"]);

    // Analyze step downloads the status files, then runs the analyzer (which
    // downloads each job's XML per `--step`). No agents override — analyze
    // inherits the parent pipeline's default (the gradle-tuned image lacks node).
    expect(analyze.key).toBe("flakiness-detection:analyze");
    // Analyze step uploads the structured outcomes as an artifact (not an
    // annotation) for the observability pipeline to read.
    expect(analyze.artifact_paths).toBe("flakiness-outcomes.json");
    expect(analyze.agents).toBeUndefined();
    expect(analyze.command).toContain('buildkite-agent artifact download "flakiness-status/*.json" . || true');
    expect(analyze.command).toContain('buildkite-agent artifact download "flakiness-skipped.json" . || true');
    expect(analyze.command).toContain("node .buildkite/scripts/flakiness-detection/entrypoints/analyze.ts");
    // Order: download statuses → analyzer.
    const downloadIdx = analyze.command.indexOf("artifact download");
    const analyzerIdx = analyze.command.indexOf("entrypoints/analyze.ts");
    expect(downloadIdx).toBeLessThan(analyzerIdx);
    // Analyze step uses timeout_in_minutes: 10, so inner timeout is 8m.
    expect(analyze.command).toContain("timeout --foreground --signal=TERM --kill-after=30s 8m bash");
  });

  test("emits an analyze-only step when all tests are not_applicable (no batches)", () => {
    // All detected tests were BWC → zero batch commands, but the analyze step
    // must still run so the not_applicable records reach the outcomes artifact.
    const pipeline = toBuildkitePipeline([], DEFAULT_AGENT_CONFIG, { hasNotApplicable: true });
    const steps = pipeline.steps[0].steps;
    expect(steps).toHaveLength(1);
    expect(steps[0].key).toBe("flakiness-detection:analyze");
    expect(steps[0].depends_on).toEqual([]);
    expect(steps[0].command).toContain('buildkite-agent artifact download "flakiness-skipped.json" . || true');
  });

  test("no analyze step when there are neither batches nor not_applicable tests", () => {
    const pipeline = toBuildkitePipeline([], DEFAULT_AGENT_CONFIG);
    expect(pipeline.steps[0].steps).toEqual([]);
  });
});

describe("toBuildkitePipeline no longer prepends a compile gate", () => {
  const cmds: RunnableCommand[] = [
    { kind: "test", label: "unit tests", key: "flakiness-detection:unit", command: "cmd" },
  ];

  test("batch steps have no depends_on and no orchestration/precompile step is emitted", () => {
    const pipeline = toBuildkitePipeline(cmds, DEFAULT_AGENT_CONFIG);
    const steps = pipeline.steps[0].steps;

    // Just the batch + analyze; the compile gate is now a first-class
    // orchestration step (toResolvePipeline), not something this function emits.
    expect(steps.map((s) => s.key)).toEqual(["flakiness-detection:unit", "flakiness-detection:analyze"]);
    expect(steps[0].depends_on).toBeUndefined();
    for (const s of steps) {
      expect(s.key).not.toBe("flakiness-detection:precompile");
      expect(s.key).not.toContain("flakiness-orchestration:");
    }
  });
});

describe("toResolvePipeline (orchestration + separate generate step)", () => {
  const pipeline = toResolvePipeline(DEFAULT_AGENT_CONFIG);
  const group = pipeline.steps[0];
  const [orchestration, generate] = group.steps;
  const cmd = orchestration.command;

  test("emits TWO steps, both under the flakiness-orchestration prefix", () => {
    expect(group.group).toBe("flakiness-detection");
    expect(group.steps).toHaveLength(2);
    expect(orchestration.key).toBe("flakiness-orchestration:run");
    expect(generate.key).toBe("flakiness-orchestration:generate");
    // Neither may be under `flakiness-detection:` - the external batch-job metric predicate matches that
    // prefix, so a red/failed orchestration run would be mis-recorded as a test batch.
    expect(orchestration.key.startsWith("flakiness-detection:")).toBe(false);
    expect(generate.key.startsWith("flakiness-detection:")).toBe(false);
    expect(orchestration.retry).toEqual({ automatic: false });
    expect(generate.retry).toEqual({ automatic: false });
  });

  test("orchestration step: gradle agent, no inline generate, resolve+compile+scan budget", () => {
    expect(orchestration.depends_on).toBeUndefined();
    // Runs gradle, so it pins the gradle-tuned image; timeout covers the three gradle phases only.
    expect(orchestration.agents?.provider).toBe("gcp");
    expect(orchestration.timeout_in_minutes).toBe(30 + 30 + 30);
    // It must NOT run node generate.ts anywhere.
    expect(cmd).not.toContain("node .buildkite/scripts/flakiness-detection/entrypoints/generate.ts");
    // Uploads the plan (+ precompile marker) the separate generate agent downloads, plus intermediates.
    // The per-project answers go up as ONE tarball, not a `*.json` glob: every project writes its share, so
    // a glob would mean ~450 uploads per build of what is debug-only detail.
    expect(orchestration.artifact_paths).toEqual([
      "flakiness-project-targets.tgz",
      "flakiness-plan.json",
      "flakiness-precompile.json",
    ]);
    expect(cmd).toContain("tar -czf flakiness-project-targets.tgz");
    // No compile-task-list artifact: the compile phase invokes a fixed, unqualified task list, so there is
    // nothing run-specific left to persist for triage.
    expect(orchestration.artifact_paths).not.toContain("flakiness-compile-tasks.txt");
  });

  test("generate step: no agents pin, depends_on orchestration allow_failure, downloads plan, uploads outputs", () => {
    // No `agents:` pin so it uses the DEFAULT node-capable image (the gradle image lacks node).
    expect(generate.agents).toBeUndefined();
    expect(generate.depends_on).toEqual([{ step: "flakiness-orchestration:run", allow_failure: true }]);
    expect(generate.command).toContain('buildkite-agent artifact download "flakiness-plan.json" . || true');
    expect(generate.command).toContain('buildkite-agent artifact download "flakiness-precompile.json" . || true');
    expect(generate.command).toContain("node .buildkite/scripts/flakiness-detection/entrypoints/generate.ts");
    // Uploads the skipped/precompile/plan artifacts the analyze step consumes.
    expect(generate.artifact_paths).toEqual([
      "flakiness-skipped.json",
      "flakiness-precompile.json",
      "flakiness-plan.json",
    ]);
  });

  test("resolve phase downloads refs and runs the UNQUALIFIED per-project task, with the config cache ON", () => {
    expect(cmd).toContain('buildkite-agent artifact download "flakiness-refs.json" . || true');
    // Unqualified: it runs in EVERY project, each of which self-selects on whether it owns a ref. No caller
    // side project guessing, and no --no-configuration-cache (the model travels through task inputs).
    expect(cmd).toContain(".ci/scripts/run-gradle.sh -Pflakiness.resolve flakinessResolveProject");
    expect(cmd).not.toContain("--no-configuration-cache");
    // A reused workspace must not leak a previous run's per-project answers into this one.
    expect(cmd).toContain("rm -rf build/flakiness/project-targets");
    expect(cmd).toContain("timeout --foreground --signal=TERM --kill-after=30s 28m .ci/scripts/run-gradle.sh");
  });

  test("compile phase is skipped entirely when resolve produced no targets", () => {
    // pr.ts turns EVERY changed file into a ref, not just test files, so the bootstrap's refs.length === 0
    // short-circuit almost never fires: a docs-only PR still reaches this step. Without the guard it would
    // pay the whole repo test compile to produce an empty plan.
    expect(cmd).toContain(`if grep -qs '"refIndex"' build/flakiness/project-targets/*.json; then`);
    expect(cmd).toContain('echo "resolve produced no runnable targets; skipping the repo-wide test compile."');
    // scan still runs either way - it is what reports refs no project could claim at all.
    const afterCompile = cmd.slice(cmd.indexOf("# --- scan"));
    expect(afterCompile).toContain(".ci/scripts/run-gradle.sh -Pflakiness.resolve flakinessScan");
  });

  test("a reused agent workspace cannot leak a previous run's plan or markers", () => {
    expect(cmd).toContain("rm -rf build/flakiness/project-targets");
    expect(cmd).toContain(
      "rm -f flakiness-plan.json flakiness-precompile.json flakiness-project-targets.tgz",
    );
    // Same hazard on the generate agent, which re-uploads the marker without ever reading it.
    expect(generate.command).toContain(
      "rm -f flakiness-plan.json flakiness-precompile.json flakiness-skipped.json",
    );
  });

  test("compile phase compiles every test source set, unqualified, reading nothing from resolve", () => {
    // A fixed, UNQUALIFIED lifecycle task list: gradle runs each in every project that has the source set,
    // so the whole repo's test code is compiled. That is what lets the scan phase connect an abstract base
    // to subclasses in other projects.
    expect(cmd).toContain(
      "timeout --foreground --signal=TERM --kill-after=30s 28m .ci/scripts/run-gradle.sh " +
        "compileTestJava compileInternalClusterTestJava compileJavaRestTestJava compileYamlRestTestJava",
    );
    // Plain compile: no -Pflakiness property and no flakiness task name in this phase.
    const compilePhase = cmd.slice(cmd.indexOf("# --- compile"), cmd.indexOf("# --- scan"));
    expect(compilePhase).not.toContain("-Pflakiness");
    // None of the old per-project glue survives: no concatenation, no task-list variable, no empty-list branch.
    expect(cmd).not.toContain(".compile-tasks.txt");
    expect(cmd).not.toContain("$$TASKS");
    expect(cmd).not.toContain("No compile tasks listed");
  });

  test("scan phase runs flakinessScan against the local compiled output", () => {
    expect(cmd).toContain(".ci/scripts/run-gradle.sh -Pflakiness.resolve flakinessScan");
  });

  test("only compile failure is build_failed; it writes markers then exits non-zero (no inline generate)", () => {
    // The buildFailed markers are written ONLY in the compile-failure branch.
    expect(cmd).toContain('printf \'{"buildFailed":true,"reason":"precompile","entries":[]}\' > flakiness-plan.json');
    expect(cmd).toContain('printf \'{"outcome":"build_failed","reason":"precompile"}\' > flakiness-precompile.json');
    // Ordering: the buildFailed plan is written, THEN the red exit propagates. generate runs on its own
    // step (depends_on allow_failure), NOT inline here.
    const planIdx = cmd.indexOf("> flakiness-plan.json");
    const exitIdx = cmd.indexOf("exit $$rc", planIdx);
    expect(planIdx).toBeGreaterThanOrEqual(0);
    expect(exitIdx).toBeGreaterThan(planIdx);
  });

  test("resolve and scan failures are NOT build_failed (no markers)", () => {
    // Both guard with a plain `exit $$rc` and a diagnostic that names them infra/resolver defects.
    expect(cmd).toContain('echo "flakiness resolve failed (rc=$$rc): resolver/infra defect, not a PR build failure."');
    expect(cmd).toContain('echo "flakiness scan failed (rc=$$rc): resolver/infra defect, not a PR build failure."');
    // The resolve guard appears before the compile phase, so a resolve failure never reaches compile/scan.
    const resolveGuardIdx = cmd.indexOf("flakiness resolve failed");
    const compilePhaseIdx = cmd.indexOf("# --- compile");
    expect(resolveGuardIdx).toBeGreaterThanOrEqual(0);
    expect(resolveGuardIdx).toBeLessThan(compilePhaseIdx);
  });

  test("happy path ends by exiting 0 after scan (generate is a separate step)", () => {
    expect(cmd.trimEnd().endsWith("exit 0")).toBe(true);
  });
});
