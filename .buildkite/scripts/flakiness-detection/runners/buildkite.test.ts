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
    // The __GRADLE__ token was substituted with the run-gradle.sh wrapper for the buildkite target. The
    // command now lives in the env var; the step's `command` is the shared wrapper.
    expect(step.env!["FLAKINESS_CMD_0"]).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.elasticsearch.index.IndexTests"
    );
    expect(step.env!["FLAKINESS_CMD_0"]).not.toContain("__GRADLE__");
    // The step invokes the checked-in runner; the timeout, annotation and status behaviour it implements
    // is covered by never-fail.test.ts. What matters here is that the invocation is correct.
    expect(step.command).toBe(
      ".buildkite/scripts/flakiness-detection/runners/never-fail.sh " +
        "--context flakiness-detection:unit --inner-timeout-minutes 58 --kind test"
    );
    // Inner timeout fires 2m before outer timeout_in_minutes so the runner still gets to annotate and
    // exit 0 even on a stuck command.
    expect(step.timeout_in_minutes).toBe(60);
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
    expect(step.env!["FLAKINESS_CMD_0"]).toContain("repeat-rest-test.sh");
    expect(step.env!["FLAKINESS_CMD_4"]).toContain("repeat-rest-test.sh");
    // Raw commands only: the wrapper is emitted once, in the step's `command`, not copied per batch.
    expect(step.env!["FLAKINESS_CMD_0"]).not.toContain("timeout --foreground");
    expect(step.env!["FLAKINESS_CMD_0"]).not.toContain("buildkite-agent annotate");
    // Batch selection moved into the runner, which reads BUILDKITE_PARALLEL_JOB itself. That removed the
    // whole `$$`-escaping problem from the generated pipeline: nothing here needs deferring past
    // Buildkite's upload-time interpolation any more, so an unescaped `$` cannot silently become empty
    // (the bug observed on build 150689).
    expect(step.command).not.toContain("$");
    expect(step.command).toContain("--context flakiness-detection:java-rest");
    expect(step.command).toContain("--kind javaRestTest");

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

    // `--kind` is what turns on outcome recording. The recording itself - the duration, the rc, the OOM
    // probe, the task-status copy, the JSON shape - is the runner's job and is covered by
    // never-fail.test.ts; here we only pin that the batch step asks for it and the analyze step does not.
    expect(batch.command).toContain("--kind test");
    expect(batch.command).toContain("--context flakiness-detection:unit");
    expect(batch.env!["FLAKINESS_TASK_PATHS_0"]).toBe("[]");

    // The analyze step is not a test batch: never-fail behaviour, but no outcome of its own.
    expect(analyze.key).toBe("flakiness-detection:analyze");
    expect(analyze.command).toContain("--context flakiness-detection:analyze");
    expect(analyze.command).not.toContain("--kind");
    expect(analyze.env!["FLAKINESS_TASK_PATHS_0"]).toBeUndefined();
  });

  test("each parallel batch writes a status file with the correct kind", () => {
    const commands: PlanCommand[] = [];
    for (let i = 0; i < 5; i++) {
      commands.push(javaRest(`:mod:${i}`, `org.elasticsearch.Rest${i}IT`));
    }

    const step = pipelineFromPlanCommands(commands).steps[0].steps[0];
    // All batches of a step share a kind (they are grouped by key), so it is one flag on the single
    // invocation rather than a value repeated per batch. Only the task paths differ per batch.
    expect(step.command).toContain("--context flakiness-detection:java-rest");
    expect(step.command).toContain("--kind javaRestTest");
    // One task-paths var per batch. This fixture carries no taskPaths, so both are the empty array; the
    // per-batch differentiation is covered by the `collapses multiple batches` test above.
    expect(step.env!["FLAKINESS_TASK_PATHS_0"]).toBe("[]");
    expect(step.env!["FLAKINESS_TASK_PATHS_4"]).toBe("[]");
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
    // The env vars hold the RAW command, not a copy of the wrapper. That is the whole point of hoisting
    // it: one wrapper per step instead of one per batch.
    expect(step.env?.FLAKINESS_CMD_0).toBe("cmd1");
    expect(step.env?.FLAKINESS_CMD_2).toBe("cmd3");
    expect(step.env?.FLAKINESS_CMD_0).not.toContain("timeout --foreground");
    expect(step.env?.FLAKINESS_CMD_0).not.toContain("buildkite-agent annotate");
    // Per-batch task paths travel alongside, so the wrapper needs nothing baked in.
    expect(step.env?.FLAKINESS_TASK_PATHS_0).toBe("[]");
  });

  test("does not set parallelism for a single batch", () => {
    const cmds: RunnableCommand[] = [
      { kind: "test", label: "unit tests", key: "flakiness-detection:unit", command: "only" },
    ];
    const pipeline = toBuildkitePipeline(cmds, DEFAULT_AGENT_CONFIG);
    const step = pipeline.steps[0].steps[0];
    expect(step.parallelism).toBeUndefined();
    // Non-parallel steps use the same wrapper and the same env var; BUILDKITE_PARALLEL_JOB is unset there,
    // so `:-0` picks index 0. One code path for both shapes.
    expect(step.env?.FLAKINESS_CMD_0).toBe("only");
    // One code path for both step shapes: the runner reads BUILDKITE_PARALLEL_JOB itself and falls back
    // to index 0, which is what a non-parallel step gets.
    expect(step.command).toContain("never-fail.sh");
    expect(step.command).not.toContain("--kind undefined");
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
    const analyzeCmd = analyze.env?.FLAKINESS_CMD_0 ?? "";
    expect(analyzeCmd).toContain('buildkite-agent artifact download "flakiness-status/*.json" . || true');
    expect(analyzeCmd).toContain('buildkite-agent artifact download "flakiness-skipped.json" . || true');
    expect(analyzeCmd).toContain("node .buildkite/scripts/flakiness-detection/entrypoints/analyze.ts");
    // Never-fail like a batch step, but it writes no batch outcome of its own.
    expect(analyze.command).not.toContain('"kind"');
    expect(analyze.env?.FLAKINESS_TASK_PATHS_0).toBeUndefined();
    // Order: download statuses → analyzer.
    const downloadIdx = analyzeCmd.indexOf("artifact download");
    const analyzerIdx = analyzeCmd.indexOf("entrypoints/analyze.ts");
    expect(downloadIdx).toBeLessThan(analyzerIdx);
    // Analyze step uses timeout_in_minutes: 10, so inner timeout is 8m.
    // 10m outer timeout leaves an 8m inner one after the grace period.
    expect(analyze.command).toContain("--inner-timeout-minutes 8");
  });

  test("emits an analyze-only step when all tests are not_applicable (no batches)", () => {
    // All detected tests were BWC → zero batch commands, but the analyze step
    // must still run so the not_applicable records reach the outcomes artifact.
    const pipeline = toBuildkitePipeline([], DEFAULT_AGENT_CONFIG, { hasNotApplicable: true });
    const steps = pipeline.steps[0].steps;
    expect(steps).toHaveLength(1);
    expect(steps[0].key).toBe("flakiness-detection:analyze");
    expect(steps[0].depends_on).toEqual([]);
    expect(steps[0].env?.FLAKINESS_CMD_0).toContain(
      'buildkite-agent artifact download "flakiness-skipped.json" . || true'
    );
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

  test("orchestration step: gradle agent, invokes the runner, resolve+compile+scan budget", () => {
    expect(orchestration.depends_on).toBeUndefined();
    // Runs gradle, so it pins the gradle-tuned image; timeout covers the three gradle phases only.
    expect(orchestration.agents?.provider).toBe("gcp");
    expect(orchestration.timeout_in_minutes).toBe(30 + 30 + 30);
    // The phase sequencing, failure classification and build_failed markers live in the script now; they
    // are covered by orchestrate.test.ts, which runs it against a fake gradle.
    expect(orchestration.command).toBe(".buildkite/scripts/flakiness-detection/runners/orchestrate.sh");
    // It must NOT run node generate.ts anywhere.
    expect(orchestration.command).not.toContain("generate.ts");
    // Uploads the plan (+ precompile marker) the separate generate agent downloads, plus intermediates.
    // The per-project answers go up as ONE tarball, not a `*.json` glob: every project writes its share, so
    // a glob would mean ~450 uploads per build of what is debug-only detail.
    expect(orchestration.artifact_paths).toEqual([
      "flakiness-project-targets.tgz",
      "flakiness-plan.json",
      "flakiness-precompile.json",
    ]);
    // No compile-task-list artifact: the compile phase invokes a fixed, unqualified task list, so there is
    // nothing run-specific left to persist for triage.
    expect(orchestration.artifact_paths).not.toContain("flakiness-compile-tasks.txt");
  });

  test("every value the runner needs is named on the step", () => {
    // The script validates each of these and exits 2 if one is missing, so a dropped value fails at the
    // top of the step rather than expanding to an empty path partway through.
    expect(orchestration.env).toEqual({
      FLAKINESS_REFS_ARTIFACT: "flakiness-refs.json",
      FLAKINESS_PLAN_ARTIFACT: "flakiness-plan.json",
      FLAKINESS_PRECOMPILE_ARTIFACT: "flakiness-precompile.json",
      FLAKINESS_TARGETS_DIR: "build/flakiness/project-targets",
      FLAKINESS_TARGETS_ARCHIVE: "flakiness-project-targets.tgz",
      // A fixed, UNQUALIFIED lifecycle task list: gradle runs each in every project that has the source
      // set, so the whole repo's test code is compiled. That is what lets the scan phase connect an
      // abstract base to subclasses in other projects.
      FLAKINESS_COMPILE_TASKS: "compileTestJava compileInternalClusterTestJava compileJavaRestTestJava compileYamlRestTestJava",
      // 30m budget each, less the 2m grace, so the script wins the race against the agent's SIGKILL.
      FLAKINESS_RESOLVE_INNER_TIMEOUT: "28",
      FLAKINESS_COMPILE_INNER_TIMEOUT: "28",
      FLAKINESS_SCAN_INNER_TIMEOUT: "28",
    });
  });

  test("generate step: no agents pin, depends_on orchestration allow_failure, leaves the plan to generate.ts", () => {
    // No `agents:` pin so it uses the DEFAULT node-capable image (the gradle image lacks node).
    expect(generate.agents).toBeUndefined();
    expect(generate.depends_on).toEqual([{ step: "flakiness-orchestration:run", allow_failure: true }]);
    // The plan is downloaded by generate.ts itself, which removes any local copy first so a stale file on a
    // reused workspace cannot win. Downloading it here as well would only mask that.
    expect(generate.command).not.toContain('artifact download "flakiness-plan.json"');
    expect(generate.command).toContain('buildkite-agent artifact download "flakiness-precompile.json" . || true');
    expect(generate.command).toContain("node .buildkite/scripts/flakiness-detection/entrypoints/generate.ts");
    // Uploads the skipped/precompile/plan artifacts the analyze step consumes.
    expect(generate.artifact_paths).toEqual([
      "flakiness-skipped.json",
      "flakiness-precompile.json",
      "flakiness-plan.json",
    ]);
  });
});
