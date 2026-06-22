import { describe, expect, test } from "vitest";
import { toBuildkitePipeline } from "./buildkite.ts";
import { buildCommands } from "../commands.ts";
import type {
  ClassifiedTest,
  RunnableCommand,
} from "../domain.ts";

import {
  DEFAULT_AGENT_CONFIG,
  DEFAULT_BATCHING_CONFIG,
} from "../domain.ts";

function pipelineFromTests(tests: ClassifiedTest[]) {
  return toBuildkitePipeline(
    buildCommands(tests, DEFAULT_BATCHING_CONFIG),
    DEFAULT_AGENT_CONFIG
  );
}

describe("toBuildkitePipeline end-to-end", () => {
  test("single batch has no parallelism", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.index.IndexTests" },
    ];

    const pipeline = pipelineFromTests(tests);
    expect(pipeline.steps).toHaveLength(1);
    expect(pipeline.steps[0].group).toBe("flakiness-detection");

    const step = pipeline.steps[0].steps[0];
    expect(step.label).toBe("unit tests");
    expect(step.key).toBe("flakiness-detection:unit");
    expect(step.parallelism).toBeUndefined();
    expect(step.env).toBeUndefined();
    expect(step.command).toContain(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.elasticsearch.index.IndexTests"
    );
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

  test("multiple batches use parallelism with env dispatch", () => {
    const tests: ClassifiedTest[] = [];
    for (let i = 0; i < 5; i++) {
      tests.push({
        gradleProject: `:mod:${i}`,
        kind: "javaRestTest",
        sourceSet: "javaRestTest",
        fqcn: `org.elasticsearch.Rest${i}IT`,
      });
    }

    const pipeline = pipelineFromTests(tests);
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

  test("dispatches default unit-test batches in parallel", () => {
    const tests: ClassifiedTest[] = [];
    for (let i = 0; i < 4; i++) {
      tests.push({
        gradleProject: ":server",
        kind: "test",
        sourceSet: "test",
        fqcn: `org.elasticsearch.Unit${i}Tests`,
      });
    }

    const pipeline = pipelineFromTests(tests);
    const step = pipeline.steps[0].steps[0];

    expect(step.key).toBe("flakiness-detection:unit");
    expect(step.parallelism).toBe(2);
    expect(step.env!["BATCH_COMMAND_0"]).toContain("--tests org.elasticsearch.Unit0Tests");
    expect(step.env!["BATCH_COMMAND_0"]).toContain("--tests org.elasticsearch.Unit1Tests");
    expect(step.env!["BATCH_COMMAND_0"]).toContain("--tests org.elasticsearch.Unit2Tests");
    expect(step.env!["BATCH_COMMAND_0"]).not.toContain("--tests org.elasticsearch.Unit3Tests");
    expect(step.env!["BATCH_COMMAND_1"]).toContain("--tests org.elasticsearch.Unit3Tests");
  });

  test("all test kinds appear in single group with unique keys", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.SomeTests" },
      {
        gradleProject: ":server",
        kind: "internalClusterTest",
        sourceSet: "internalClusterTest",
        fqcn: "org.elasticsearch.ClusterIT",
      },
    ];

    const pipeline = pipelineFromTests(tests);
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
    const tests: ClassifiedTest[] = [
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/test",
      },
    ];

    const pipeline = pipelineFromTests(tests);
    expect(pipeline.steps).toHaveLength(1);
    // 2 batch steps + 1 trailing analyze step.
    expect(pipeline.steps[0].steps).toHaveLength(3);
    expect(pipeline.steps[0].steps[0].label).toBe("yaml rest test runner");
    expect(pipeline.steps[0].steps[1].label).toBe("yaml rest tests");
    expect(pipeline.steps[0].steps[2].key).toBe("flakiness-detection:analyze");
  });

  test("returns empty group for empty input", () => {
    const pipeline = pipelineFromTests([]);
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

  test("batch steps upload JUnit XML artifacts; analyze step downloads them", () => {
    const cmds: RunnableCommand[] = [
      { kind: "test", label: "unit tests", key: "flakiness-detection:unit", command: "cmd" },
    ];
    const pipeline = toBuildkitePipeline(cmds, DEFAULT_AGENT_CONFIG);
    const [batch, analyze] = pipeline.steps[0].steps;

    // Batch step uploads — auto-uploaded by BK when artifact_paths is set.
    expect(batch.artifact_paths).toBe("**/build/test-results/**/TEST-*.xml");

    // Analyze step installs bun, downloads from earlier steps, then runs the analyzer.
    expect(analyze.key).toBe("flakiness-detection:analyze");
    expect(analyze.artifact_paths).toBeUndefined();
    // No agents override — analyze inherits the parent pipeline's default
    // (which has npm). The gradle-tuned cfg.agents image does not.
    expect(analyze.agents).toBeUndefined();
    expect(analyze.command).toContain(
      'buildkite-agent artifact download "**/build/test-results/**/TEST-*.xml" .'
    );
    expect(analyze.command).toContain("node .buildkite/scripts/flakiness-detection/entrypoints/analyze.ts");
    // Order: bun install → download → analyzer.
    const installIdx = analyze.command.indexOf("npm install");
    const downloadIdx = analyze.command.indexOf("artifact download");
    const analyzerIdx = analyze.command.indexOf("entrypoints/analyze.ts");
    expect(installIdx).toBeLessThan(downloadIdx);
    expect(downloadIdx).toBeLessThan(analyzerIdx);
    // Analyze step uses timeout_in_minutes: 10, so inner timeout is 8m.
    expect(analyze.command).toContain("timeout --foreground --signal=TERM --kill-after=30s 8m bash");
  });
});
