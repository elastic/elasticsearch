import { describe, expect, test } from "vitest";
import { run, type GenerateIO } from "./generate.ts";
import type { FlakinessPlan, PlanCommand, ReportInputs, RunnableCommand } from "../domain.ts";

// generate is now a thin consumer of the Java-produced plan: read plan (local-or-download) -> shape +
// upload, with no batching. These tests drive run() through an injected I/O boundary so we can assert the
// real new contract (which branch, what got uploaded, what got annotated/written) without disk or
// buildkite-agent.

interface Recorded {
  uploads: { commands: RunnableCommand[]; report: ReportInputs }[];
  annotations: { style: string; body: string }[];
  files: { name: string; body: string }[];
  logs: string[];
}

function fakeIO(plan: FlakinessPlan | undefined, isCI = true): { io: GenerateIO; rec: Recorded } {
  const rec: Recorded = { uploads: [], annotations: [], files: [], logs: [] };
  const io: GenerateIO = {
    isCI,
    jobId: "0198-generate",
    readPlan: () => plan,
    publish: (name, body) => rec.files.push({ name, body }),
    annotate: (style, body) => rec.annotations.push({ style, body }),
    upload: (commands, report) => rec.uploads.push({ commands, report }),
    log: (msg) => rec.logs.push(msg),
  };
  return { io, rec };
}

const UNIT_CMD: PlanCommand = {
  kind: "test",
  label: "unit tests",
  key: "flakiness-detection:unit",
  command: "__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.foo.FooTests",
};

describe("generate run() - buildFailed", () => {
  test("uploads an analyze-only pipeline that is told the compile failed", () => {
    const plan: FlakinessPlan = { buildFailed: true, reason: "precompile", entries: [] };
    const { io, rec } = fakeIO(plan);

    run(io);

    expect(rec.uploads).toHaveLength(1);
    expect(rec.uploads[0].commands).toEqual([]);
    expect(rec.uploads[0].report).toEqual({ producerJobId: "0198-generate", compileFailed: true, skippedCount: 0 });
    expect(rec.files).toEqual([]);
  });
});

describe("generate run() - happy path", () => {
  test("maps plan.commands to runnable (buildkite gradle binary) and uploads them", () => {
    const plan: FlakinessPlan = {
      buildFailed: false,
      entries: [
        { gradleProject: ":server", sourceSet: "test", kind: "test", fqcn: "org.foo.FooTests", disposition: "run" },
      ],
      commands: [UNIT_CMD],
    };
    const { io, rec } = fakeIO(plan);

    run(io);

    expect(rec.uploads).toHaveLength(1);
    const uploaded = rec.uploads[0].commands;
    expect(uploaded).toHaveLength(1);
    // __GRADLE__ replaced with the buildkite wrapper.
    expect(uploaded[0].command).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.foo.FooTests"
    );
    expect(uploaded[0].command).not.toContain("__GRADLE__");
    expect(uploaded[0].key).toBe("flakiness-detection:unit");
    expect(rec.uploads[0].report).toEqual({ producerJobId: "0198-generate", compileFailed: false, skippedCount: 0 });
    // Nothing skipped, so no skip list: the analyze step is told the count is 0 and does not read one.
    expect(rec.files).toEqual([]);
  });

  test("writes flakiness-skipped.json for skip entries and declares their count", () => {
    const plan: FlakinessPlan = {
      buildFailed: false,
      entries: [
        { gradleProject: ":server", sourceSet: "test", kind: "test", fqcn: "org.foo.FooTests", disposition: "run" },
        {
          gradleProject: ":qa:rolling",
          sourceSet: "javaRestTest",
          kind: "javaRestTest",
          fqcn: "org.foo.SomeIT",
          disposition: "skip",
          reason: "requires-packaging-host",
        },
      ],
      commands: [UNIT_CMD],
    };
    const { io, rec } = fakeIO(plan);

    run(io);

    expect(rec.uploads[0].report.skippedCount).toBe(1);
    const skipped = rec.files.find((f) => f.name === "flakiness-skipped.json");
    expect(skipped).toBeDefined();
    const parsed = JSON.parse(skipped!.body);
    expect(parsed).toHaveLength(1);
    expect(parsed[0].fqcn).toBe("org.foo.SomeIT");
    // The reason travels to the analyze step, so the not_applicable record explains itself.
    expect(parsed[0].reason).toBe("requires-packaging-host");
  });

  test("logs the capped task fan-out so a bwc selection is never invisible", () => {
    const plan: FlakinessPlan = {
      buildFailed: false,
      entries: [
        {
          gradleProject: ":qa:rolling",
          sourceSet: "javaRestTest",
          kind: "javaRestTest",
          fqcn: "org.foo.SomeIT",
          disposition: "run",
          runnableTasks: [":qa:rolling:v9.6.0#bwcTest", ":qa:rolling:v9.5.1#bwcTest"],
        },
      ],
      taskSelections: [
        {
          gradleProject: ":qa:rolling",
          sourceSet: "javaRestTest",
          selected: [":qa:rolling:v9.6.0#bwcTest", ":qa:rolling:v9.5.1#bwcTest"],
          total: 67,
          cap: 2,
        },
      ],
      commands: [UNIT_CMD],
    };
    const { io, rec } = fakeIO(plan);

    run(io);

    expect(rec.logs.join("\n")).toContain("selected 2 of 67 candidate tasks (cap 2)");
    // Reported on the console only - not an annotation (it is already in the plan artifact).
    expect(rec.annotations).toEqual([]);
  });

  test("no runnable commands and no skips -> no upload, early return", () => {
    const plan: FlakinessPlan = { buildFailed: false, entries: [], commands: [] };
    const { io, rec } = fakeIO(plan);

    run(io);

    expect(rec.uploads).toEqual([]);
    expect(rec.files).toEqual([]);
  });

  test("missing commands field is treated as empty (no throw, no upload)", () => {
    const plan: FlakinessPlan = { buildFailed: false, entries: [] };
    const { io, rec } = fakeIO(plan);

    expect(() => run(io)).not.toThrow();
    expect(rec.uploads).toEqual([]);
  });
});

describe("generate run() - no plan", () => {
  test("no plan.json -> logs and returns without uploading or throwing", () => {
    const { io, rec } = fakeIO(undefined);

    expect(() => run(io)).not.toThrow();
    expect(rec.uploads).toEqual([]);
    expect(rec.annotations).toEqual([]);
    expect(rec.files).toEqual([]);
    expect(rec.logs.some((l) => l.includes("nothing to upload"))).toBe(true);
  });
});

describe("generate run() - enrichment reporting", () => {
  test("non-empty unresolved emits exactly one annotation listing every ref", () => {
    const plan: FlakinessPlan = {
      buildFailed: false,
      entries: [
        { gradleProject: ":server", sourceSet: "test", kind: "test", fqcn: "org.foo.FooTests", disposition: "run" },
      ],
      commands: [UNIT_CMD],
      unresolved: [{ ref: { source: "unmute", className: "org.foo.GoneTests" }, reason: "class not found" }],
    };
    const { io, rec } = fakeIO(plan);

    run(io);

    expect(rec.annotations).toHaveLength(1);
    // `warning`: an unmute is not fatal. The style-per-source rule is covered on its own below.
    expect(rec.annotations[0].style).toBe("warning");
    expect(rec.annotations[0].body).toContain("org.foo.GoneTests");
    expect(rec.annotations[0].body).toContain("class not found");
  });

  test("empty unresolved emits NO annotation", () => {
    const plan: FlakinessPlan = {
      buildFailed: false,
      entries: [
        { gradleProject: ":server", sourceSet: "test", kind: "test", fqcn: "org.foo.FooTests", disposition: "run" },
      ],
      commands: [UNIT_CMD],
      unresolved: [],
    };
    const { io, rec } = fakeIO(plan);

    run(io);

    expect(rec.annotations).toEqual([]);
  });

  test("expansions are logged to console only, never annotated", () => {
    const plan: FlakinessPlan = {
      buildFailed: false,
      entries: [
        { gradleProject: ":server", sourceSet: "test", kind: "test", fqcn: "org.foo.BarTests", disposition: "run" },
      ],
      commands: [UNIT_CMD],
      expansions: [{ abstractFqcn: "org.foo.AbstractTests", ran: 4, total: 4, cap: 5 }],
    };
    const { io, rec } = fakeIO(plan);

    run(io);

    // Expansions go to the log, not to any annotation.
    expect(rec.annotations).toEqual([]);
    expect(rec.logs.some((l) => l.includes("org.foo.AbstractTests"))).toBe(true);
  });
});

describe("generate run() - unresolved references", () => {
  const RUN_ENTRY = {
    gradleProject: ":server",
    sourceSet: "test",
    kind: "test" as const,
    fqcn: "org.foo.FooTests",
    disposition: "run" as const,
  };

  test("a ref that named a class and did not resolve fails the step", () => {
    const plan: FlakinessPlan = {
      buildFailed: false,
      entries: [],
      commands: [],
      unresolved: [{ ref: { source: "explicit", spec: "org.foo.MisspeltTests" }, reason: "no-source-file" }],
    };
    const { io, rec } = fakeIO(plan);

    // A typo in FLAKINESS_CLASSES used to exit 1 from the bootstrap step. Resolution moved to the resolver,
    // so the check lands here instead - but it must still fail, or the run is green having tested nothing.
    expect(run(io)).toBe(false);
    expect(rec.annotations).toHaveLength(1);
    expect(rec.annotations[0].style).toBe("error");
    expect(rec.annotations[0].body).toContain("org.foo.MisspeltTests");
  });

  test("the resolvable part of a mixed request is still uploaded", () => {
    const plan: FlakinessPlan = {
      buildFailed: false,
      entries: [RUN_ENTRY],
      commands: [UNIT_CMD],
      unresolved: [{ ref: { source: "explicit", spec: "org.foo.GoneTests" }, reason: "no-source-file" }],
    };
    const { io, rec } = fakeIO(plan);

    // Upload happens before the verdict: the batch steps do not depend_on generate, so they run even though
    // this step goes red. Failing without uploading would punish the good half of the request.
    expect(run(io)).toBe(false);
    expect(rec.uploads).toHaveLength(1);
    expect(rec.uploads[0].commands).toHaveLength(1);
  });

  test("deleting a muted test and its mute entry does not fail the run", () => {
    const plan: FlakinessPlan = {
      buildFailed: false,
      entries: [],
      commands: [],
      unresolved: [{ ref: { source: "unmute", className: "org.foo.DeletedTests" }, reason: "no-source-file" }],
    };
    const { io, rec } = fakeIO(plan);

    // `--diff-filter=d` means the deletion produces no changed-file ref, while removing the mute still
    // produces an unmute ref for a class that is gone. That is routine cleanup, not a defect, so it must
    // annotate and pass rather than red the PR.
    expect(run(io)).toBe(true);
    expect(rec.annotations[0].style).toBe("warning");
  });

  test("an unresolved changed-file ref stays a warning and passes", () => {
    const plan: FlakinessPlan = {
      buildFailed: false,
      entries: [RUN_ENTRY],
      commands: [UNIT_CMD],
      unresolved: [{ ref: { source: "changed-file", path: "server/src/test/java/org/foo/Odd.java" }, reason: "no-source-file" }],
    };
    const { io, rec } = fakeIO(plan);

    // Nobody asserted that file was a test, so it cannot fail the run.
    expect(run(io)).toBe(true);
    expect(rec.annotations).toHaveLength(1);
    expect(rec.annotations[0].style).toBe("warning");
  });
});
