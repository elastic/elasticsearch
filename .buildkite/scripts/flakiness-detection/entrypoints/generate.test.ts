import { describe, expect, test } from "vitest";
import { run, type GenerateIO } from "./generate.ts";
import type { FlakinessPlan, PlanCommand, RunnableCommand } from "../domain.ts";

// generate is now a thin consumer of the Java-produced plan: read plan (local-or-download) -> shape +
// upload, with no batching. These tests drive run() through an injected I/O boundary so we can assert the
// real new contract (which branch, what got uploaded, what got annotated/written) without disk or
// buildkite-agent.

interface Recorded {
  uploads: { commands: RunnableCommand[]; hasNotApplicable: boolean }[];
  annotations: { style: string; body: string }[];
  files: { name: string; body: string }[];
  logs: string[];
}

function fakeIO(plan: FlakinessPlan | undefined, isCI = true): { io: GenerateIO; rec: Recorded } {
  const rec: Recorded = { uploads: [], annotations: [], files: [], logs: [] };
  const io: GenerateIO = {
    isCI,
    readPlan: () => plan,
    writeFile: (name, body) => rec.files.push({ name, body }),
    annotate: (style, body) => rec.annotations.push({ style, body }),
    upload: (commands, opts) => rec.uploads.push({ commands, hasNotApplicable: opts.hasNotApplicable }),
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
  test("writes the precompile marker and uploads an analyze-only pipeline", () => {
    const plan: FlakinessPlan = { buildFailed: true, reason: "precompile", entries: [] };
    const { io, rec } = fakeIO(plan);

    run(io);

    // Analyze-only upload: zero batch commands but hasNotApplicable so the analyze step still runs.
    expect(rec.uploads).toHaveLength(1);
    expect(rec.uploads[0].commands).toEqual([]);
    expect(rec.uploads[0].hasNotApplicable).toBe(true);
    // The build_failed marker is written for the analyze step to record.
    expect(rec.files).toHaveLength(1);
    expect(rec.files[0].name).toBe("flakiness-precompile.json");
    expect(JSON.parse(rec.files[0].body)).toEqual({ outcome: "build_failed", reason: "precompile" });
  });

  test("does not write the precompile marker when not in CI", () => {
    const plan: FlakinessPlan = { buildFailed: true, reason: "precompile", entries: [] };
    const { io, rec } = fakeIO(plan, false);

    run(io);

    expect(rec.files).toEqual([]);
    // Still uploads analyze-only (upload itself is a no-op outside CI at the buildkite layer).
    expect(rec.uploads).toHaveLength(1);
    expect(rec.uploads[0].hasNotApplicable).toBe(true);
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
    // No skip entries -> analyze not forced via hasNotApplicable; the batch step's presence is enough.
    expect(rec.uploads[0].hasNotApplicable).toBe(false);
    // No skipped file when there are no skip entries.
    expect(rec.files).toEqual([]);
  });

  test("writes flakiness-skipped.json for skip entries and forces hasNotApplicable", () => {
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

    expect(rec.uploads[0].hasNotApplicable).toBe(true);
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
  test("non-empty unresolved emits exactly one warning annotation", () => {
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
