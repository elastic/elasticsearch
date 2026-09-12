import { describe, expect, test } from "vitest";
import { planCommandsToRunnable, planEntryToSkippedTest, withGradleBinary } from "./commands.ts";
import type { PlanCommand, PlanEntry } from "./domain.ts";

describe("planEntryToSkippedTest", () => {
  test("maps a unit-test entry (fqcn, no suitePath/yamlTest) and carries the skip reason", () => {
    const e: PlanEntry = {
      gradleProject: ":qa:packaging",
      sourceSet: "test",
      kind: "test",
      fqcn: "org.elasticsearch.packaging.test.ArchiveTests",
      disposition: "skip",
      reason: "requires-packaging-host",
    };
    expect(planEntryToSkippedTest(e)).toEqual({
      gradleProject: ":qa:packaging",
      kind: "test",
      sourceSet: "test",
      fqcn: "org.elasticsearch.packaging.test.ArchiveTests",
      reason: "requires-packaging-host",
    });
  });

  test("maps a yaml suite entry (suitePath, no fqcn)", () => {
    const e: PlanEntry = {
      gradleProject: ":x-pack:plugin:esql",
      sourceSet: "yamlRestTest",
      kind: "yamlRestTestSuite",
      suitePath: "esql/10_foo",
      disposition: "run",
    };
    expect(planEntryToSkippedTest(e)).toEqual({
      gradleProject: ":x-pack:plugin:esql",
      kind: "yamlRestTestSuite",
      sourceSet: "yamlRestTest",
      suitePath: "esql/10_foo",
    });
  });

  test("maps a yaml case entry (fqcn + yamlTest)", () => {
    const e: PlanEntry = {
      gradleProject: ":x-pack:plugin:apm-data",
      sourceSet: "yamlRestTest",
      kind: "yamlRestTestCase",
      fqcn: "org.foo.ApmIT",
      yamlTest: "test {yaml=/10_apm/Reinstall}",
      disposition: "run",
    };
    expect(planEntryToSkippedTest(e)).toEqual({
      gradleProject: ":x-pack:plugin:apm-data",
      kind: "yamlRestTestCase",
      sourceSet: "yamlRestTest",
      fqcn: "org.foo.ApmIT",
      yamlTest: "test {yaml=/10_apm/Reinstall}",
    });
  });
});

describe("withGradleBinary", () => {
  test("buildkite target replaces __GRADLE__ with the run-gradle.sh wrapper", () => {
    expect(withGradleBinary("__GRADLE__ -Dtests.iters=100 :server:test --tests Foo", "buildkite")).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 :server:test --tests Foo"
    );
  });

  test("local target replaces __GRADLE__ with ./gradlew", () => {
    expect(withGradleBinary("__GRADLE__ -Dtests.iters=100 :server:test --tests Foo", "local")).toBe(
      "./gradlew -Dtests.iters=100 :server:test --tests Foo"
    );
  });

  test("replaces every __GRADLE__ occurrence, incl. inside the repeat-rest-test.sh form", () => {
    const cmd =
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 __GRADLE__ :m:a:javaRestTest --rerun";
    expect(withGradleBinary(cmd, "buildkite")).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :m:a:javaRestTest --rerun"
    );
    expect(withGradleBinary(cmd, "local")).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 ./gradlew :m:a:javaRestTest --rerun"
    );
  });

  test("leaves a command with no token unchanged", () => {
    expect(withGradleBinary("echo hello", "buildkite")).toBe("echo hello");
  });
});

describe("planCommandsToRunnable", () => {
  const commands: PlanCommand[] = [
    {
      kind: "test",
      label: "unit tests",
      key: "flakiness-detection:unit",
      command: "__GRADLE__ -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.foo.FooTests",
    },
    {
      kind: "javaRestTest",
      label: "java rest tests",
      key: "flakiness-detection:java-rest",
      command:
        ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 __GRADLE__ :server:javaRestTest --tests org.foo.BarIT --rerun",
    },
  ];

  test("buildkite target maps each PlanCommand, substituting the run-gradle.sh wrapper", () => {
    const runnable = planCommandsToRunnable(commands, "buildkite");
    expect(runnable).toHaveLength(2);
    expect(runnable[0]).toEqual({
      kind: "test",
      label: "unit tests",
      key: "flakiness-detection:unit",
      command:
        ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.foo.FooTests",
      // Carried through verbatim so the batch wrapper can scope its skipped-task check; a plan written
      // before taskPaths existed yields [].
      taskPaths: [],
    });
    // The wrapped rest form gets the same substitution inside repeat-rest-test.sh.
    expect(runnable[1].command).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :server:javaRestTest --tests org.foo.BarIT --rerun"
    );
    // No unresolved tokens remain.
    expect(runnable.every((c) => c.command.includes("__GRADLE__"))).toBe(false);
  });

  test("local target maps each PlanCommand, substituting ./gradlew", () => {
    const runnable = planCommandsToRunnable(commands, "local");
    expect(runnable[0].command).toBe(
      "./gradlew -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.foo.FooTests"
    );
    expect(runnable[1].command).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 ./gradlew :server:javaRestTest --tests org.foo.BarIT --rerun"
    );
    expect(runnable.every((c) => c.command.includes("__GRADLE__"))).toBe(false);
  });

  test("preserves kind/label/key metadata verbatim and order", () => {
    const runnable = planCommandsToRunnable(commands, "buildkite");
    expect(runnable.map((c) => c.kind)).toEqual(["test", "javaRestTest"]);
    expect(runnable.map((c) => c.key)).toEqual(["flakiness-detection:unit", "flakiness-detection:java-rest"]);
    expect(runnable.map((c) => c.label)).toEqual(["unit tests", "java rest tests"]);
  });

  test("empty input yields empty output", () => {
    expect(planCommandsToRunnable([], "buildkite")).toEqual([]);
  });
});
