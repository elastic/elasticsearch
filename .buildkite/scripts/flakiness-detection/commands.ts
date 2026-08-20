import type { PlanCommand, PlanEntry, RunnableCommand, SkippedTest } from "./domain.ts";

/**
 * Map a skipped `flakiness-plan.json` entry to the {@link SkippedTest} record generate writes to
 * `flakiness-skipped.json`. The Java resolver has already done all the resolution/enrichment work (project,
 * source set, kind, abstract-flattening) and decided the disposition, so this is a pure field copy - the
 * interesting logic that used to live in the TS detectors is gone. The `reason` is carried through so the
 * analyze step's `not_applicable` record says why the target was not re-runnable.
 */
export function planEntryToSkippedTest(e: PlanEntry): SkippedTest {
  const t: SkippedTest = { gradleProject: e.gradleProject, kind: e.kind, sourceSet: e.sourceSet };
  if (e.fqcn !== undefined) t.fqcn = e.fqcn;
  if (e.suitePath !== undefined) t.suitePath = e.suitePath;
  if (e.yamlTest !== undefined) t.yamlTest = e.yamlTest;
  if (e.reason !== undefined) t.reason = e.reason;
  return t;
}

// The gradle-binary tokens. Java emits the neutral `__GRADLE__` placeholder; TS substitutes the
// target-appropriate binary here.
//   - buildkite: `.ci/scripts/run-gradle.sh` — the BK-agent wrapper that copies init.gradle, computes
//     MAX_WORKERS, reads the ldd version, etc. (Linux-only, expects $WORKSPACE / $GRADLEW).
//   - local: `./gradlew` directly, suitable for a developer laptop.
// The `runners/repeat-rest-test.sh` wrapper is portable bash and works for both targets; Java bakes the
// `__GRADLE__` placeholder inside the `repeat-rest-test.sh <iters> __GRADLE__ <tasks>` form too, so a
// blanket replace-all covers both plain and wrapped invocations.
const GRADLE_TOKEN = "__GRADLE__";
const GRADLE_BINARY: Record<"buildkite" | "local", string> = {
  buildkite: ".ci/scripts/run-gradle.sh",
  local: "./gradlew",
};

/**
 * Replace every `__GRADLE__` token in a Java-emitted batch command with the target-appropriate gradle
 * binary. Java stays target neutral (it does not bake in `.ci/scripts/run-gradle.sh` vs `./gradlew`); this
 * is the single point where TS resolves the target.
 */
export function withGradleBinary(command: string, target: "buildkite" | "local"): string {
  return command.split(GRADLE_TOKEN).join(GRADLE_BINARY[target]);
}

/**
 * Map the ready {@link PlanCommand}s the Java scan task emitted to {@link RunnableCommand}s, substituting
 * the target-appropriate gradle binary into each command string. This is now the whole of the TS
 * command layer - all batching (dedupe, yaml-suite collapse, per-cap slicing, gradle-string assembly) moved
 * to Java.
 */
export function planCommandsToRunnable(commands: PlanCommand[], target: "buildkite" | "local"): RunnableCommand[] {
  return commands.map((c) => ({
    kind: c.kind,
    label: c.label,
    key: c.key,
    command: withGradleBinary(c.command, target),
    // A plan written before taskPaths existed simply yields none; the wrapper then skips the check.
    taskPaths: c.taskPaths ?? [],
  }));
}
