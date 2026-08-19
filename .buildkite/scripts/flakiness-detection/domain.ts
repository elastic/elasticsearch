export const AGENTS = {
  provider: "gcp",
  image: "family/elasticsearch-ubuntu-2404",
  machineType: "n4-custom-32-98304",
  diskType: "hyperdisk-balanced",
  buildDirectory: "/dev/shm/bk",
};

// The test kinds. In the B2 architecture these are assigned authoritatively by the Java resolver
// (build-tools-internal `FlakinessResolveProjectTask`) from the real Gradle project model + compiled bytecode,
// not by path regexes. Keep this union in sync with `Kinds.java` on the Java side - the strings are a
// hard wire contract shared by `flakiness-plan.json`.
export type TestKind =
  | "test"
  | "internalClusterTest"
  | "javaRestTest"
  | "yamlRestTestSuite"
  | "yamlRestTestRunner"
  | "yamlRestTestCase";

export interface ClassifiedTest {
  gradleProject: string;
  kind: TestKind;
  sourceSet: string;
  fqcn?: string;
  suitePath?: string;
  /**
   * Full JUnit test descriptor for a single parameterized yaml test case,
   * e.g. "test {yaml=10_apm/Test template reinstallation}". Only set for
   * {@link TestKind} of "yamlRestTestCase".
   */
  yamlTest?: string;
}

// ---------------------------------------------------------------------------
// Contract 1: flakiness-refs.json (bootstrap -> Java resolver)
//
// Heterogeneous input references. A changed-file ref carries a repo-relative path; an unmute ref carries
// a class name (+ optional method descriptor); an explicit ref carries a developer-supplied spec. The Java
// resolver (build-tools-internal) turns these into a plan; TS no longer resolves them itself.
// ---------------------------------------------------------------------------
export interface FlakinessRef {
  source: "changed-file" | "unmute" | "explicit";
  path?: string; // changed-file
  className?: string; // unmute
  method?: string; // unmute
  spec?: string; // explicit
}

export interface FlakinessRefsFile {
  mergeBase: string;
  refs: FlakinessRef[];
}

// ---------------------------------------------------------------------------
// Contract 2: flakiness-plan.json (Java resolver -> generate step); single source of truth.
// Abstract bases are already flattened to concrete subclasses by the resolver.
// ---------------------------------------------------------------------------
export interface PlanEntry {
  gradleProject: string;
  sourceSet: string;
  kind: TestKind;
  fqcn?: string;
  suitePath?: string;
  yamlTest?: string;
  disposition: "run" | "skip";
  // Set on skip: why the target cannot be re-run, from the Java resolver's TestTaskSelector -
  // "no-runnable-task" (the source set has no enabled Test task) or "requires-packaging-host" (only the
  // destructive packaging tasks would run it). Folded into `not_applicable` by the analyze step.
  reason?: string;
  expandedFrom?: string; // set when this concrete entry came from an abstract base
  // The authoritative Gradle task paths that re-run this entry, from the project's real Test tasks - so a
  // bwc target carries its `v<version>#bwcTest` tasks rather than the disabled bare task. Empty on a skip.
  // TS never has to build a task path itself; the Java side already baked these into `commands`.
  runnableTasks?: string[];
}

export interface PlanExpansion {
  abstractFqcn: string;
  ran: number;
  total: number;
  cap: number;
}

/**
 * One target whose candidate Test tasks were capped by the resolver: `selected` of `total` candidates were
 * kept (newest-first). Reported so a bwc target that fans out to dozens of `v<version>#bwcTest` tasks
 * visibly says which ones actually ran.
 */
export interface PlanTaskSelection {
  gradleProject: string;
  sourceSet: string;
  selected: string[];
  total: number;
  cap: number;
}

/**
 * A target the resolver could not re-run, as written to `flakiness-skipped.json` for the analyze step. It is
 * a {@link ClassifiedTest} plus the resolver's machine-readable reason, so the recorded `not_applicable`
 * outcome explains itself ("requires-packaging-host" reads very differently from "no-runnable-task").
 */
export interface SkippedTest extends ClassifiedTest {
  reason?: string;
}

export interface PlanUnresolved {
  ref: FlakinessRef;
  reason: string;
}

/**
 * A ready-to-run batch command emitted by the Java scan task, one per Buildkite batch step. The Java side
 * has already done all batching (dedupe, yaml-suite collapse, per-cap slicing, gradle-string assembly), so
 * TS treats this as opaque - it only needs to swap the gradle binary token (see {@link withGradleBinary}).
 *
 * `command` contains the literal token `__GRADLE__` wherever the gradle binary belongs (both plain
 * invocations and inside the `repeat-rest-test.sh <iters> __GRADLE__ <tasks>` form). Java stays target
 * neutral; the TS runner layer substitutes the target-appropriate binary. `key`/`label`/`kind` match the
 * existing {@link KIND_KEYS} / {@link KIND_LABELS} / {@link TestKind} tables.
 */
export interface PlanCommand {
  kind: TestKind;
  label: string;
  key: string;
  command: string;
}

export interface FlakinessPlan {
  buildFailed: boolean;
  reason?: string | null;
  entries: PlanEntry[];
  expansions?: PlanExpansion[];
  taskSelections?: PlanTaskSelection[];
  unresolved?: PlanUnresolved[];
  // Ready batch commands, one per BK batch step. Present and possibly empty (no run entries). A buildFailed
  // plan carries no useful commands (handle buildFailed first). See {@link PlanCommand}.
  commands?: PlanCommand[];
}

export const KIND_ORDER: TestKind[] = [
  "test",
  "internalClusterTest",
  "javaRestTest",
  "yamlRestTestRunner",
  "yamlRestTestSuite",
  "yamlRestTestCase",
];

export const KIND_LABELS: Record<TestKind, string> = {
  test: "unit tests",
  internalClusterTest: "integ tests",
  javaRestTest: "java rest tests",
  yamlRestTestRunner: "yaml rest test runner",
  yamlRestTestSuite: "yaml rest tests",
  yamlRestTestCase: "yaml rest test cases",
};

export const KIND_KEYS: Record<TestKind, string> = {
  test: "flakiness-detection:unit",
  internalClusterTest: "flakiness-detection:integ",
  javaRestTest: "flakiness-detection:java-rest",
  yamlRestTestRunner: "flakiness-detection:yaml-runner",
  yamlRestTestSuite: "flakiness-detection:yaml-suite",
  yamlRestTestCase: "flakiness-detection:yaml-case",
};

export interface RunnableCommand {
  kind: TestKind;
  label: string;     // "unit tests"
  key: string;       // "flakiness-detection:unit"
  command: string;   // shell-ready invocation
}

export interface AgentConfig {
  agents: typeof AGENTS;
  timeoutInMinutes: number;
  groupName: string;
}

export const DEFAULT_AGENT_CONFIG: AgentConfig = {
  agents: AGENTS,
  timeoutInMinutes: 60,
  groupName: "flakiness-detection",
};
