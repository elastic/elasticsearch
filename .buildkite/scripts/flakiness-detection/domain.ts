export const AGENTS = {
  provider: "gcp",
  image: "family/elasticsearch-ubuntu-2404",
  machineType: "n4-custom-32-98304",
  diskType: "hyperdisk-balanced",
  buildDirectory: "/dev/shm/bk",
};

export const SOURCE_SET_PATTERNS = [
  { regex: /^(.+)\/src\/test\/java\/(.+Tests)\.java$/, sourceSet: "test", kind: "test" as const },
  {
    regex: /^(.+)\/src\/internalClusterTest\/java\/(.+IT)\.java$/,
    sourceSet: "internalClusterTest",
    kind: "internalClusterTest" as const,
  },
  { regex: /^(.+)\/src\/javaRestTest\/java\/(.+IT)\.java$/, sourceSet: "javaRestTest", kind: "javaRestTest" as const },
  {
    regex: /^(.+)\/src\/yamlRestTest\/resources\/rest-api-spec\/test\/(.+)\.yml$/,
    sourceSet: "yamlRestTest",
    kind: "yamlRestTestSuite" as const,
  },
  {
    regex: /^(.+)\/src\/yamlRestTest\/java\/(.+IT)\.java$/,
    sourceSet: "yamlRestTest",
    kind: "yamlRestTestRunner" as const,
  },
];

// yamlRestTestCase is synthesised from muted-tests.yml unmutes and does not
// correspond to a file pattern; the rest of the kinds are derived from files
// matched by SOURCE_SET_PATTERNS.
type PatternKind = (typeof SOURCE_SET_PATTERNS)[number]["kind"];
export type TestKind = PatternKind | "yamlRestTestCase";

// Data-backed rollout caps sized from recent gradle-tests p95 durations
// multiplied by each kind's flakiness iteration count, targeting roughly
// 50 minutes of projected test runtime per batch.
export const BATCH_CAPS: Record<TestKind, number> = {
  test: 3,
  internalClusterTest: 2,
  javaRestTest: 1,
  yamlRestTestSuite: 4,
  yamlRestTestRunner: 1,
  yamlRestTestCase: 4,
};

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

export interface TestRef {
  className: string;
  method?: string;
}

export function toGradleProject(path: string): string {
  const segments = path.split("/");
  // Mirror the rename in settings.gradle: direct children of :test:external-modules
  // have their project name prefixed with "test-".
  if (segments[0] === "test" && segments[1] === "external-modules" && segments.length >= 3) {
    segments[2] = `test-${segments[2]}`;
  }
  return ":" + segments.join(":");
}

export function toFqcn(javaPath: string): string {
  return javaPath.replace(/\//g, ".");
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

export interface BatchingConfig {
  capByKind: Record<TestKind, number>;
  itersByKind: Record<"test" | "internalClusterTest", number>;
  // Loop count passed to .ci/scripts/repeat-rest-test.sh for all REST test kinds.
  // Shared across javaRestTest / yamlRestTestRunner / yamlRestTestSuite / yamlRestTestCase
  // because the bash wrapper has one knob and there's no operator scenario justifying
  // per-kind values.
  restIters: number;
  suiteTimeoutMs: number;
  // "buildkite" emits `.ci/scripts/run-gradle.sh ...` (the BK-agent wrapper that
  // copies init.gradle, computes MAX_WORKERS, reads ldd version, etc. — Linux-only).
  // "local" emits `./gradlew ...` directly, suitable for a developer laptop.
  // The `.ci/scripts/repeat-rest-test.sh` wrapper is portable bash and is used
  // for both targets.
  target: "buildkite" | "local";
}

export const DEFAULT_BATCHING_CONFIG: BatchingConfig = {
  capByKind: BATCH_CAPS,
  itersByKind: { test: 100, internalClusterTest: 20 },
  restIters: 10,
  suiteTimeoutMs: 3_600_000,
  target: "buildkite",
};

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
