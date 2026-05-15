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
export type PatternKind = (typeof SOURCE_SET_PATTERNS)[number]["kind"];
export type TestKind = PatternKind | "yamlRestTestCase";

export const BATCH_CAPS: Record<TestKind, number> = {
  test: 360,
  internalClusterTest: 36,
  javaRestTest: 4,
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

export interface MutedEntry {
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

export const KIND_LABELS: Record<TestKind, string> = {
  test: "unit tests",
  internalClusterTest: "integ tests",
  javaRestTest: "java rest tests",
  yamlRestTestRunner: "yaml rest test runner",
  yamlRestTestSuite: "yaml rest tests",
  yamlRestTestCase: "yaml rest test cases",
};

export const KIND_KEYS: Record<TestKind, string> = {
  test: "repeat-changed-tests:unit",
  internalClusterTest: "repeat-changed-tests:integ",
  javaRestTest: "repeat-changed-tests:java-rest",
  yamlRestTestRunner: "repeat-changed-tests:yaml-runner",
  yamlRestTestSuite: "repeat-changed-tests:yaml-suite",
  yamlRestTestCase: "repeat-changed-tests:yaml-case",
};
