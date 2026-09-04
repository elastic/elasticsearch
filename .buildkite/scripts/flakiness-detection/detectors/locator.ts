import type {
  ClassifiedTest,
  TestRef,
} from "../domain.ts";

import {
  SOURCE_SET_PATTERNS,
  toFqcn,
  toGradleProject,
} from "../domain.ts";

const YAML_METHOD_REGEX = /^test \{yaml=.+\}$/;

/**
 * Maps a `(className, method?)` reference to the `ClassifiedTest` describing
 * which Gradle project, source set, and test kind it belongs to. Used by both
 * the unmute detector (resolving entries removed from muted-tests.yml) and the
 * explicit-list detector (resolving developer-supplied specs).
 *
 * Returns `null` when no source file matches the className — typically because
 * the class was deleted in the same PR.
 */
export function locateTest(ref: TestRef, repoFiles: string[]): ClassifiedTest | null {
  const pathSuffix = ref.className.replace(/\./g, "/") + ".java";
  const candidate = repoFiles.find((f) => f === pathSuffix || f.endsWith("/" + pathSuffix));
  if (candidate === undefined) return null;

  for (const pattern of SOURCE_SET_PATTERNS) {
    const match = candidate.match(pattern.regex);
    if (match === null) continue;

    const gradleProject = toGradleProject(match[1]);
    const kind = pattern.kind;

    switch (kind) {
      case "test":
      case "internalClusterTest":
      case "javaRestTest":
        return {
          gradleProject,
          kind,
          sourceSet: pattern.sourceSet,
          fqcn: toFqcn(match[2]),
        };
      case "yamlRestTestRunner":
        // A parameterized yaml test case is identified by its full descriptor
        // "test {yaml=<path>/<test name>}". We target it exactly via
        // `--tests "<FQCN>.test {yaml=...}"` rather than `tests.rest.suite`,
        // which only accepts file/directory paths and cannot address an
        // individual case.
        if (ref.method !== undefined && YAML_METHOD_REGEX.test(ref.method)) {
          return {
            gradleProject,
            kind: "yamlRestTestCase",
            sourceSet: "yamlRestTest",
            fqcn: ref.className,
            yamlTest: ref.method,
          };
        }
        return {
          gradleProject,
          kind: "yamlRestTestRunner",
          sourceSet: "yamlRestTest",
        };
      case "yamlRestTestSuite":
        // Unreachable: TestRef references a Java class, but yamlRestTestSuite
        // only matches `.yml` resources. Fail loudly so any future change to
        // SOURCE_SET_PATTERNS that breaks this invariant surfaces here rather
        // than later as a malformed ClassifiedTest in generateBatchCommand.
        throw new Error(`yamlRestTestSuite pattern unexpectedly matched Java file ${candidate}`);
      default:
        return assertNever(kind);
    }
  }
  return null;
}

function assertNever(x: never): never {
  throw new Error(`Unhandled SOURCE_SET_PATTERN kind: ${x as string}`);
}
