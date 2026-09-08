import type { ClassifiedTest } from "../domain.ts";

import { isSeparateBuild, SOURCE_SET_PATTERNS, toFqcn, toGradleProject } from "../domain.ts";

export function classifyChangedFiles(files: string[]): ClassifiedTest[] {
  const tests: ClassifiedTest[] = [];

  for (const file of files) {
    // Skip builds the root build cannot address; deriving a task path for them fails the compile gate.
    if (isSeparateBuild(file)) continue;

    for (const pattern of SOURCE_SET_PATTERNS) {
      const match = file.match(pattern.regex);
      if (match) {
        const test: ClassifiedTest = {
          gradleProject: toGradleProject(match[1]),
          kind: pattern.kind,
          sourceSet: pattern.sourceSet,
        };

        if (pattern.kind === "yamlRestTestSuite") {
          test.suitePath = match[2];
        } else if (pattern.kind !== "yamlRestTestRunner") {
          test.fqcn = toFqcn(match[2]);
        }

        tests.push(test);
        break;
      }
    }
  }

  return tests;
}
