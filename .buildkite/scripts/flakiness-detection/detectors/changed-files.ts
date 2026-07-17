import type { ClassifiedTest } from "../domain.ts";

import { SOURCE_SET_PATTERNS, toFqcn, toGradleProject } from "../domain.ts";
import { isAbstractTestClass, type JavaSourceReader } from "./abstract.ts";

export function classifyChangedFiles(files: string[], readSource?: JavaSourceReader): ClassifiedTest[] {
  const tests: ClassifiedTest[] = [];

  for (const file of files) {
    for (const pattern of SOURCE_SET_PATTERNS) {
      const match = file.match(pattern.regex);
      if (match) {
        // `match[2]` is the class path for java kinds and the yaml path for
        // yamlRestTestSuite. Skip abstract base classes (java kinds only): their
        // `*Tests`/`*IT` name matches but `--tests` on them runs nothing.
        if (readSource && pattern.kind !== "yamlRestTestSuite") {
          const source = readSource(file);
          if (source && isAbstractTestClass(source, match[2].split("/").pop()!)) {
            break;
          }
        }

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
