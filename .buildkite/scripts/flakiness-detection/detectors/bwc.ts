import { readFileSync } from "fs";
import { join } from "path";

import type { ClassifiedTest } from "../domain.ts";

import { toProjectDir } from "../domain.ts";

// A project applies the BWC test plugin when its build script *applies* the
// `elasticsearch.bwc-test` plugin. On such projects the bare test task
// (`test` / `javaRestTest` / ...) is disabled by `elasticsearch.bwc-test.gradle`
// - the tests only run under the version-qualified `v<version>#bwcTest` tasks -
// so re-running the bare task the detector emits does nothing (0 tests, exit 0),
// which the analyzer would otherwise record as a `hang`. We therefore treat these
// as `not_applicable` ("nothing to re-run"). Actually re-running them via the
// version-qualified tasks is a separate, harder follow-up.
//
// Covers the Groovy `apply plugin: '...'` and `id '...'` forms and the Kotlin
// `id("...")` form.
const BWC_TEST_PLUGIN_APPLIED = /(?:apply\s+plugin\s*:\s*|id\s*\(?\s*)['"]elasticsearch\.bwc-test['"]/;

// Reads a project's build script, given its source directory (repo-relative).
// Returns null when no build script is found. Injected so the partition logic
// stays pure and unit-testable without touching the filesystem.
export type BuildScriptReader = (projectDir: string) => string | null;

export function defaultBuildScriptReader(repoRoot: string): BuildScriptReader {
  return (projectDir) => {
    for (const name of ["build.gradle", "build.gradle.kts"]) {
      try {
        return readFileSync(join(repoRoot, projectDir, name), "utf8");
      } catch {
        // Try the next candidate name; fall through to null if none exist.
      }
    }
    return null;
  };
}

export function isBwcProject(gradleProject: string, read: BuildScriptReader): boolean {
  const text = read(toProjectDir(gradleProject));
  return text !== null && BWC_TEST_PLUGIN_APPLIED.test(text);
}

export interface BwcPartition {
  // Tests whose bare task actually runs them - fan out to batch jobs as usual.
  runnable: ClassifiedTest[];
  // Tests on BWC projects that the bare task cannot run - recorded as
  // `not_applicable`, never fanned out.
  notApplicable: ClassifiedTest[];
}

/**
 * Splits the detected tests into runnable ones and BWC ones that cannot be
 * re-run via the bare task. The BWC decision is cached per Gradle project so
 * each `build.gradle` is read at most once.
 */
export function partitionByBwc(tests: ClassifiedTest[], read: BuildScriptReader): BwcPartition {
  const cache = new Map<string, boolean>();
  const runnable: ClassifiedTest[] = [];
  const notApplicable: ClassifiedTest[] = [];
  for (const t of tests) {
    let bwc = cache.get(t.gradleProject);
    if (bwc === undefined) {
      bwc = isBwcProject(t.gradleProject, read);
      cache.set(t.gradleProject, bwc);
    }
    (bwc ? notApplicable : runnable).push(t);
  }
  return { runnable, notApplicable };
}
