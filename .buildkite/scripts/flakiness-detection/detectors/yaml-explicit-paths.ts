import { readFileSync } from "fs";
import { join } from "path";

import type { ClassifiedTest } from "../domain.ts";

// A yaml REST test runner declares its suites in one of two ways. The default
// `ESClientYamlSuiteTestCase.createParameters()` / `createParameters(registry)`
// overloads read the suite set from the `tests.rest.suite` system property. The
// "explicit paths" overloads - `createParameters("watcher")`,
// `createParameters("gpu/supported")`, ... - hard-wire the suite dirs in code and
// THROW `IllegalArgumentException("The 'tests.rest.suite' system property is not
// supported with explicit test paths.")` if that property is set (including the
// per-task scoped form `tests.rest.suite.<taskPath>`).
//
// The `yamlRestTestSuite` command sets exactly that scoped property, so on a
// project whose runner uses explicit paths every re-run iteration fails in
// `initializationError` and is recorded as a false `flaky_detected`. We detect
// such projects statically and re-run their whole `yamlRestTest` task instead
// (see reclassifyExplicitYamlSuites).
//
// Heuristic: a `createParameters(` call whose first argument is a string literal.
// This matches every explicit-paths runner in the repo today. Known blind spots
// (none present today): a @ParametersFactory inherited from a parent in another
// module, and a non-literal path argument.
const EXPLICIT_PATHS_CALL = /createParameters\(\s*"/;

// Reads a repo-relative file, returning its text or null when it cannot be read.
// Injected so the detector stays pure and unit-testable without the filesystem.
export type FileReader = (repoRelativePath: string) => string | null;

export function defaultFileReader(repoRoot: string): FileReader {
  return (repoRelativePath) => {
    try {
      return readFileSync(join(repoRoot, repoRelativePath), "utf8");
    } catch {
      return null;
    }
  };
}

// Repo-relative source dir of a project's yamlRestTest Java source set, e.g.
// `:x-pack:plugin:watcher` -> `x-pack/plugin/watcher/src/yamlRestTest/java/`.
function yamlRestTestSrcPrefix(gradleProject: string): string {
  const dir = gradleProject.replace(/^:/, "").split(":").join("/");
  return `${dir}/src/yamlRestTest/java/`;
}

/**
 * True when any concrete runner in the project's yamlRestTest source set uses the
 * explicit-paths `createParameters(...)` overload, so its `yamlRestTest` task
 * rejects the `tests.rest.suite` system property. One such runner is enough: the
 * task runs all its runner classes, so a single explicit-paths runner fails the
 * whole invocation when the property is set.
 */
export function usesExplicitYamlRestPaths(gradleProject: string, repoFiles: string[], read: FileReader): boolean {
  const prefix = yamlRestTestSrcPrefix(gradleProject);
  for (const f of repoFiles) {
    if (!f.startsWith(prefix) || !f.endsWith(".java")) continue;
    const src = read(f);
    if (src !== null && EXPLICIT_PATHS_CALL.test(src)) {
      return true;
    }
  }
  return false;
}

/**
 * Reclassify each `yamlRestTestSuite` whose project cannot accept
 * `tests.rest.suite` into a whole-task `yamlRestTestRunner` run (which emits
 * `:proj:yamlRestTest --rerun`, with no suite property). Every other entry passes
 * through unchanged. The per-project decision is cached so each source set is
 * scanned at most once.
 */
export function reclassifyExplicitYamlSuites(tests: ClassifiedTest[], repoFiles: string[], read: FileReader): ClassifiedTest[] {
  const cache = new Map<string, boolean>();
  return tests.map((t) => {
    if (t.kind !== "yamlRestTestSuite") return t;
    let explicit = cache.get(t.gradleProject);
    if (explicit === undefined) {
      explicit = usesExplicitYamlRestPaths(t.gradleProject, repoFiles, read);
      cache.set(t.gradleProject, explicit);
    }
    if (!explicit) return t;
    // Whole-task rerun: drop the suite path and target the task directly.
    return { gradleProject: t.gradleProject, kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" };
  });
}
