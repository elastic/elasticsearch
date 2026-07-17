import { readFileSync } from "fs";
import { join } from "path";

// Reads a repo-relative `.java` file, returning its text or null when it cannot
// be read. Injected so the detectors stay pure and unit-testable without the
// filesystem.
export type JavaSourceReader = (repoRelativePath: string) => string | null;

export function defaultJavaSourceReader(repoRoot: string): JavaSourceReader {
  return (repoRelativePath) => {
    try {
      return readFileSync(join(repoRoot, repoRelativePath), "utf8");
    } catch {
      return null;
    }
  };
}

/**
 * True when the given source declares `<simpleClassName>` as an abstract class.
 *
 * Abstract base classes are named to the same `*Tests`/`*IT` conventions that
 * `SOURCE_SET_PATTERNS` matches (e.g. `AbstractFooTests`), so the detector would
 * emit `--tests <AbstractFooTests>` - which matches zero runnable tests and,
 * because `MutedTestPlugin` sets `failOnNoMatchingTests(ci == false)`, passes
 * silently in CI and is recorded as a `hang`. Skipping abstract classes up front
 * removes that whole class of silent no-op re-runs.
 */
export function isAbstractTestClass(source: string, simpleClassName: string): boolean {
  // The class declaration, allowing modifiers before `abstract` (e.g.
  // `public abstract class Foo`). `abstract` sits immediately before `class`.
  // Escape the class name: a nested-class descriptor can carry a `$`
  // (`Outer$Inner`), which would otherwise act as a regex end-anchor and make
  // the match silently fail - re-admitting the abstract class as a `hang`.
  return new RegExp(`\\babstract\\s+class\\s+${escapeRegExp(simpleClassName)}\\b`).test(source);
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
