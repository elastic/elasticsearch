import { ClassifiedTest, MutedEntry } from "../domain";
import { locateUnmutedTest } from "./unmutes";

export interface UnresolvedSpec {
  spec: string;
}

export interface ExplicitListResult {
  located: ClassifiedTest[];
  unlocated: UnresolvedSpec[];
}

/**
 * A spec is one of:
 *   - "org.foo.BarTests"                 — whole class
 *   - "org.foo.BarTests.methodName"      — specific method (note: v1 returns
 *     class-level ClassifiedTest; method filtering is deferred)
 *   - "org.foo.YamlIT.test {yaml=...}"   — specific yaml test case
 *
 * Strategy: parse each spec into a MutedEntry shape, then delegate to
 * locateUnmutedTest, which already knows how to map (class, method) to the
 * right ClassifiedTest. This keeps the source-set + yaml-case heuristics in
 * exactly one place (unmutes.ts).
 */
export function classifyExplicitList(
  specs: string[],
  repoFiles: string[]
): ExplicitListResult {
  const located: ClassifiedTest[] = [];
  const unlocated: UnresolvedSpec[] = [];

  for (const raw of specs) {
    const spec = raw.trim();
    if (spec === "") continue;

    const entry = parseSpec(spec);
    const result = locateUnmutedTest(entry, repoFiles);
    if (result === null) {
      unlocated.push({ spec });
    } else {
      located.push(result);
    }
  }

  return { located, unlocated };
}

const YAML_METHOD_PREFIX = "test {yaml=";

function parseSpec(spec: string): MutedEntry {
  // "ClassName.test {yaml=...}" — class is everything before the first dot
  // that precedes the yaml-case prefix.
  const yamlIdx = spec.indexOf(`.${YAML_METHOD_PREFIX}`);
  if (yamlIdx !== -1) {
    return {
      className: spec.slice(0, yamlIdx),
      method: spec.slice(yamlIdx + 1),
    };
  }

  // "ClassName.methodName" — split on the last dot only if the part after
  // the dot looks like a Java method identifier (camelCase starting lowercase).
  const lastDot = spec.lastIndexOf(".");
  if (lastDot !== -1) {
    const tail = spec.slice(lastDot + 1);
    if (/^[a-z][A-Za-z0-9_]*$/.test(tail)) {
      return { className: spec.slice(0, lastDot), method: tail };
    }
  }

  return { className: spec };
}
