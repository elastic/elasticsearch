import { describe, expect, test } from "vitest";

import type { ClassifiedTest } from "../domain.ts";

import { generateBatchCommand } from "../commands.ts";
import { DEFAULT_BATCHING_CONFIG } from "../domain.ts";
import { reclassifyExplicitYamlSuites, usesExplicitYamlRestPaths, type FileReader } from "./yaml-explicit-paths.ts";

// An in-memory reader keyed by repo-relative path.
function reader(files: Record<string, string>): FileReader {
  return (p) => (p in files ? files[p] : null);
}

const WATCHER_RUNNER = "x-pack/plugin/watcher/src/yamlRestTest/java/org/elasticsearch/smoketest/WatcherYamlRestIT.java";
const WATCHER_SRC = `
@ParametersFactory
public static Iterable<Object[]> parameters() throws Exception {
    return createParameters("mustache", "painless", "watcher");
}`;

// A default runner that reads tests.rest.suite (unaffected).
const REINDEX_RUNNER = "modules/reindex/src/yamlRestTest/java/org/elasticsearch/reindex/ReindexClientYamlTestSuiteIT.java";
const REINDEX_SRC = `
@ParametersFactory
public static Iterable<Object[]> parameters() throws Exception {
    return ESClientYamlSuiteTestCase.createParameters();
}`;

describe("usesExplicitYamlRestPaths", () => {
  test("true when a runner in the project's yamlRestTest source set uses explicit paths", () => {
    const read = reader({ [WATCHER_RUNNER]: WATCHER_SRC });
    expect(usesExplicitYamlRestPaths(":x-pack:plugin:watcher", [WATCHER_RUNNER], read)).toBe(true);
  });

  test("false for a default runner that honours tests.rest.suite", () => {
    const read = reader({ [REINDEX_RUNNER]: REINDEX_SRC });
    expect(usesExplicitYamlRestPaths(":modules:reindex", [REINDEX_RUNNER], read)).toBe(false);
  });

  test("false when the project has no yamlRestTest source files", () => {
    expect(usesExplicitYamlRestPaths(":server", ["server/src/test/java/org/elasticsearch/FooTests.java"], reader({}))).toBe(false);
  });

  test("only scans the target project's yamlRestTest source set", () => {
    // An explicit-paths runner in a *different* project must not leak in.
    const read = reader({ [WATCHER_RUNNER]: WATCHER_SRC });
    expect(usesExplicitYamlRestPaths(":modules:reindex", [WATCHER_RUNNER], read)).toBe(false);
  });
});

describe("reclassifyExplicitYamlSuites", () => {
  const suite = (gradleProject: string, suitePath: string): ClassifiedTest => ({
    gradleProject,
    kind: "yamlRestTestSuite",
    sourceSet: "yamlRestTest",
    suitePath,
  });

  test("maps an explicit-paths project's suite to a whole-task runner (no suitePath)", () => {
    const read = reader({ [WATCHER_RUNNER]: WATCHER_SRC });
    const out = reclassifyExplicitYamlSuites([suite(":x-pack:plugin:watcher", "painless")], [WATCHER_RUNNER], read);
    expect(out).toEqual([{ gradleProject: ":x-pack:plugin:watcher", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" }]);
  });

  test("leaves a default project's suite untouched", () => {
    const read = reader({ [REINDEX_RUNNER]: REINDEX_SRC });
    const input = [suite(":modules:reindex", "reindex/10_basic")];
    expect(reclassifyExplicitYamlSuites(input, [REINDEX_RUNNER], read)).toEqual(input);
  });

  test("passes non-suite kinds through unchanged", () => {
    const unit: ClassifiedTest = { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "a.FooTests" };
    expect(reclassifyExplicitYamlSuites([unit], [], reader({}))).toEqual([unit]);
  });

  test("scans each project's source set at most once", () => {
    const reads: string[] = [];
    const read: FileReader = (p) => {
      reads.push(p);
      return p === WATCHER_RUNNER ? WATCHER_SRC : null;
    };
    reclassifyExplicitYamlSuites(
      [suite(":x-pack:plugin:watcher", "painless"), suite(":x-pack:plugin:watcher", "watcher")],
      [WATCHER_RUNNER],
      read,
    );
    // One scan for the single project despite two suites.
    expect(reads).toEqual([WATCHER_RUNNER]);
  });

  test("end-to-end: a reclassified suite generates a whole-task rerun with no tests.rest.suite", () => {
    const read = reader({ [WATCHER_RUNNER]: WATCHER_SRC });
    const [reclassified] = reclassifyExplicitYamlSuites([suite(":x-pack:plugin:watcher", "painless")], [WATCHER_RUNNER], read);
    const command = generateBatchCommand([reclassified], DEFAULT_BATCHING_CONFIG);
    expect(command).toContain(":x-pack:plugin:watcher:yamlRestTest --rerun");
    expect(command).not.toContain("tests.rest.suite");
  });
});
