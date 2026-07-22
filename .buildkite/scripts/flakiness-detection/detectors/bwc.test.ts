import { describe, expect, test } from "vitest";

import type { ClassifiedTest } from "../domain.ts";

import { isBwcProject, partitionByBwc, type BuildScriptReader } from "./bwc.ts";

// A reader backed by an in-memory map keyed by the project source directory.
function reader(files: Record<string, string>): BuildScriptReader {
  return (projectDir) => (projectDir in files ? files[projectDir] : null);
}

const bwcBuildGradle = `
apply plugin: 'elasticsearch.internal-java-rest-test'
apply plugin: 'elasticsearch.bwc-test'

buildParams.bwcVersions.withWireCompatible { bwcVersion, baseName -> }
`;

const plainBuildGradle = `
apply plugin: 'elasticsearch.internal-java-rest-test'
dependencies { }
`;

describe("isBwcProject", () => {
  test("true when build.gradle applies elasticsearch.bwc-test", () => {
    const read = reader({ "x-pack/plugin/logsdb/qa/rolling-upgrade": bwcBuildGradle });
    expect(isBwcProject(":x-pack:plugin:logsdb:qa:rolling-upgrade", read)).toBe(true);
  });

  test("false for a plain project", () => {
    const read = reader({ "server": plainBuildGradle });
    expect(isBwcProject(":server", read)).toBe(false);
  });

  test("false when no build script exists", () => {
    expect(isBwcProject(":does:not:exist", reader({}))).toBe(false);
  });

  test("false when the plugin id only appears in a comment or unrelated string", () => {
    const commented = "// TODO: this is not a elasticsearch.bwc-test project\nString s = \"elasticsearch.bwc-test\"\n";
    expect(isBwcProject(":server", reader({ server: commented }))).toBe(false);
  });

  test("true for the Kotlin id(\"...\") application form", () => {
    const kts = 'plugins {\n  id("elasticsearch.bwc-test")\n}\n';
    expect(isBwcProject(":qa:kts", reader({ "qa/kts": kts }))).toBe(true);
  });

  test("maps the :test:external-modules test- rename back to its directory", () => {
    const read = reader({ "test/external-modules/foo": bwcBuildGradle });
    expect(isBwcProject(":test:external-modules:test-foo", read)).toBe(true);
  });
});

describe("partitionByBwc", () => {
  test("splits BWC tests into notApplicable and keeps the rest runnable", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.FooTests" },
      { gradleProject: ":x-pack:plugin:logsdb:qa:rolling-upgrade", kind: "javaRestTest", sourceSet: "javaRestTest", fqcn: "org.elasticsearch.BwcIT" },
    ];
    const read = reader({
      "server": plainBuildGradle,
      "x-pack/plugin/logsdb/qa/rolling-upgrade": bwcBuildGradle,
    });

    const { runnable, notApplicable } = partitionByBwc(tests, read);
    expect(runnable).toEqual([tests[0]]);
    expect(notApplicable).toEqual([tests[1]]);
  });

  test("reads each project's build script at most once", () => {
    const reads: string[] = [];
    const read: BuildScriptReader = (projectDir) => {
      reads.push(projectDir);
      return projectDir === "qa/bwc" ? bwcBuildGradle : plainBuildGradle;
    };
    const tests: ClassifiedTest[] = [
      { gradleProject: ":qa:bwc", kind: "javaRestTest", sourceSet: "javaRestTest", fqcn: "a.A" },
      { gradleProject: ":qa:bwc", kind: "javaRestTest", sourceSet: "javaRestTest", fqcn: "a.B" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "a.C" },
    ];

    const { runnable, notApplicable } = partitionByBwc(tests, read);
    expect(reads).toEqual(["qa/bwc", "server"]);
    expect(notApplicable).toHaveLength(2);
    expect(runnable).toHaveLength(1);
  });
});
