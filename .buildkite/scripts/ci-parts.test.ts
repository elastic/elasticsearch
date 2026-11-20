import { readFileSync, readdirSync, statSync } from "fs";
import { join } from "path";
import { describe, expect, test } from "vitest";

// checkPartN only runs the share of `check` that .ci/test-parts/assignment.json gives part N, so a
// pipeline that runs some parts but not all of them silently skips tests. These checks keep every
// pipeline in step with the assignment's part count.

const PROJECT_ROOT = join(import.meta.dirname, "..", "..");
const PIPELINES = join(PROJECT_ROOT, ".buildkite", "pipelines");
const readPartCount = (...path: string[]): number => {
  const assignment: unknown = JSON.parse(readFileSync(join(PROJECT_ROOT, ".ci", "test-parts", ...path), "utf-8"));
  if (typeof assignment !== "object" || assignment === null || !("parts" in assignment) || typeof assignment.parts !== "number") {
    throw new Error(`${path.join("/")} has no numeric 'parts'`);
  }
  return assignment.parts;
};
const partList = (parts: number): number[] => Array.from({ length: parts }, (_, i) => i + 1);
const PARTS = readPartCount("assignment.json");
const ALL_PARTS = partList(PARTS);
const BC_UPGRADE_PARTS = partList(readPartCount("bc-upgrade", "assignment.json"));
const ESQL_RELEASE_PARTS = partList(readPartCount("esql-release", "assignment.json"));

const pipelineFiles = (dir: string): string[] =>
  readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) {
      return pipelineFiles(path);
    }
    return name.endsWith(".yml") ? [path] : [];
  });

const partsReferenced = (yaml: string): number[] =>
  [...new Set([...yaml.matchAll(/\bcheckPart(\d+)\b/g)].map((m) => Number(m[1])))].sort((a, b) => a - b);

describe("CI parts", () => {
  test("pipelines that run several parts run all of them", () => {
    const files = pipelineFiles(PIPELINES).filter((f) => !/pull-request\/part-\d+[^/]*\.yml$/.test(f));
    const users = files.filter((f) => partsReferenced(readFileSync(f, "utf-8")).length > 0);
    expect(users.length).toBeGreaterThan(0);
    for (const file of users) {
      expect(partsReferenced(readFileSync(file, "utf-8")), file).toEqual(ALL_PARTS);
    }
  });

  test("upgrade test scripts run every bcUpgradeTestPartN", () => {
    for (const script of ["run-pr-upgrade-tests.sh", "run-bc-upgrade-tests.sh"]) {
      const text = readFileSync(join(PROJECT_ROOT, ".buildkite", "scripts", script), "utf-8");
      expect(text, script).toContain("bcUpgradeTestPart{{matrix.PART}}");
      const matrix = text.match(/PART:\s*\[([^\]]*)\]/);
      expect(matrix, script).not.toBeNull();
      const parts = [...(matrix?.[1] ?? "").matchAll(/\d+/g)].map((m) => Number(m[0]));
      expect(parts, script).toEqual(BC_UPGRADE_PARTS);
    }
  });

  test("the ES|QL release test pipeline runs every releaseTestCheckEsqlPartN", () => {
    const yaml = readFileSync(join(PIPELINES, "pull-request", "esql-release-tests.yml"), "utf-8");
    const parts = [...new Set([...yaml.matchAll(/\breleaseTestCheckEsqlPart(\d+)\b/g)].map((m) => Number(m[1])))].sort((a, b) => a - b);
    expect(parts).toEqual(ESQL_RELEASE_PARTS);
  });

  test("every pull request part variant has one pipeline per part", () => {
    const dir = join(PIPELINES, "pull-request");
    const byVariant = new Map<string, number[]>();
    for (const name of readdirSync(dir)) {
      const match = name.match(/^part-(\d+)(.*)\.yml$/);
      if (match) {
        const variant = match[2];
        byVariant.set(variant, [...(byVariant.get(variant) || []), Number(match[1])]);
        expect(partsReferenced(readFileSync(join(dir, name), "utf-8")), name).toEqual([Number(match[1])]);
      }
    }
    expect([...byVariant.keys()].sort()).toEqual(["", "-arm", "-fips", "-fips-140-3", "-windows"]);
    for (const [variant, parts] of byVariant) {
      expect(parts.sort((a, b) => a - b), `part-N${variant}.yml`).toEqual(ALL_PARTS);
    }
  });
});
