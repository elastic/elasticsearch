import { spawnSync } from "child_process";
import { resolve } from "path";
import { expect, test } from "vitest";

const ROOT = resolve(import.meta.dirname, "../..");

test("dra-maven-publish.sh retry and javadoc upload behavior", () => {
  const result = spawnSync("bash", [resolve(ROOT, ".buildkite/scripts/dra-maven-publish.test.sh")], {
    encoding: "utf8",
  });
  expect(result.stderr).toBe("");
  expect(result.stdout).toContain("All dra-maven-publish tests passed");
  expect(result.status).toBe(0);
});
