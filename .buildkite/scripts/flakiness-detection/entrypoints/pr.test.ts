import { describe, expect, test } from "vitest";
import { resolveMergeBaseTarget } from "./pr.ts";

describe("resolveMergeBaseTarget", () => {
  test("uses target branch directly when ref exists locally", () => {
    const commands: string[] = [];
    const runner = (command: string): Buffer => {
      commands.push(command);
      return Buffer.from("");
    };

    const result = resolveMergeBaseTarget("main", runner, "/repo");

    expect(result).toBe("main");
    expect(commands).toEqual(["git rev-parse --verify main^{commit}"]);
  });

  test("fetches remote target and falls back to FETCH_HEAD when ref is missing", () => {
    const commands: string[] = [];
    const runner = (command: string): Buffer => {
      commands.push(command);
      if (command.startsWith("git rev-parse")) {
        throw new Error("missing ref");
      }
      return Buffer.from("");
    };

    const result = resolveMergeBaseTarget("gh/MattAlp/1/base", runner, "/repo");

    expect(result).toBe("FETCH_HEAD");
    expect(commands).toEqual([
      "git rev-parse --verify gh/MattAlp/1/base^{commit}",
      "git fetch --no-tags origin gh/MattAlp/1/base",
    ]);
  });
});
