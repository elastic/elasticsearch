import { describe, expect, test } from "vitest";
import { mayBeTestSource, resolveMergeBaseTarget } from "./pr.ts";

describe("mayBeTestSource", () => {
  test("accepts every shape the resolver can act on", () => {
    // The four candidate source sets, java and resources, plus a non-conventional project depth.
    for (const path of [
      "server/src/test/java/org/elasticsearch/FooTests.java",
      "server/src/internalClusterTest/java/org/elasticsearch/BarIT.java",
      "modules/transport-netty4/src/javaRestTest/java/org/elasticsearch/RestIT.java",
      "x-pack/plugin/ml/src/yamlRestTest/java/org/elasticsearch/MlYamlIT.java",
      "x-pack/plugin/ml/src/yamlRestTest/resources/rest-api-spec/test/ml/10_foo.yml",
      "x-pack/plugin/esql/qa/server/single-node/src/csvSpecTest/java/org/elasticsearch/SpecIT.java",
    ]) {
      expect(mayBeTestSource(path), path).toBe(true);
    }
  });

  test("rejects the paths that make a PR pay the resolver for nothing", () => {
    for (const path of [
      "docs/reference/esql/functions/case.md",
      "build.gradle",
      "gradle/verification-metadata.xml",
      "muted-tests.yml",
      ".buildkite/pipelines/pull-request/flakiness-detection.yml",
      "README.asciidoc",
    ]) {
      expect(mayBeTestSource(path), path).toBe(false);
    }
  });

  test("is a cost gate, not a classifier: non-test sources still pass", () => {
    // src/main is not a candidate source set, so the resolver will ignore it - but excluding it here would
    // be classification, and the gate is deliberately over-inclusive.
    expect(mayBeTestSource("server/src/main/java/org/elasticsearch/index/Index.java")).toBe(true);
  });

  test("matches a src segment at any depth, including the repo root", () => {
    expect(mayBeTestSource("src/test/java/Foo.java")).toBe(true);
    // ...but not a directory that merely starts with "src".
    expect(mayBeTestSource("srcgen/test/java/Foo.java")).toBe(false);
    expect(mayBeTestSource("docs/src-notes/foo.md")).toBe(false);
  });
});

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
