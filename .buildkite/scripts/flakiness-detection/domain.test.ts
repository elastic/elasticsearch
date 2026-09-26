import { describe, expect, test } from "vitest";
import { toGradleProject, toFqcn } from "./domain.ts";

describe("toGradleProject", () => {
  test("converts simple path", () => {
    expect(toGradleProject("server")).toBe(":server");
  });

  test("converts nested path", () => {
    expect(toGradleProject("x-pack/plugin/core")).toBe(":x-pack:plugin:core");
  });

  test("converts modules path", () => {
    expect(toGradleProject("modules/transport-netty4")).toBe(":modules:transport-netty4");
  });

  test("converts deeply nested qa path", () => {
    expect(toGradleProject("x-pack/plugin/ml/qa/native-multi-node-tests")).toBe(
      ":x-pack:plugin:ml:qa:native-multi-node-tests"
    );
  });

  test("prefixes test- on children of test/external-modules", () => {
    expect(toGradleProject("test/external-modules/apm-integration")).toBe(
      ":test:external-modules:test-apm-integration"
    );
  });

  test("leaves sibling test paths unchanged", () => {
    expect(toGradleProject("test/fixtures/some-fixture")).toBe(":test:fixtures:some-fixture");
  });
});

describe("toFqcn", () => {
  test("converts java package path to FQCN", () => {
    expect(toFqcn("org/elasticsearch/index/IndexTests")).toBe("org.elasticsearch.index.IndexTests");
  });

  test("converts deeply nested path", () => {
    expect(toFqcn("org/elasticsearch/xpack/core/security/AuthTests")).toBe(
      "org.elasticsearch.xpack.core.security.AuthTests"
    );
  });
});
