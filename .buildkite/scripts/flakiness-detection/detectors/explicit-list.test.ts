import { describe, expect, test } from "vitest";
import { explicitRefs } from "./explicit-list.ts";

describe("explicitRefs", () => {
  test("wraps each spec as an explicit ref", () => {
    expect(explicitRefs(["org.foo.BarTests", "org.foo.BazTests.testX"])).toEqual([
      { source: "explicit", spec: "org.foo.BarTests" },
      { source: "explicit", spec: "org.foo.BazTests.testX" },
    ]);
  });

  test("trims whitespace and drops blank specs", () => {
    expect(explicitRefs(["  org.foo.BarTests  ", "", "   ", "org.foo.BazTests"])).toEqual([
      { source: "explicit", spec: "org.foo.BarTests" },
      { source: "explicit", spec: "org.foo.BazTests" },
    ]);
  });

  test("preserves yaml-case specs verbatim (parsing is the resolver's job)", () => {
    expect(explicitRefs(["org.foo.YamlIT.test {yaml=/10_apm/Test name}"])).toEqual([
      { source: "explicit", spec: "org.foo.YamlIT.test {yaml=/10_apm/Test name}" },
    ]);
  });

  test("returns empty for an all-blank list", () => {
    expect(explicitRefs(["", "  "])).toEqual([]);
  });
});
