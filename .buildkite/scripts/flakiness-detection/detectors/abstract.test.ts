import { describe, expect, test } from "vitest";

import { isAbstractTestClass } from "./abstract.ts";

describe("isAbstractTestClass", () => {
  test("true for a public abstract class", () => {
    const src = "package x;\npublic abstract class AbstractFooTests extends ESTestCase {}";
    expect(isAbstractTestClass(src, "AbstractFooTests")).toBe(true);
  });

  test("true for a package-private abstract class", () => {
    expect(isAbstractTestClass("abstract class FooTests {}", "FooTests")).toBe(true);
  });

  test("false for a concrete class", () => {
    const src = "public class FooTests extends ESTestCase {}";
    expect(isAbstractTestClass(src, "FooTests")).toBe(false);
  });

  test("false when abstract applies to a different (inner/helper) class", () => {
    const src = "public class FooTests {\n  abstract class Helper {}\n}";
    expect(isAbstractTestClass(src, "FooTests")).toBe(false);
  });

  test("does not confuse an abstract method for an abstract class", () => {
    const src = "public class FooTests {\n  abstract void doThing();\n}";
    expect(isAbstractTestClass(src, "FooTests")).toBe(false);
  });

  test("handles a nested-class name containing regex metacharacters ($)", () => {
    // A `$` in the name would act as an end-anchor if not escaped, silently
    // failing the match. `Outer$Inner` really appears as a `class Outer$Inner`?
    // No - the source declares the inner simple name; we assert the escaped
    // literal is matched, not treated as an anchor.
    const src = "public abstract class Outer$Inner extends ESTestCase {}";
    expect(isAbstractTestClass(src, "Outer$Inner")).toBe(true);
    // And a concrete same-name class is correctly not matched.
    expect(isAbstractTestClass("public class Outer$Inner {}", "Outer$Inner")).toBe(false);
  });
});
