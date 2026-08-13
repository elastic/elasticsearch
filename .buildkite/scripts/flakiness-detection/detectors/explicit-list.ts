import type { FlakinessRef } from "../domain.ts";

/**
 * Turn developer-supplied specs (from `FLAKINESS_CLASSES` or the local CLI) into `explicit`
 * {@link FlakinessRef}s. A spec is one of:
 *   - `org.foo.BarTests`                 - whole class
 *   - `org.foo.BarTests.methodName`      - specific method
 *   - `org.foo.YamlIT.test {yaml=...}`   - specific yaml case
 *
 * Parsing the spec into (class, method) and mapping it to a source set is now the Java resolver's job
 * (`RefResolver.parseSpec`); this gatherer only trims and wraps, so the spec grammar lives in exactly one
 * place. Blank specs are dropped.
 */
export function explicitRefs(specs: string[]): FlakinessRef[] {
  return specs
    .map((s) => s.trim())
    .filter((s) => s !== "")
    .map((spec) => ({ source: "explicit" as const, spec }));
}
