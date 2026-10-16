import { describe, expect, test } from "vitest";

import { BLOCKING_LABELS, FLAKINESS_PROVEN_EXIT_CODE, matchedBlockingLabels } from "./domain.ts";

describe("matchedBlockingLabels", () => {
  const OPTED_IN = BLOCKING_LABELS[0];

  test("returns the label that opted in, so the analyze step can name it", () => {
    expect(matchedBlockingLabels(OPTED_IN)).toEqual([OPTED_IN]);
  });

  test("finds it among other labels, whatever the spacing", () => {
    // Buildkite sets GITHUB_PR_LABELS as a comma-separated list; the spacing around entries is not
    // something we control, and pipeline.ts trims it the same way.
    expect(matchedBlockingLabels(`>bug, ${OPTED_IN} ,v9.3.0`)).toEqual([OPTED_IN]);
    expect(matchedBlockingLabels(`>bug, ${OPTED_IN},v9.3.0`)).toEqual([OPTED_IN]);
  });

  test("empty for a PR with no labels at all", () => {
    // Also the manually-triggered pipeline, which has no PR and so never sets the variable.
    expect(matchedBlockingLabels("")).toEqual([]);
    expect(matchedBlockingLabels(",, ,")).toEqual([]);
  });

  test("empty when no label opted in", () => {
    expect(matchedBlockingLabels(">bug,v9.3.0,Team:SomeOtherTeam")).toEqual([]);
  });

  test("matches exactly, so a near miss does not opt a team in by accident", () => {
    expect(matchedBlockingLabels(`${OPTED_IN}-something`)).toEqual([]);
    expect(matchedBlockingLabels(OPTED_IN.toLowerCase())).toEqual([]);
  });
});

describe("FLAKINESS_PROVEN_EXIT_CODE", () => {
  test("cannot collide with a code that means something other than a failing test", () => {
    // never-fail.sh propagates this one code and swallows every other. 124/137 are timeout's own exits, 1
    // is gradle's, and 2 is the wrapper's malformed-invocation exit: if the verdict shared any of them, a
    // labelled PR would go red for an infrastructure failure.
    expect([0, 1, 2, 124, 137]).not.toContain(FLAKINESS_PROVEN_EXIT_CODE);
    // Must also fit in a process exit status.
    expect(FLAKINESS_PROVEN_EXIT_CODE).toBeLessThan(126);
  });
});
