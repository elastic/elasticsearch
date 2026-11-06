import { describe, expect, test } from "vitest";

import type { BuildkiteStep } from "../pull-request/types.ts";
import { type IntakeVersionPlan, intakeBwcGroup, unpinnedIntakeBwcGroup } from "./intake-pipeline.ts";

// A known-good record is just the build ID.
const knownGood = (buildId: string): string => buildId;

const latest = (buildId: string) => ({ buildId, version: buildId.substring(0, buildId.lastIndexOf("-")) });

// Label, command tail and the step attributes that matter for pinning/promotion; agents and retry are constant.
const summary = (group: BuildkiteStep): unknown[] =>
  (group.steps ?? []).map((step) => ({
    label: step.label,
    key: step.key,
    command: String(step.command).replace(/^.*-Dtests\.bwc\.mode=auto/, "..."),
    ...(step.soft_fail ? { soft_fail: step.soft_fail } : {}),
    ...(step.depends_on ? { depends_on: step.depends_on, allow_dependency_failure: step.allow_dependency_failure } : {}),
    ...(step.env ? { env: step.env } : {}),
  }));

describe("intake bwc-snapshots group", () => {
  test("pins every branch to its known-good build and only tests newer builds as soft-failing candidates", () => {
    const plans: IntakeVersionPlan[] = [
      // latest == known good: nothing new to test
      { version: "8.19.25", branch: "8.19", knownGood: knownGood("8.19.25-aaaa1111"), latest: latest("8.19.25-aaaa1111") },
      // a newer DRA build exists: candidate + promote
      { version: "9.4.10", branch: "9.4", knownGood: knownGood("9.4.10-bbbb2222"), latest: latest("9.4.10-cccc3333") },
      // no known-good build yet (gated): candidate only
      { version: "9.6.0", branch: "9.6", latest: latest("9.6.0-dddd4444") },
      // nothing in DRA yet (gated): no job
      { version: "9.5.7", branch: "9.5" },
      // the version this checkout builds
      { version: "9.7.0", branch: "9.7", current: true },
    ];

    const group = intakeBwcGroup(plans);

    expect(group.group).toBe("bwc-snapshots");
    expect(summary(group)).toEqual([
      {
        label: "8.19.25 / bwc-snapshots (known-good 8.19.25-aaaa1111)",
        key: "bwc-snapshots-8_19_25",
        command: "... -Dtests.bwc.dra.hash.8.19=aaaa1111 -Dtests.bwc.dra.hash.9.4=bbbb2222 v8.19.25#bwcTest",
        env: { BWC_VERSION: "8.19.25" },
      },
      {
        label: "9.4.10 / bwc-snapshots (known-good 9.4.10-bbbb2222)",
        key: "bwc-snapshots-9_4_10",
        command: "... -Dtests.bwc.dra.hash.8.19=aaaa1111 -Dtests.bwc.dra.hash.9.4=bbbb2222 v9.4.10#bwcTest",
        env: { BWC_VERSION: "9.4.10" },
      },
      {
        label: "9.4.10 / bwc-snapshots candidate 9.4.10-cccc3333",
        key: "bwc-snapshots-candidate-9_4_10",
        command: "... -Dtests.bwc.dra.hash.8.19=aaaa1111 -Dtests.bwc.dra.hash.9.4=cccc3333 v9.4.10#bwcTest",
        soft_fail: true,
        env: { BWC_VERSION: "9.4.10" },
      },
      {
        label: "9.4.10 / promote 9.4.10-cccc3333 to known-good",
        key: "bwc-snapshots-promote-9_4_10",
        command:
          "node .buildkite/scripts/bwc-snapshots/cli.ts promote --branch 9.4 --build-id 9.4.10-cccc3333 --step bwc-snapshots-candidate-9_4_10",
        depends_on: "bwc-snapshots-candidate-9_4_10",
        allow_dependency_failure: true,
        env: { USE_BWC_KNOWN_GOOD_CREDENTIALS: "true" },
      },
      // gated: only a soft-failing candidate until it has a known-good build
      {
        label: "9.6.0 / bwc-snapshots candidate 9.6.0-dddd4444 (not gating: no known-good build yet)",
        key: "bwc-snapshots-candidate-9_6_0",
        command:
          "... -Dtests.bwc.dra.hash.8.19=aaaa1111 -Dtests.bwc.dra.hash.9.4=bbbb2222 -Dtests.bwc.dra.hash.9.6=dddd4444 v9.6.0#bwcTest",
        soft_fail: true,
        env: { BWC_VERSION: "9.6.0" },
      },
      {
        label: "9.6.0 / promote 9.6.0-dddd4444 to known-good",
        key: "bwc-snapshots-promote-9_6_0",
        command:
          "node .buildkite/scripts/bwc-snapshots/cli.ts promote --branch 9.6 --build-id 9.6.0-dddd4444 --step bwc-snapshots-candidate-9_6_0",
        depends_on: "bwc-snapshots-candidate-9_6_0",
        allow_dependency_failure: true,
        env: { USE_BWC_KNOWN_GOOD_CREDENTIALS: "true" },
      },
      // 9.5.7: nothing in DRA to test or promote yet; periodic source builds cover it
      {
        label: "9.7.0 / bwc-snapshots",
        key: "bwc-snapshots-9_7_0",
        command: "... -Dtests.bwc.dra.hash.8.19=aaaa1111 -Dtests.bwc.dra.hash.9.4=bbbb2222 v9.7.0#bwcTest",
        env: { BWC_VERSION: "9.7.0" },
      },
    ]);
  });

  test("bwc jobs keep intake's agents, retries and gradle flags", () => {
    const [job] = intakeBwcGroup([{ version: "9.4.10", branch: "9.4", knownGood: knownGood("9.4.10-bbbb2222") }]).steps ?? [];

    expect(job?.command).toBe(
      ".ci/scripts/run-gradle.sh -Dbwc.checkout.align=true -Dorg.elasticsearch.build.cache.push=true -Dignore.tests.seed -Dscan.capture-file-fingerprints -Dtests.bwc.mode=auto -Dtests.bwc.dra.hash.9.4=bbbb2222 v9.4.10#bwcTest",
    );
    expect(job?.agents).toMatchObject({ machineType: "n4-standard-16", preemptible: true });
    expect(job?.retry).toMatchObject({ automatic: expect.arrayContaining([{ limit: 3, exit_status: "47", signal_reason: "none" }]) });
  });

  test("without the store intake tests the moving tips exactly as before", () => {
    expect(summary(unpinnedIntakeBwcGroup(["9.4.10", "9.7.0"]))).toEqual([
      { label: "9.4.10 / bwc-snapshots", key: "bwc-snapshots-9_4_10", command: "... v9.4.10#bwcTest", env: { BWC_VERSION: "9.4.10" } },
      { label: "9.7.0 / bwc-snapshots", key: "bwc-snapshots-9_7_0", command: "... v9.7.0#bwcTest", env: { BWC_VERSION: "9.7.0" } },
    ]);
  });

  test("alerts on Slack when a version has not gone green", () => {
    const group = intakeBwcGroup(
      [{ version: "9.6.0", branch: "9.6", latest: latest("9.6.0-dddd4444") }],
      [
        {
          version: "9.6.0",
          branch: "9.6",
          since: "2026-11-01T00:00:00.000Z",
          hours: 50,
          latestBuildId: "9.6.0-dddd4444",
          knownGoodBuildId: undefined,
        },
      ],
      { channel: "#es-delivery-alerts", mention: "<@U123>" },
    );

    const alert = group.steps?.find((step) => step.key === "bwc-snapshots-alert-9_6_0");
    expect(alert?.label).toBe(":rotating_light: 9.6.0 bwc not green for 50h");
    expect(alert?.soft_fail).toBe(true);
    expect(alert?.notify).toEqual([
      {
        slack: {
          channels: ["#es-delivery-alerts"],
          message:
            "<@U123> bwc 9.6.0 has had no passing bwc run since 2026-11-01T00:00:00.000Z (50h), so it is left out of PR, merge-queue and intake bwc. Newest DRA build: 9.6.0-dddd4444. ${BUILDKITE_BUILD_URL}",
        },
      },
    ]);
  });
});
