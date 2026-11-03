// Generates intake's bwc-snapshots group.
//
// Every snapshot bwc version gets one blocking job. When the branch has a known-good DRA build the job is pinned to
// it, so a broken branch tip does not turn intake red. When the newest DRA build of the branch differs from the
// known-good one, a candidate job tests the newest build (soft-failing: the branch, not this commit, is at fault if
// it fails) and a promote step records it as known good once the candidate passed.

import type { BuildkiteStep } from "../pull-request/types.ts";
import type { Dra, DraBuild } from "./dra.ts";
import {
  type KnownGoodStore,
  type VersionPin,
  branchForVersion,
  knownGoodBuildIds,
  latestBuildOf,
  pinArgs,
  resolvePins,
} from "./known-good.ts";

export type IntakeVersionPlan = VersionPin & {
  /** Newest DRA build of the branch, when it is a build of this version */
  latest?: DraBuild;
};

const GRADLE =
  ".ci/scripts/run-gradle.sh -Dbwc.checkout.align=true -Dorg.elasticsearch.build.cache.push=true -Dignore.tests.seed -Dscan.capture-file-fingerprints -Dtests.bwc.mode=auto";

const AGENTS = {
  provider: "gcp",
  image: "family/elasticsearch-ubuntu-2404",
  machineType: "n4-standard-16",
  preemptible: true,
  spotZones:
    "asia-south2-a,asia-south2-b,asia-south2-c,europe-west2-a,europe-west2-b,europe-west2-c,northamerica-northeast2-b,northamerica-northeast2-c,southamerica-east1-a,southamerica-east1-b,southamerica-east1-c",
  diskType: "hyperdisk-balanced",
  buildDirectory: "/dev/shm/bk",
};

const RETRY = {
  manual: { reason: "Retry with smart test selection if desired", allowed: true, permit_on_passed: false },
  automatic: [
    { limit: 2, exit_status: "-1", signal_reason: "none" },
    { limit: 3, exit_status: "47", signal_reason: "none" },
    { limit: 2, signal_reason: "agent_stop" },
    { exit_status: "1", limit: 1 },
  ],
};

const snake = (version: string): string => version.replaceAll(".", "_");

export const resolveIntakePlans = async (
  versions: string[],
  current: string,
  store: KnownGoodStore,
  dra: Dra,
): Promise<IntakeVersionPlan[]> => {
  const pins = await resolvePins(versions, current, store, dra);
  return Promise.all(
    pins.map(async (pin) => (pin.current ? pin : { ...pin, latest: await latestBuildOf(dra, pin.branch, pin.version) })),
  );
};

const bwcJob = (version: string, label: string, key: string, pins: Record<string, string>): BuildkiteStep => ({
  label,
  key,
  command: `${GRADLE}${pinArgs(pins)} v${version}#bwcTest`,
  timeout_in_minutes: 300,
  agents: AGENTS,
  env: { BWC_VERSION: version },
  retry: RETRY,
});

const promoteStep = (plan: IntakeVersionPlan, buildId: string, testedStepKey: string): BuildkiteStep => ({
  label: `${plan.version} / promote ${buildId} to known-good`,
  key: `bwc-snapshots-promote-${snake(plan.version)}`,
  depends_on: testedStepKey,
  // A soft-failed dependency would still let this step run, so it checks the outcome itself instead.
  allow_dependency_failure: true,
  command: `node .buildkite/scripts/bwc-snapshots/cli.ts promote --branch ${plan.branch} --build-id ${buildId} --step ${testedStepKey}`,
  timeout_in_minutes: 10,
  env: { USE_BWC_KNOWN_GOOD_CREDENTIALS: "true" },
});

/** Steps for one version; see the comment at the top of this file. */
export const intakeVersionSteps = (plan: IntakeVersionPlan, allPins: Record<string, string>): BuildkiteStep[] => {
  const key = `bwc-snapshots-${snake(plan.version)}`;
  const label = `${plan.version} / bwc-snapshots`;
  const knownGood = plan.knownGood?.build_id;
  const latest = plan.latest?.buildId;

  if (!knownGood) {
    if (!latest) {
      // Nothing in DRA for this version yet: test the moving tip as before.
      return [bwcJob(plan.version, label, key, allPins)];
    }
    // No known-good build yet: test the newest one, and promote it when it passes.
    return [
      bwcJob(plan.version, `${label} (${latest})`, key, { ...allPins, [plan.branch]: latest }),
      promoteStep(plan, latest, key),
    ];
  }

  const steps = [bwcJob(plan.version, `${label} (known-good ${knownGood})`, key, allPins)];
  if (latest && latest !== knownGood) {
    const candidateKey = `bwc-snapshots-candidate-${snake(plan.version)}`;
    steps.push(
      {
        ...bwcJob(plan.version, `${label} candidate ${latest}`, candidateKey, { ...allPins, [plan.branch]: latest }),
        soft_fail: true,
      },
      promoteStep(plan, latest, candidateKey),
    );
  }
  return steps;
};

export const intakeBwcGroup = (plans: IntakeVersionPlan[]): BuildkiteStep => {
  const allPins = knownGoodBuildIds(plans);
  return { group: "bwc-snapshots", steps: plans.flatMap((plan) => intakeVersionSteps(plan, allPins)) };
};

/** The group as it was before known-good pinning, used when the store is disabled or unreachable. */
export const unpinnedIntakeBwcGroup = (versions: string[]): BuildkiteStep =>
  intakeBwcGroup(versions.map((version) => ({ version, branch: branchForVersion(version) })));
