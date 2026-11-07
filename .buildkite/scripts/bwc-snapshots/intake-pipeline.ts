// Generates intake's bwc-snapshots group.
//
// Every snapshot bwc version with a known-good DRA build gets one blocking job pinned to that build, so a broken
// branch tip does not turn intake red. When the newest DRA build of the branch differs from the known-good one, a
// candidate job tests the newest build (soft-failing: the branch, not this commit, is at fault if it fails) and a
// promote step records it as known good once the candidate passed. A version without a known-good build (newly added,
// or bumped after a release) only gets the candidate until it goes green; see gating.ts.

import type { BuildkiteStep } from "../pull-request/types.ts";
import type { Dra, DraBuild } from "./dra.ts";
import type { NotGreenAlert } from "./gating.ts";
import {
  type KnownGoodStore,
  type VersionPin,
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

const candidateJob = (
  plan: IntakeVersionPlan,
  buildId: string,
  allPins: Record<string, string>,
  labelSuffix = "",
): BuildkiteStep => ({
  ...bwcJob(
    plan.version,
    `${plan.version} / bwc-snapshots candidate ${buildId}${labelSuffix}`,
    `bwc-snapshots-candidate-${snake(plan.version)}`,
    { ...allPins, [plan.branch]: buildId },
  ),
  soft_fail: true,
});

/** Steps for one version; see the comment at the top of this file. */
export const intakeVersionSteps = (plan: IntakeVersionPlan, allPins: Record<string, string>): BuildkiteStep[] => {
  const key = `bwc-snapshots-${snake(plan.version)}`;
  const label = `${plan.version} / bwc-snapshots`;
  const knownGood = plan.knownGood;
  const latest = plan.latest?.buildId;

  if (plan.current) {
    // Built locally from this commit, nothing to pin.
    return [bwcJob(plan.version, label, key, allPins)];
  }

  if (!knownGood) {
    if (!latest) {
      // Nothing in DRA to test or promote yet; periodic source builds cover the branch meanwhile.
      return [];
    }
    const candidate = candidateJob(plan, latest, allPins, " (not gating: no known-good build yet)");
    return [candidate, promoteStep(plan, latest, String(candidate.key))];
  }

  const steps = [bwcJob(plan.version, `${label} (known-good ${knownGood})`, key, allPins)];
  if (latest && latest !== knownGood) {
    const candidate = candidateJob(plan, latest, allPins);
    steps.push(candidate, promoteStep(plan, latest, String(candidate.key)));
  }
  return steps;
};

export type SlackTarget = { channel: string; mention: string };

/**
 * Where not-green alerts go. The plan asks for a mention of @brian.seeders (Slack user UL69YSF54) until the
 * alerts have an owning team; BWC_ALERT_SLACK_CHANNEL and BWC_ALERT_SLACK_MENTION override both.
 */
export const slackTargetFromEnv = (env: NodeJS.ProcessEnv = process.env): SlackTarget => ({
  channel: env["BWC_ALERT_SLACK_CHANNEL"] || "#es-core-infra-alerts",
  mention: env["BWC_ALERT_SLACK_MENTION"] || "<@UL69YSF54>",
});

const alertMessage = (alert: NotGreenAlert): string =>
  alert.knownGoodBuildId
    ? `bwc ${alert.version} has had no passing bwc run on a newer DRA build since ${alert.since} (${alert.hours}h), ` +
      `so PR, merge-queue and intake bwc are still pinned to ${alert.knownGoodBuildId}. Newest DRA build: ${alert.latestBuildId}.`
    : `bwc ${alert.version} has had no passing bwc run since ${alert.since} (${alert.hours}h), ` +
      `so it is left out of PR, merge-queue and intake bwc. Newest DRA build: ${alert.latestBuildId ?? "none"}.`;

/** A soft-failing step whose Slack notification carries the alert. */
export const alertStep = (alert: NotGreenAlert, slack: SlackTarget): BuildkiteStep => {
  const message = alertMessage(alert);
  return {
    label: `:rotating_light: ${alert.version} bwc not green for ${alert.hours}h`,
    key: `bwc-snapshots-alert-${snake(alert.version)}`,
    command: `buildkite-agent annotate --style error --context bwc-not-green-${snake(alert.version)} '${message.replaceAll("'", "")}' && exit 1`,
    soft_fail: true,
    timeout_in_minutes: 5,
    notify: [{ slack: { channels: [slack.channel], message: `${slack.mention} ${message} \${BUILDKITE_BUILD_URL}` } }],
  };
};

export const intakeBwcGroup = (
  plans: IntakeVersionPlan[],
  alerts: NotGreenAlert[] = [],
  slack: SlackTarget = slackTargetFromEnv(),
): BuildkiteStep => {
  const allPins = knownGoodBuildIds(plans);
  return {
    group: "bwc-snapshots",
    steps: [
      ...plans.flatMap((plan) => intakeVersionSteps(plan, allPins)),
      ...alerts.map((alert) => alertStep(alert, slack)),
    ],
  };
};

/** The group as it was before known-good pinning, used when the store is disabled or unreachable. */
export const unpinnedIntakeBwcGroup = (versions: string[]): BuildkiteStep => ({
  group: "bwc-snapshots",
  steps: versions.map((version) => bwcJob(version, `${version} / bwc-snapshots`, `bwc-snapshots-${snake(version)}`, {})),
});
