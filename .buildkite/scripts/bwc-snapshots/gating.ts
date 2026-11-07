// New-version gating for bwc snapshot tests.
//
// A version only takes part in PR, merge-queue and (blocking) intake bwc once its branch has a known-good DRA build of
// that version. Until then intake runs it as a soft-failing candidate, which promotes the build once it passes, and
// periodic keeps building it from source. Each intake build tracks how long each branch has been waiting to go green
// (a new version with no known-good build, or a newer DRA build that keeps failing) and raises a Slack alert once
// that passes 48 hours, repeated at most daily, so pinning cannot quietly hide a broken branch.
//
// The clock lives next to the known-good objects:
//   gs://$BWC_KNOWN_GOOD_BUCKET/$BWC_KNOWN_GOOD_PREFIX/pending/<branch>.json

import { Bucket, type BucketWriter, bucketFromEnv } from "./gcs.ts";
import type { IntakeVersionPlan } from "./intake-pipeline.ts";
import type { VersionPin } from "./known-good.ts";

export const ALERT_AFTER_HOURS = 48;
export const REALERT_AFTER_HOURS = 24;
const HOUR = 60 * 60 * 1000;

export type Pending = {
  branch: string;
  version: string;
  /** When intake first saw this version waiting to go green */
  since: string;
  alerted_at?: string;
};

export type NotGreenAlert = {
  version: string;
  branch: string;
  since: string;
  hours: number;
  latestBuildId?: string;
  knownGoodBuildId?: string;
};

export class PendingStore {
  private readonly bucket: Bucket;
  private readonly prefix: string;

  constructor(bucket: Bucket, prefix: string) {
    this.bucket = bucket;
    this.prefix = `${prefix}/pending`;
  }

  async read(branch: string): Promise<Pending | undefined> {
    const object = await this.bucket.read(this.objectName(branch));
    return object ? JSON.parse(object.body) : undefined;
  }

  /** Read-modify-write; `change` returns the new record, null to delete it, or undefined to leave it as is. */
  async update(branch: string, change: (current: Pending | undefined) => Pending | null | undefined): Promise<void> {
    await this.bucket.update(this.objectName(branch), (current) => {
      const next = change(current ? JSON.parse(current) : undefined);
      return next ? JSON.stringify(next, null, 2) + "\n" : next;
    });
  }

  private objectName(branch: string): string {
    return `${this.prefix}/${branch}.json`;
  }
}

export const pendingStoreFromEnv = (env: NodeJS.ProcessEnv = process.env, writer?: BucketWriter): PendingStore | undefined => {
  const configured = bucketFromEnv(env, writer);
  return configured && new PendingStore(configured.bucket, configured.prefix);
};

/** Versions that take part in PR, merge-queue and blocking intake bwc. */
export const includedVersions = (pins: VersionPin[]): string[] =>
  pins.filter((pin) => pin.current || pin.knownGood).map((pin) => pin.version);

/** True while a branch has no known-good build of this version, or a newer DRA build that has not passed yet. */
export const waitingToGoGreen = (plan: IntakeVersionPlan): boolean => {
  if (plan.current) {
    return false;
  }
  if (!plan.knownGood) {
    return true;
  }
  return !!plan.latest && plan.latest.buildId !== plan.knownGood;
};

/**
 * Starts, keeps or clears the branch's clock and returns an alert when it has been waiting for 48 hours or more and
 * was not alerted on in the last 24 hours. Only intake calls this, with write credentials.
 */
export const trackPending = async (
  store: PendingStore,
  plan: IntakeVersionPlan,
  now: Date = new Date(),
): Promise<NotGreenAlert | undefined> => {
  if (plan.current) {
    return undefined;
  }
  const waiting = waitingToGoGreen(plan);
  let alert: NotGreenAlert | undefined;
  await store.update(plan.branch, (current) => {
    alert = undefined;
    if (!waiting) {
      return current ? null : undefined;
    }
    if (!current || current.version !== plan.version) {
      return { branch: plan.branch, version: plan.version, since: now.toISOString() };
    }
    const hours = Math.floor((now.getTime() - Date.parse(current.since)) / HOUR);
    const sinceAlert = current.alerted_at ? (now.getTime() - Date.parse(current.alerted_at)) / HOUR : Infinity;
    if (hours < ALERT_AFTER_HOURS || sinceAlert < REALERT_AFTER_HOURS) {
      return undefined;
    }
    alert = {
      version: plan.version,
      branch: plan.branch,
      since: current.since,
      hours,
      latestBuildId: plan.latest?.buildId,
      knownGoodBuildId: plan.knownGood,
    };
    return { ...current, alerted_at: now.toISOString() };
  });
  return alert;
};
