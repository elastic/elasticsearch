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

import { GcsClient, PreconditionFailedError } from "./gcs.ts";
import type { IntakeVersionPlan } from "./intake-pipeline.ts";
import { type VersionPin, gcsFromEnv } from "./known-good.ts";

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

export interface PendingStore {
  read(branch: string): Promise<Pending | undefined>;
  /** Read-modify-write; `change` returns the new record, null to delete it, or undefined to leave it as is. */
  update(branch: string, change: (current: Pending | undefined) => Pending | null | undefined): Promise<void>;
}

export class GcsPendingStore implements PendingStore {
  private readonly gcs: GcsClient;
  private readonly prefix: string;

  constructor(gcs: GcsClient, prefix: string) {
    this.gcs = gcs;
    this.prefix = `${prefix}/pending`;
  }

  async read(branch: string): Promise<Pending | undefined> {
    const object = await this.gcs.read(`${this.prefix}/${branch}.json`);
    return object ? JSON.parse(object.body) : undefined;
  }

  async update(branch: string, change: (current: Pending | undefined) => Pending | null | undefined): Promise<void> {
    const name = `${this.prefix}/${branch}.json`;
    for (let attempt = 1; ; attempt++) {
      const object = await this.gcs.read(name);
      const next = change(object ? JSON.parse(object.body) : undefined);
      try {
        if (next === null) {
          if (object) {
            await this.gcs.delete(name, object.generation);
          }
        } else if (next) {
          await this.gcs.write(name, JSON.stringify(next, null, 2) + "\n", object?.generation ?? "0");
        }
        return;
      } catch (e) {
        if (!(e instanceof PreconditionFailedError) || attempt >= 3) {
          throw e;
        }
      }
    }
  }
}

export const pendingStoreFromEnv = (env: NodeJS.ProcessEnv = process.env): PendingStore | undefined => {
  const gcs = gcsFromEnv(env);
  return gcs && new GcsPendingStore(gcs.client, gcs.prefix);
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
  return !!plan.latest && plan.latest.buildId !== plan.knownGood.build_id;
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
      knownGoodBuildId: plan.knownGood?.build_id,
    };
    return { ...current, alerted_at: now.toISOString() };
  });
  return alert;
};
