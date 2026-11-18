import { afterEach, beforeEach, describe, expect, test } from "vitest";

import { type FakeBucket, startFakeBucket } from "./fake-servers.ts";
import { PendingStore, includedVersions, trackPending } from "./gating.ts";
import type { IntakeVersionPlan } from "./intake-pipeline.ts";

const HOUR = 60 * 60 * 1000;
const T0 = new Date("2026-11-02T00:00:00Z");
const at = (hours: number): Date => new Date(T0.getTime() + hours * HOUR);

// A known-good record is just the build ID.
const knownGood = (buildId: string): string => buildId;
const latest = (buildId: string) => ({ buildId, version: buildId.substring(0, buildId.lastIndexOf("-")) });

describe("new-version gating", () => {
  test("PR, merge-queue and intake bwc only include versions with a known-good build, and the current version", () => {
    expect(
      includedVersions([
        { version: "8.19.25", branch: "8.19", knownGood: knownGood("8.19.25-aaaa1111") },
        { version: "9.6.0", branch: "9.6" },
        { version: "9.7.0", branch: "9.7", current: true },
      ]),
    ).toEqual(["8.19.25", "9.7.0"]);
  });

  describe("pending tracking and the 48 hour alert", () => {
    let bucket: FakeBucket;
    let store: PendingStore;

    beforeEach(async () => {
      bucket = await startFakeBucket();
      store = new PendingStore(bucket.bucket, "elasticsearch/bwc-known-good");
    });

    afterEach(async () => {
      await bucket.close();
    });

    const newVersion: IntakeVersionPlan = { version: "9.7.0", branch: "9.7", latest: latest("9.7.0-dddd4444") };

    test("a new version is not alerted on before 48 hours, then once a day until it goes green", async () => {
      expect(await trackPending(store, newVersion, at(0))).toBeUndefined();
      expect(await trackPending(store, { ...newVersion, latest: latest("9.7.0-eeee5555") }, at(30))).toBeUndefined();

      const alert = await trackPending(store, newVersion, at(49));
      expect(alert).toEqual({
        version: "9.7.0",
        branch: "9.7",
        since: T0.toISOString(),
        hours: 49,
        latestBuildId: "9.7.0-dddd4444",
        knownGoodBuildId: undefined,
      });

      expect(await trackPending(store, newVersion, at(60))).toBeUndefined();
      expect((await trackPending(store, newVersion, at(73)))?.hours).toBe(73);
    });

    test("going green clears the clock", async () => {
      await trackPending(store, newVersion, at(0));
      const green: IntakeVersionPlan = {
        ...newVersion,
        knownGood: knownGood("9.7.0-dddd4444"),
        latest: latest("9.7.0-dddd4444"),
      };
      expect(await trackPending(store, green, at(10))).toBeUndefined();
      expect(await store.read("9.7")).toBeUndefined();

      // A later failing build starts a new clock.
      const stale: IntakeVersionPlan = { ...green, latest: latest("9.7.0-ffff6666") };
      expect(await trackPending(store, stale, at(20))).toBeUndefined();
      expect(await trackPending(store, stale, at(67))).toBeUndefined();
      expect((await trackPending(store, stale, at(69)))?.knownGoodBuildId).toBe("9.7.0-dddd4444");
    });

    test("a version bump on the branch restarts the clock", async () => {
      await trackPending(store, { version: "9.4.10", branch: "9.4", latest: latest("9.4.10-aaaa1111") }, at(0));
      const bumped: IntakeVersionPlan = { version: "9.4.11", branch: "9.4", latest: latest("9.4.11-bbbb2222") };
      expect(await trackPending(store, bumped, at(50))).toBeUndefined();
      expect((await trackPending(store, bumped, at(99)))?.since).toBe(at(50).toISOString());
    });

    test("the current version is never tracked", async () => {
      expect(await trackPending(store, { version: "9.8.0", branch: "9.8", current: true }, at(100))).toBeUndefined();
      expect(await store.read("9.8")).toBeUndefined();
    });
  });
});
