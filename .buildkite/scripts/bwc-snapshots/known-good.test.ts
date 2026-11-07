import { afterEach, beforeEach, describe, expect, test } from "vitest";

import { DraClient } from "./dra.ts";
import { type FakeBucket, type FakeDra, addDraBuild, startFakeBucket, startFakeDra } from "./fake-servers.ts";
import { KnownGoodStore, loadIncludedSnapshotBwcVersions, promote, resolvePins, storeFromEnv } from "./known-good.ts";

const PREFIX = "elasticsearch/bwc-known-good";

describe("known-good store", () => {
  let bucket: FakeBucket;
  let fakeDra: FakeDra;
  let dra: DraClient;
  let store: KnownGoodStore;

  beforeEach(async () => {
    bucket = await startFakeBucket();
    store = new KnownGoodStore(bucket.bucket, PREFIX);
    fakeDra = await startFakeDra();
    dra = new DraClient(fakeDra.url);
  });

  afterEach(async () => {
    await bucket.close();
    await fakeDra.close();
  });

  const stored = (branch: string): string | undefined => bucket.objects.get(`${PREFIX}/${branch}.txt`)?.body;

  test("is disabled without a bucket", () => {
    expect(storeFromEnv({})).toBeUndefined();
  });

  test("reads the public object URL of the bucket", () => {
    expect(storeFromEnv({ BWC_KNOWN_GOOD_BUCKET: "some-bucket" })?.location).toBe(
      "https://storage.googleapis.com/some-bucket/elasticsearch/bwc-known-good",
    );
  });

  test("promotes a build into one text object per branch holding the build ID", async () => {
    addDraBuild(fakeDra, "9.4.10-aaaa1111", "Mon, 02 Nov 2026 10:00:00 GMT");

    const result = await promote(store, dra, { branch: "9.4", buildId: "9.4.10-aaaa1111" });

    expect(result.promoted).toBe(true);
    expect(stored("9.4")).toBe("9.4.10-aaaa1111\n");
    expect(await store.read("9.4")).toBe("9.4.10-aaaa1111");
  });

  test("never moves a branch back to an older build or version", async () => {
    addDraBuild(fakeDra, "9.4.10-aaaa1111", "Mon, 02 Nov 2026 10:00:00 GMT");
    addDraBuild(fakeDra, "9.4.10-bbbb2222", "Tue, 03 Nov 2026 10:00:00 GMT");
    addDraBuild(fakeDra, "9.4.11-cccc3333", "Wed, 04 Nov 2026 10:00:00 GMT");

    expect((await promote(store, dra, { branch: "9.4", buildId: "9.4.10-bbbb2222" })).promoted).toBe(true);
    expect((await promote(store, dra, { branch: "9.4", buildId: "9.4.10-aaaa1111" })).promoted).toBe(false);
    expect((await promote(store, dra, { branch: "9.4", buildId: "9.4.10-bbbb2222" })).promoted).toBe(false);
    expect((await promote(store, dra, { branch: "9.4", buildId: "9.4.11-cccc3333" })).promoted).toBe(true);
    expect((await promote(store, dra, { branch: "9.4", buildId: "9.4.10-bbbb2222" })).promoted).toBe(false);
    expect(stored("9.4")).toBe("9.4.11-cccc3333\n");
  });

  test("replaces a known-good build that DRA no longer has", async () => {
    addDraBuild(fakeDra, "9.4.10-bbbb2222", "Tue, 03 Nov 2026 10:00:00 GMT");
    addDraBuild(fakeDra, "9.4.10-aaaa1111", "Mon, 02 Nov 2026 10:00:00 GMT");
    await promote(store, dra, { branch: "9.4", buildId: "9.4.10-bbbb2222" });
    fakeDra.builds.delete("9.4.10-bbbb2222");

    expect((await promote(store, dra, { branch: "9.4", buildId: "9.4.10-aaaa1111" })).promoted).toBe(true);
  });

  test("refuses builds that do not exist or belong to another branch", async () => {
    addDraBuild(fakeDra, "9.5.7-aaaa1111", "Mon, 02 Nov 2026 10:00:00 GMT");

    await expect(promote(store, dra, { branch: "9.4", buildId: "9.4.10-missing" })).rejects.toThrow("does not exist");
    await expect(promote(store, dra, { branch: "9.4", buildId: "9.5.7-aaaa1111" })).rejects.toThrow("not a build of branch");
    expect(stored("9.4")).toBeUndefined();
  });

  test("retries when another intake build promoted concurrently", async () => {
    addDraBuild(fakeDra, "9.4.10-aaaa1111", "Mon, 02 Nov 2026 10:00:00 GMT");
    addDraBuild(fakeDra, "9.4.10-bbbb2222", "Tue, 03 Nov 2026 10:00:00 GMT");
    await promote(store, dra, { branch: "9.4", buildId: "9.4.10-aaaa1111" });
    // Another writer bumps the generation between our read and our first write.
    bucket.interfereOnce(`${PREFIX}/9.4.txt`);

    expect((await promote(store, dra, { branch: "9.4", buildId: "9.4.10-bbbb2222" })).promoted).toBe(true);
    expect(bucket.preconditionFailures).toBe(1);
    expect(stored("9.4")).toBe("9.4.10-bbbb2222\n");
  });

  test("pins a version only to a known-good build of that exact version that DRA still has", async () => {
    addDraBuild(fakeDra, "9.4.10-aaaa1111", "Mon, 02 Nov 2026 10:00:00 GMT");
    addDraBuild(fakeDra, "9.5.6-bbbb2222", "Mon, 02 Nov 2026 10:00:00 GMT");
    addDraBuild(fakeDra, "8.19.25-cccc3333", "Mon, 02 Nov 2026 10:00:00 GMT");
    await promote(store, dra, { branch: "9.4", buildId: "9.4.10-aaaa1111" });
    // 9.5 has since moved to 9.5.7: its known-good build is for the previous patch version.
    await promote(store, dra, { branch: "9.5", buildId: "9.5.6-bbbb2222" });
    await promote(store, dra, { branch: "8.19", buildId: "8.19.25-cccc3333" });
    // DRA retention removed the 8.19 build.
    fakeDra.builds.delete("8.19.25-cccc3333");

    const pins = await resolvePins(["8.19.25", "9.4.10", "9.5.7", "9.6.0", "9.7.0"], "9.7.0", store, dra);

    expect(pins.map((p) => [p.version, p.knownGood, p.current])).toEqual([
      ["8.19.25", undefined, undefined],
      ["9.4.10", "9.4.10-aaaa1111", undefined],
      ["9.5.7", undefined, undefined],
      ["9.6.0", undefined, undefined],
      ["9.7.0", undefined, true],
    ]);
  });

  test("leaves versions without a known-good build out of PR jobs", async () => {
    addDraBuild(fakeDra, "9.4.10-aaaa1111", "Mon, 02 Nov 2026 10:00:00 GMT");
    addDraBuild(fakeDra, "8.19.25-cccc3333", "Mon, 02 Nov 2026 10:00:00 GMT");
    await promote(store, dra, { branch: "9.4", buildId: "9.4.10-aaaa1111" });
    await promote(store, dra, { branch: "8.19", buildId: "8.19.25-cccc3333" });

    expect(await loadIncludedSnapshotBwcVersions(["8.19.25", "9.4.10", "9.6.0", "9.7.0"], store, dra, "9.7.0")).toEqual([
      "8.19.25",
      "9.4.10",
      "9.7.0",
    ]);
  });

  test("PR gating fails open when the store is disabled or unreachable", async () => {
    expect(await loadIncludedSnapshotBwcVersions(["9.4.10"], undefined, dra, "9.7.0")).toBeUndefined();

    await bucket.close();
    expect(await loadIncludedSnapshotBwcVersions(["9.4.10"], store, dra, "9.7.0")).toBeUndefined();
    bucket = await startFakeBucket();
  });
});
