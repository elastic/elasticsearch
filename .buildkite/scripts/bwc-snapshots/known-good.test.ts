import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { generateKeyPairSync } from "crypto";
import { mkdtempSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

import { DraClient } from "./dra.ts";
import { type FakeDra, type FakeGcs, addDraBuild, startFakeDra, startFakeGcs } from "./fake-servers.ts";
import { type KnownGoodStore, loadPinArgs, promote, resolvePins, storeFromEnv } from "./known-good.ts";

const BUCKET = "bwc-known-good-test";
const PREFIX = "elasticsearch/bwc-known-good";

describe("known-good store", () => {
  let gcs: FakeGcs;
  let fakeDra: FakeDra;
  let dra: DraClient;
  let store: KnownGoodStore;

  beforeEach(async () => {
    // Authenticated like CI: a service account key file exchanged for a token.
    const { publicKey, privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    gcs = await startFakeGcs({ publicKeyPem: publicKey.export({ type: "spki", format: "pem" }).toString() });
    const keyFile = join(mkdtempSync(join(tmpdir(), "bwc-known-good-")), "key.json");
    writeFileSync(
      keyFile,
      JSON.stringify({
        client_email: "bwc-known-good@test.iam.gserviceaccount.com",
        private_key: privateKey.export({ type: "pkcs8", format: "pem" }).toString(),
        token_uri: `${gcs.url}/token`,
      }),
    );
    const fromEnv = storeFromEnv({
      BWC_KNOWN_GOOD_BUCKET: BUCKET,
      BWC_KNOWN_GOOD_GCS_URL: gcs.url,
      BWC_KNOWN_GOOD_CREDENTIALS: keyFile,
    });
    if (!fromEnv) {
      throw new Error("store should be enabled");
    }
    store = fromEnv;
    fakeDra = await startFakeDra();
    dra = new DraClient(fakeDra.url);
  });

  afterEach(async () => {
    await gcs.close();
    await fakeDra.close();
  });

  const stored = (branch: string): Record<string, unknown> | undefined => {
    const object = gcs.objects.get(`${BUCKET}/${PREFIX}/${branch}.json`);
    return object ? JSON.parse(object.body) : undefined;
  };

  test("is disabled without a bucket", () => {
    expect(storeFromEnv({})).toBeUndefined();
  });

  test("promotes a build into one object per branch", async () => {
    addDraBuild(fakeDra, "9.4.10-aaaa1111", "Mon, 02 Nov 2026 10:00:00 GMT");

    const result = await promote(store, dra, {
      branch: "9.4",
      buildId: "9.4.10-aaaa1111",
      now: new Date("2026-11-02T12:00:00Z"),
      sourceBuildUrl: "https://buildkite.com/elastic/elasticsearch-intake/builds/1",
      testedBranch: "main",
      testedCommit: "c0ffee",
    });

    expect(result.promoted).toBe(true);
    expect(stored("9.4")).toEqual({
      branch: "9.4",
      version: "9.4.10",
      build_id: "9.4.10-aaaa1111",
      commit: "commit-of-9.4.10-aaaa1111",
      dra_start_time: "Mon, 02 Nov 2026 10:00:00 GMT",
      promoted_at: "2026-11-02T12:00:00.000Z",
      source_build_url: "https://buildkite.com/elastic/elasticsearch-intake/builds/1",
      tested_branch: "main",
      tested_commit: "c0ffee",
    });
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
    expect(stored("9.4")?.["build_id"]).toBe("9.4.11-cccc3333");
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

    let interfered = false;
    const racingStore: KnownGoodStore = {
      location: store.location,
      read: (branch) => store.read(branch),
      update: (branch, change) =>
        store.update(branch, (current) => {
          if (!interfered) {
            // Another writer bumps the generation between our read and our write.
            interfered = true;
            const key = `${BUCKET}/${PREFIX}/9.4.json`;
            const object = gcs.objects.get(key);
            if (object) {
              gcs.objects.set(key, { ...object, generation: object.generation + 1 });
            }
          }
          return change(current);
        }),
    };

    expect((await promote(racingStore, dra, { branch: "9.4", buildId: "9.4.10-bbbb2222" })).promoted).toBe(true);
    expect(gcs.preconditionFailures).toBe(1);
    expect(stored("9.4")?.["build_id"]).toBe("9.4.10-bbbb2222");
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

    expect(pins.map((p) => [p.version, p.knownGood?.build_id, p.current])).toEqual([
      ["8.19.25", undefined, undefined],
      ["9.4.10", "9.4.10-aaaa1111", undefined],
      ["9.5.7", undefined, undefined],
      ["9.6.0", undefined, undefined],
      ["9.7.0", undefined, true],
    ]);
  });

  test("builds gradle pin arguments for PR jobs", async () => {
    addDraBuild(fakeDra, "9.4.10-aaaa1111", "Mon, 02 Nov 2026 10:00:00 GMT");
    addDraBuild(fakeDra, "8.19.25-cccc3333", "Mon, 02 Nov 2026 10:00:00 GMT");
    await promote(store, dra, { branch: "9.4", buildId: "9.4.10-aaaa1111" });
    await promote(store, dra, { branch: "8.19", buildId: "8.19.25-cccc3333" });

    expect(await loadPinArgs(["8.19.25", "9.4.10", "9.6.0", "9.7.0"], store, dra, "9.7.0")).toBe(
      " -Dtests.bwc.dra.hash.8.19=cccc3333 -Dtests.bwc.dra.hash.9.4=aaaa1111",
    );
  });

  test("PR pins fail open when the store is disabled or unreachable", async () => {
    expect(await loadPinArgs(["9.4.10"], undefined, dra, "9.7.0")).toBe("");

    await gcs.close();
    expect(await loadPinArgs(["9.4.10"], store, dra, "9.7.0")).toBe("");
    gcs = await startFakeGcs();
  });
});
