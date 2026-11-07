// Known-good DRA snapshot builds for bwc testing.
//
// bwc tests against unreleased versions download DRA snapshot builds of the release branches. Testing the moving
// tip of each branch turns every PR red whenever a branch (or its snapshot) breaks, so bwc jobs pin each branch to
// the last DRA build that a bwc run actually consumed and passed with ("known good"). Intake promotes a newer build
// once a candidate run on it passes. Each branch's record is a text object holding just the build ID
// (e.g. "9.4.10-3710ab3e") in a publicly readable GCS bucket:
//
//   gs://$BWC_KNOWN_GOOD_BUCKET/$BWC_KNOWN_GOOD_PREFIX/<branch>.txt
//
// PR and merge-queue bwc jobs run with -Dtests.bwc.mode=dra, in which Gradle reads these objects itself and falls back
// to the latest DRA build of a branch without a usable record. This module promotes builds and leaves versions without
// a known-good build out of those jobs. With BWC_KNOWN_GOOD_BUCKET unset the generator does no gating.

import { readFileSync } from "fs";
import { resolve } from "path";

import { type Dra, type DraBuild, buildHash, buildVersion } from "./dra.ts";
import { Bucket, type BucketWriter, bucketFromEnv } from "./gcs.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../..`);

export class KnownGoodStore {
  /** Public URL of the records, for logs */
  readonly location: string;
  private readonly bucket: Bucket;
  private readonly prefix: string;

  constructor(bucket: Bucket, prefix: string) {
    this.bucket = bucket;
    this.prefix = prefix;
    this.location = `${bucket.url}/${prefix}`;
  }

  /** The branch's known-good build ID, or undefined when it has none. */
  async read(branch: string): Promise<string | undefined> {
    return (await this.bucket.read(this.objectName(branch)))?.body.trim() || undefined;
  }

  /**
   * Read-modify-write of one branch's build ID. `change` returns the new build ID, or undefined to leave it as is.
   * Returns the build ID the branch ends up with.
   */
  async update(
    branch: string,
    change: (current: string | undefined) => Promise<string | undefined>,
  ): Promise<string | undefined> {
    const body = await this.bucket.update(this.objectName(branch), async (current) => {
      const next = await change(current?.trim() || undefined);
      return next === undefined ? undefined : `${next}\n`;
    });
    return body?.trim() || undefined;
  }

  private objectName(branch: string): string {
    return `${this.prefix}/${branch}.txt`;
  }
}

/** Builds the store from the environment (see bucketFromEnv), or returns undefined when BWC_KNOWN_GOOD_BUCKET is unset. */
export const storeFromEnv = (env: NodeJS.ProcessEnv = process.env, writer?: BucketWriter): KnownGoodStore | undefined => {
  const configured = bucketFromEnv(env, writer);
  return configured && new KnownGoodStore(configured.bucket, configured.prefix);
};

/** Release branch of an unreleased version: 9.4.10 => 9.4 */
export const branchForVersion = (version: string): string => version.split(".").slice(0, 2).join(".");

/** The version this checkout builds (never pinned: it is built locally, not downloaded). */
export const currentVersion = (projectRoot: string = PROJECT_ROOT): string => {
  const properties = readFileSync(`${projectRoot}/build-tools-internal/version.properties`, "utf-8");
  const match = properties.match(/^elasticsearch\s*=\s*(\S+)/m);
  if (!match?.[1]) {
    throw new Error("could not read the elasticsearch version from version.properties");
  }
  return match[1].replace(/-SNAPSHOT$/, "");
};

export const compareVersions = (a: string, b: string): number => {
  const pa = a.split(".").map(Number);
  const pb = b.split(".").map(Number);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const diff = (pa[i] ?? 0) - (pb[i] ?? 0);
    if (diff !== 0) {
      return diff;
    }
  }
  return 0;
};

export type VersionPin = {
  version: string;
  branch: string;
  /** The version this checkout builds: tested from the local build, never pinned or gated */
  current?: boolean;
  /** The branch's known-good build ID, when it is a build of this version that DRA still has */
  knownGood?: string;
};

/**
 * Looks up the known-good build of every snapshot bwc version's branch. A record only pins a version when it is for
 * that exact version (after a patch release the branch moves to the next version, which needs its own known-good
 * build) and the build has not been removed from DRA.
 */
export const resolvePins = async (
  versions: string[],
  current: string,
  store: KnownGoodStore,
  dra: Dra,
): Promise<VersionPin[]> => {
  return Promise.all(
    versions.map(async (version): Promise<VersionPin> => {
        const branch = branchForVersion(version);
        if (version === current) {
          return { version, branch, current: true };
        }
        const record = await store.read(branch);
        if (!record || buildVersion(record) !== version) {
          return { version, branch };
        }
        if (!(await dra.manifest(record))) {
          console.log(`Known-good build ${record} of ${branch} is no longer available from DRA; not pinning it`);
          return { version, branch };
        }
        return { version, branch, knownGood: record };
    }),
  );
};

/** Gradle arguments pinning each branch to a DRA build, e.g. " -Dtests.bwc.dra.hash.9.4=3710ab3e" (leading space). */
export const pinArgs = (buildIds: Record<string, string>): string =>
  Object.keys(buildIds)
    .sort()
    .map((branch) => ` -Dtests.bwc.dra.hash.${branch}=${buildHash(buildIds[branch] ?? "")}`)
    .join("");

export const knownGoodBuildIds = (pins: VersionPin[]): Record<string, string> =>
  Object.fromEntries(pins.flatMap((pin) => (pin.knownGood ? [[pin.branch, pin.knownGood]] : [])));

/**
 * Gating for PR and merge-queue bwc jobs: the versions to test, which are those with a known-good build plus the
 * current version. A version without one is left out until intake promotes one. Fails open: if the store is disabled
 * or unreachable, returns undefined and every version is tested, rather than silently losing bwc coverage.
 */
export const loadIncludedSnapshotBwcVersions = async (
  versions: string[],
  store: KnownGoodStore | undefined,
  dra: Dra,
  current: string = currentVersion(),
): Promise<string[] | undefined> => {
  if (!store) {
    console.log("BWC_KNOWN_GOOD_BUCKET is not set; every bwc snapshot version is tested");
    return undefined;
  }
  try {
    const pins = await resolvePins(versions, current, store, dra);
    for (const pin of pins.filter((p) => !p.current)) {
      console.log(
        `bwc ${pin.version}: ${pin.knownGood ? `known-good build ${pin.knownGood}` : "no known-good build, left out until one is promoted"}`,
      );
    }
    return pins.filter((pin) => pin.current || pin.knownGood).map((pin) => pin.version);
  } catch (e) {
    console.log(`Could not read known-good bwc builds from ${store.location}, testing every version: ${e}`);
    return undefined;
  }
};

export type PromoteRequest = { branch: string; buildId: string };

export type PromoteResult = { promoted: boolean; knownGood: string | undefined; reason: string };

/**
 * Records `buildId` as the known-good build of its branch. Only call this after a bwc run that consumed exactly this
 * build passed. Never moves a branch backwards: an older version, or an older build of the same version (by DRA start
 * time), is ignored.
 */
export const promote = async (store: KnownGoodStore, dra: Dra, request: PromoteRequest): Promise<PromoteResult> => {
  const { branch, buildId } = request;
  if (branchForVersion(buildVersion(buildId)) !== branch) {
    throw new Error(`build ${buildId} is not a build of branch ${branch}`);
  }
  const manifest = await dra.manifest(buildId);
  if (!manifest) {
    throw new Error(`DRA build ${buildId} does not exist`);
  }
  let reason = "promoted";
  let promoted = false;
  const knownGood = await store.update(branch, async (current) => {
    const keep = current && (await keepCurrent(dra, current, buildId, manifest.startTime));
    reason = keep || "promoted";
    promoted = !keep;
    return keep ? undefined : buildId;
  });
  return { promoted, knownGood, reason };
};

const keepCurrent = async (
  dra: Dra,
  current: string,
  candidate: string,
  candidateStartTime: string | undefined,
): Promise<string | undefined> => {
  if (current === candidate) {
    return `${candidate} is already the known-good build`;
  }
  const versionOrder = compareVersions(buildVersion(candidate), buildVersion(current));
  if (versionOrder < 0) {
    return `known-good ${current} is for a newer version than ${candidate}`;
  }
  if (versionOrder === 0) {
    // A known-good build that DRA no longer has can always be replaced.
    const currentStartTime = (await dra.manifest(current))?.startTime;
    if (currentStartTime && candidateStartTime && Date.parse(candidateStartTime) < Date.parse(currentStartTime)) {
      return `known-good ${current} is newer than ${candidate}`;
    }
  }
  return undefined;
};

/** The newest DRA build of `branch` if it is a build of `version`. */
export const latestBuildOf = async (dra: Dra, branch: string, version: string): Promise<DraBuild | undefined> => {
  const latest = await dra.latest(branch);
  return latest?.version === version ? latest : undefined;
};
