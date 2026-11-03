// Known-good DRA snapshot builds for bwc testing.
//
// bwc tests against unreleased versions download DRA snapshot builds of the release branches. Testing the moving
// tip of each branch turns every PR red whenever a branch (or its snapshot) breaks, so bwc jobs pin each branch to
// the last DRA build that a bwc run actually consumed and passed with ("known good"). Intake promotes a newer build
// once a candidate run on it passes. One JSON object per branch lives in a GCS bucket:
//
//   gs://$BWC_KNOWN_GOOD_BUCKET/$BWC_KNOWN_GOOD_PREFIX/<branch>.json
//
// With BWC_KNOWN_GOOD_BUCKET unset the store is disabled and bwc jobs behave as before (moving tip, -Dtests.bwc.mode=auto).

import { readFileSync } from "fs";
import { resolve } from "path";

import { type Dra, type DraBuild, buildHash, buildVersion } from "./dra.ts";
import { GcsClient, PreconditionFailedError, serviceAccountToken } from "./gcs.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../..`);

export type KnownGood = {
  branch: string;
  version: string;
  /** DRA build ID, e.g. 9.4.10-3710ab3e */
  build_id: string;
  /** elasticsearch commit the DRA build was made from */
  commit?: string;
  /** DRA manifest start_time, used to never promote an older build over a newer one */
  dra_start_time?: string;
  promoted_at: string;
  /** Buildkite build whose bwc run passed on this DRA build */
  source_build_url?: string;
  /** branch and commit of the build that ran the bwc tests */
  tested_branch?: string;
  tested_commit?: string;
};

export interface KnownGoodStore {
  /** A short description for logs, e.g. gs://bucket/prefix */
  readonly location: string;
  read(branch: string): Promise<KnownGood | undefined>;
  /**
   * Read-modify-write of one branch's record. `change` returns the new record, or undefined to leave it as is.
   * Retries when another writer changed the record in between.
   */
  update(branch: string, change: (current: KnownGood | undefined) => KnownGood | undefined): Promise<KnownGood | undefined>;
}

export class GcsKnownGoodStore implements KnownGoodStore {
  readonly location: string;
  private readonly gcs: GcsClient;
  private readonly prefix: string;

  constructor(gcs: GcsClient, bucket: string, prefix: string) {
    this.gcs = gcs;
    this.prefix = prefix.replace(/\/$/, "");
    this.location = `gs://${bucket}/${this.prefix}`;
  }

  async read(branch: string): Promise<KnownGood | undefined> {
    const object = await this.gcs.read(this.objectName(branch));
    return object ? JSON.parse(object.body) : undefined;
  }

  async update(
    branch: string,
    change: (current: KnownGood | undefined) => KnownGood | undefined,
  ): Promise<KnownGood | undefined> {
    for (let attempt = 1; ; attempt++) {
      const object = await this.gcs.read(this.objectName(branch));
      const current: KnownGood | undefined = object ? JSON.parse(object.body) : undefined;
      const next = change(current);
      if (!next) {
        return current;
      }
      try {
        await this.gcs.write(this.objectName(branch), JSON.stringify(next, null, 2) + "\n", object?.generation ?? "0");
        return next;
      } catch (e) {
        if (!(e instanceof PreconditionFailedError) || attempt >= 3) {
          throw e;
        }
        console.log(`${e.message}; retrying`);
      }
    }
  }

  private objectName(branch: string): string {
    return `${this.prefix}/${branch}.json`;
  }
}

/**
 * Builds the store from the environment, or returns undefined when BWC_KNOWN_GOOD_BUCKET is unset.
 *
 * - BWC_KNOWN_GOOD_BUCKET: bucket name; unset disables pinning everywhere
 * - BWC_KNOWN_GOOD_PREFIX: object prefix, default elasticsearch/bwc-known-good
 * - BWC_KNOWN_GOOD_CREDENTIALS: service account key file (written by the pre-command hook); unset means anonymous
 *   access, which works for reads from a publicly readable bucket
 * - BWC_KNOWN_GOOD_GCS_URL: API base URL, for tests and dry-runs against a fake GCS server
 */
export const storeFromEnv = (env: NodeJS.ProcessEnv = process.env): KnownGoodStore | undefined => {
  const bucket = env["BWC_KNOWN_GOOD_BUCKET"];
  if (!bucket) {
    return undefined;
  }
  const credentials = env["BWC_KNOWN_GOOD_CREDENTIALS"];
  const gcs = new GcsClient({
    bucket,
    baseUrl: env["BWC_KNOWN_GOOD_GCS_URL"],
    token: credentials
      ? serviceAccountToken(credentials, "https://www.googleapis.com/auth/devstorage.read_write")
      : undefined,
  });
  return new GcsKnownGoodStore(gcs, bucket, env["BWC_KNOWN_GOOD_PREFIX"] ?? "elasticsearch/bwc-known-good");
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
  /** Present when the branch's known-good build is for this version and still exists in DRA */
  knownGood?: KnownGood;
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
        if (!record || record.version !== version || buildVersion(record.build_id) !== version) {
          return { version, branch };
        }
        if (!(await dra.manifest(record.build_id))) {
          console.log(`Known-good build ${record.build_id} of ${branch} is no longer available from DRA; not pinning it`);
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
  Object.fromEntries(pins.flatMap((pin) => (pin.knownGood ? [[pin.branch, pin.knownGood.build_id]] : [])));

/**
 * Pins for PR and merge-queue bwc jobs. Fails open: if the store is disabled or unreachable, returns no pins so that
 * bwc jobs keep today's behaviour instead of losing coverage.
 */
export const loadPinArgs = async (
  versions: string[],
  store: KnownGoodStore | undefined,
  dra: Dra,
  current: string = currentVersion(),
): Promise<string> => {
  if (!store) {
    console.log("BWC_KNOWN_GOOD_BUCKET is not set; bwc snapshot jobs test the moving branch tips");
    return "";
  }
  try {
    const pins = await resolvePins(versions, current, store, dra);
    for (const pin of pins.filter((p) => !p.current)) {
      console.log(`bwc ${pin.version}: ${pin.knownGood ? `pinned to ${pin.knownGood.build_id}` : "no known-good build"}`);
    }
    return pinArgs(knownGoodBuildIds(pins));
  } catch (e) {
    console.log(`Could not read known-good bwc builds from ${store.location}, testing the moving branch tips: ${e}`);
    return "";
  }
};

export type PromoteRequest = {
  branch: string;
  buildId: string;
  now?: Date;
  sourceBuildUrl?: string;
  testedBranch?: string;
  testedCommit?: string;
};

export type PromoteResult = { promoted: boolean; record: KnownGood | undefined; reason: string };

/**
 * Records `buildId` as the known-good build of its branch. Only call this after a bwc run that consumed exactly this
 * build passed. Never moves a branch backwards: an older version, or an older build of the same version, is ignored.
 */
export const promote = async (store: KnownGoodStore, dra: Dra, request: PromoteRequest): Promise<PromoteResult> => {
  const version = buildVersion(request.buildId);
  if (branchForVersion(version) !== request.branch) {
    throw new Error(`build ${request.buildId} is not a build of branch ${request.branch}`);
  }
  const manifest = await dra.manifest(request.buildId);
  if (!manifest) {
    throw new Error(`DRA build ${request.buildId} does not exist`);
  }
  const candidate: KnownGood = {
    branch: request.branch,
    version,
    build_id: request.buildId,
    commit: manifest.commit,
    dra_start_time: manifest.startTime,
    promoted_at: (request.now ?? new Date()).toISOString(),
    source_build_url: request.sourceBuildUrl,
    tested_branch: request.testedBranch,
    tested_commit: request.testedCommit,
  };
  let reason = "promoted";
  let promoted = false;
  const record = await store.update(request.branch, (current) => {
    const keep = keepCurrent(current, candidate);
    if (keep) {
      reason = keep;
      promoted = false;
      return undefined;
    }
    reason = "promoted";
    promoted = true;
    return candidate;
  });
  return { promoted, record, reason };
};

const keepCurrent = (current: KnownGood | undefined, candidate: KnownGood): string | undefined => {
  if (!current) {
    return undefined;
  }
  if (current.build_id === candidate.build_id) {
    return `${candidate.build_id} is already the known-good build`;
  }
  const versionOrder = compareVersions(candidate.version, current.version);
  if (versionOrder < 0) {
    return `known-good ${current.build_id} is for a newer version than ${candidate.build_id}`;
  }
  if (
    versionOrder === 0 &&
    current.dra_start_time &&
    candidate.dra_start_time &&
    Date.parse(candidate.dra_start_time) < Date.parse(current.dra_start_time)
  ) {
    return `known-good ${current.build_id} is newer than ${candidate.build_id}`;
  }
  return undefined;
};

/** The newest DRA build of `branch` if it is a build of `version`. */
export const latestBuildOf = async (dra: Dra, branch: string, version: string): Promise<DraBuild | undefined> => {
  const latest = await dra.latest(branch);
  return latest?.version === version ? latest : undefined;
};
