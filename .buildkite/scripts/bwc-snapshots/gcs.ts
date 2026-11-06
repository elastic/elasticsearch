// The GCS bucket that holds the bwc known-good builds.
//
// The bucket is publicly readable, so objects are read with a plain HTTP GET (no credentials: PR builds and Gradle
// read the same URLs). Only intake writes, with gsutil, as the service account its pre-command hook activates.
// Writes carry an x-goog-if-generation-match precondition so that concurrent intake builds cannot overwrite each
// other, and Cache-Control:no-store so that public reads never see a stale object.

import { spawnSync } from "child_process";

export type BucketObject = { body: string; generation: string };

export class PreconditionFailedError extends Error {}

/** Writes objects; "0" as the generation means the object must not exist yet. */
export interface BucketWriter {
  write(name: string, body: string, ifGenerationMatch: string): Promise<void>;
  remove(name: string, ifGenerationMatch: string): Promise<void>;
}

export class Bucket {
  /** Public base URL of the objects, e.g. https://storage.googleapis.com/<bucket> */
  readonly url: string;
  private readonly writer: BucketWriter;

  constructor(url: string, writer: BucketWriter) {
    this.url = url.replace(/\/$/, "");
    this.writer = writer;
  }

  /** Returns the object, or undefined when it does not exist. */
  async read(name: string): Promise<BucketObject | undefined> {
    const response = await fetch(`${this.url}/${name}`, { cache: "no-store", signal: AbortSignal.timeout(30_000) });
    if (response.status === 404) {
      return undefined;
    }
    if (!response.ok) {
      throw new Error(`GET ${this.url}/${name} returned HTTP ${response.status}`);
    }
    return { body: await response.text(), generation: response.headers.get("x-goog-generation") ?? "0" };
  }

  /**
   * Read-modify-write of one object. `change` returns the new body, null to delete the object, or undefined to leave
   * it as is. Retries when another writer changed the object in between. Returns the body the object ends up with.
   */
  async update(
    name: string,
    change: (current: string | undefined) => Promise<string | null | undefined> | string | null | undefined,
  ): Promise<string | undefined> {
    for (let attempt = 1; ; attempt++) {
      const object = await this.read(name);
      const next = await change(object?.body);
      try {
        if (next === undefined) {
          return object?.body;
        }
        if (next === null) {
          if (object) {
            await this.writer.remove(name, object.generation);
          }
          return undefined;
        }
        await this.writer.write(name, next, object?.generation ?? "0");
        return next;
      } catch (e) {
        if (!(e instanceof PreconditionFailedError) || attempt >= 3) {
          throw e;
        }
        console.log(`${e.message}; retrying`);
      }
    }
  }
}

export const gsutilArgs = {
  write: (bucket: string, name: string, ifGenerationMatch: string): string[] => [
    "-h",
    "Cache-Control:no-store",
    "-h",
    `x-goog-if-generation-match:${ifGenerationMatch}`,
    "cp",
    "-",
    `gs://${bucket}/${name}`,
  ],
  remove: (bucket: string, name: string, ifGenerationMatch: string): string[] => [
    "-h",
    `x-goog-if-generation-match:${ifGenerationMatch}`,
    "rm",
    `gs://${bucket}/${name}`,
  ],
};

const gsutil = (args: string[], input?: string): void => {
  const result = spawnSync("gsutil", args, { input, encoding: "utf-8" });
  if (result.status === 0) {
    return;
  }
  const output = `${result.stderr ?? ""}${result.error ?? ""}`.trim();
  if (/PreconditionException|412/.test(output)) {
    throw new PreconditionFailedError(`${args.at(-1)} changed concurrently: ${output}`);
  }
  throw new Error(`gsutil ${args.join(" ")} failed: ${output}`);
};

export const gsutilWriter = (bucket: string): BucketWriter => ({
  write: async (name, body, ifGenerationMatch) => gsutil(gsutilArgs.write(bucket, name, ifGenerationMatch), body),
  remove: async (name, ifGenerationMatch) => gsutil(gsutilArgs.remove(bucket, name, ifGenerationMatch)),
});

/**
 * The bucket and object prefix configured by the environment, or undefined when BWC_KNOWN_GOOD_BUCKET is unset.
 *
 * - BWC_KNOWN_GOOD_BUCKET: bucket name; unset disables pinning and gating
 * - BWC_KNOWN_GOOD_PREFIX: object prefix, default elasticsearch/bwc-known-good
 * - BWC_KNOWN_GOOD_URL: public base URL of the objects, default https://storage.googleapis.com/<bucket>; tests and
 *   dry-runs point it at a fake server
 */
export const bucketFromEnv = (
  env: NodeJS.ProcessEnv = process.env,
  writer?: BucketWriter,
): { bucket: Bucket; prefix: string } | undefined => {
  const name = env["BWC_KNOWN_GOOD_BUCKET"];
  if (!name) {
    return undefined;
  }
  return {
    bucket: new Bucket(env["BWC_KNOWN_GOOD_URL"] || `https://storage.googleapis.com/${name}`, writer ?? gsutilWriter(name)),
    prefix: (env["BWC_KNOWN_GOOD_PREFIX"] ?? "elasticsearch/bwc-known-good").replace(/\/$/, ""),
  };
};
