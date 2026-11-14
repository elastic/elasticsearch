// In-process fakes of the known-good bucket and the DRA snapshot endpoints.
// Used by the vitest suites and for dry-running the CLI without touching real buckets.

import { createServer, type Server } from "http";

import { Bucket, type BucketWriter, PreconditionFailedError } from "./gcs.ts";

const listen = async (server: Server): Promise<string> => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("server has no port");
  }
  return `http://127.0.0.1:${address.port}`;
};

const close = (server: Server) => (): Promise<void> => new Promise((resolve) => server.close(() => resolve()));

export type FakeBucket = {
  /** Reads over HTTP from the fake server, writes in memory with the same generation preconditions as gsutil */
  bucket: Bucket;
  /** object name => { body, generation } */
  objects: Map<string, { body: string; generation: number }>;
  /** Number of writes rejected because of a stale generation */
  preconditionFailures: number;
  /** Bumps the object's generation right before the next write, as a concurrent writer would */
  interfereOnce: (name: string) => void;
  close: () => Promise<void>;
};

export const startFakeBucket = async (): Promise<FakeBucket> => {
  const objects = new Map<string, { body: string; generation: number }>();
  let generation = 1000;
  let interfere: string | undefined;

  const server = createServer((req, res) => {
    const object = objects.get(decodeURIComponent(new URL(req.url ?? "/", "http://localhost").pathname.slice(1)));
    if (req.method !== "GET" || !object) {
      res.writeHead(404).end("not found");
      return;
    }
    res.writeHead(200, { "x-goog-generation": String(object.generation) }).end(object.body);
  });

  const checkGeneration = (name: string, expected: string): void => {
    if (interfere === name) {
      interfere = undefined;
      const object = objects.get(name);
      if (object) {
        objects.set(name, { ...object, generation: ++generation });
      }
    }
    if (Number(expected) !== (objects.get(name)?.generation ?? 0)) {
      fake.preconditionFailures++;
      throw new PreconditionFailedError(`${name} changed concurrently`);
    }
  };
  const writer: BucketWriter = {
    write: async (name, body, ifGenerationMatch) => {
      checkGeneration(name, ifGenerationMatch);
      objects.set(name, { body, generation: ++generation });
    },
    remove: async (name, ifGenerationMatch) => {
      checkGeneration(name, ifGenerationMatch);
      objects.delete(name);
    },
  };

  const url = await listen(server);
  const fake: FakeBucket = {
    bucket: new Bucket(url, writer),
    objects,
    preconditionFailures: 0,
    interfereOnce: (name) => {
      interfere = name;
    },
    close: close(server),
  };
  return fake;
};

export type FakeDraBuild = { buildId: string; startTime: string; commit: string };

export type FakeDra = {
  url: string;
  /** branch => newest build ID */
  latest: Map<string, string>;
  /** build ID => build */
  builds: Map<string, FakeDraBuild>;
  close: () => Promise<void>;
};

export const startFakeDra = async (): Promise<FakeDra> => {
  const latest = new Map<string, string>();
  const builds = new Map<string, FakeDraBuild>();
  const server = createServer((req, res) => {
    const path = new URL(req.url ?? "/", "http://localhost").pathname;
    const latestMatch = path.match(/^\/elasticsearch\/latest\/(.+)\.json$/);
    const buildId = latestMatch ? latest.get(latestMatch[1] ?? "") : undefined;
    if (latestMatch && buildId) {
      res.writeHead(200).end(JSON.stringify({ build_id: buildId, version: `${buildId.split("-")[0]}-SNAPSHOT` }));
      return;
    }
    const manifestMatch = path.match(/^\/elasticsearch\/([^/]+)\/manifest-.+-SNAPSHOT\.json$/);
    const build = manifestMatch ? builds.get(manifestMatch[1] ?? "") : undefined;
    if (build) {
      res.writeHead(200).end(
        JSON.stringify({
          build_id: build.buildId,
          start_time: build.startTime,
          projects: { elasticsearch: { commit_hash: build.commit } },
        }),
      );
      return;
    }
    res.writeHead(404).end("not found");
  });
  const url = await listen(server);
  return { url, latest, builds, close: close(server) };
};

/** Adds a DRA build and makes it the newest build of its branch. */
export const addDraBuild = (dra: FakeDra, buildId: string, startTime: string): void => {
  dra.builds.set(buildId, { buildId, startTime, commit: `commit-of-${buildId}` });
  dra.latest.set(buildId.split(".").slice(0, 2).join("."), buildId);
};
