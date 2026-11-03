// In-process fakes of the GCS JSON API, the Google OAuth2 token endpoint and the DRA snapshot endpoints.
// Used by the vitest suites and for dry-running the CLI without touching real buckets.

import { createServer, type IncomingMessage, type Server } from "http";
import { createVerify } from "crypto";

export type FakeGcs = {
  url: string;
  /** "bucket/object" => { body, generation } */
  objects: Map<string, { body: string; generation: number }>;
  /** Number of writes rejected because of a stale ifGenerationMatch */
  preconditionFailures: number;
  close: () => Promise<void>;
};

export type FakeGcsOptions = {
  /** When set, requests must carry the token issued for a JWT signed by this key, and the token endpoint is served */
  publicKeyPem?: string;
};

const readBody = async (req: IncomingMessage): Promise<string> => {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf-8");
};

const listen = async (server: Server): Promise<string> => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("server has no port");
  }
  return `http://127.0.0.1:${address.port}`;
};

const close = (server: Server) => (): Promise<void> => new Promise((resolve) => server.close(() => resolve()));

const FAKE_TOKEN = "fake-access-token";

export const startFakeGcs = async (options: FakeGcsOptions = {}): Promise<FakeGcs> => {
  const objects = new Map<string, { body: string; generation: number }>();
  let generation = 1000;
  const fake = { objects, preconditionFailures: 0 };

  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const body = await readBody(req);

    if (req.method === "POST" && url.pathname === "/token") {
      const assertion = new URLSearchParams(body).get("assertion") ?? "";
      const [header, claims, signature] = assertion.split(".");
      const valid =
        options.publicKeyPem &&
        signature &&
        createVerify("RSA-SHA256").update(`${header}.${claims}`).verify(options.publicKeyPem, signature, "base64url");
      res.writeHead(valid ? 200 : 401, { "Content-Type": "application/json" });
      res.end(JSON.stringify(valid ? { access_token: FAKE_TOKEN, expires_in: 3600 } : { error: "invalid_grant" }));
      return;
    }

    if (options.publicKeyPem && req.headers.authorization !== `Bearer ${FAKE_TOKEN}`) {
      res.writeHead(401).end("missing or wrong token");
      return;
    }

    const read = url.pathname.match(/^\/storage\/v1\/b\/([^/]+)\/o\/(.+)$/);
    if (req.method === "GET" && read) {
      const object = objects.get(`${decodeURIComponent(read[1] ?? "")}/${decodeURIComponent(read[2] ?? "")}`);
      if (!object) {
        res.writeHead(404).end("not found");
        return;
      }
      res.writeHead(200, { "x-goog-generation": String(object.generation) }).end(object.body);
      return;
    }

    const write = url.pathname.match(/^\/upload\/storage\/v1\/b\/([^/]+)\/o$/);
    if (req.method === "POST" && write) {
      const key = `${decodeURIComponent(write[1] ?? "")}/${url.searchParams.get("name")}`;
      const expected = url.searchParams.get("ifGenerationMatch");
      const current = objects.get(key)?.generation ?? 0;
      if (expected !== null && Number(expected) !== current) {
        fake.preconditionFailures++;
        res.writeHead(412).end("precondition failed");
        return;
      }
      objects.set(key, { body, generation: ++generation });
      res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify({ generation: String(generation) }));
      return;
    }

    res.writeHead(400).end(`unexpected ${req.method} ${url.pathname}`);
  });

  const url = await listen(server);
  return Object.assign(fake, { url, close: close(server) });
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
