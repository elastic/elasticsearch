// Minimal Google Cloud Storage JSON API client for the bwc known-good store.
//
// Uses fetch only (no gcloud on the agents, no SDK dependency). Objects are read and written whole; writes carry an
// ifGenerationMatch precondition so that concurrent intake builds cannot overwrite each other's promotions.

import { createSign } from "crypto";
import { readFileSync } from "fs";

export type GcsObject = { body: string; generation: string };

export class PreconditionFailedError extends Error {}

export type GcsOptions = {
  bucket: string;
  /** Defaults to https://storage.googleapis.com. Tests and dry-runs point this at a fake server. */
  baseUrl?: string;
  /** Returns an OAuth2 access token, or undefined for anonymous access (a publicly readable bucket). */
  token?: () => Promise<string | undefined>;
};

export class GcsClient {
  private readonly bucket: string;
  private readonly baseUrl: string;
  private readonly token: () => Promise<string | undefined>;

  constructor(options: GcsOptions) {
    this.bucket = options.bucket;
    this.baseUrl = (options.baseUrl ?? "https://storage.googleapis.com").replace(/\/$/, "");
    this.token = options.token ?? (async () => undefined);
  }

  /** Returns the object, or undefined when it does not exist. */
  async read(name: string): Promise<GcsObject | undefined> {
    const url = `${this.baseUrl}/storage/v1/b/${encodeURIComponent(this.bucket)}/o/${encodeURIComponent(name)}?alt=media`;
    const response = await fetch(url, { headers: await this.headers(), signal: AbortSignal.timeout(30_000) });
    if (response.status === 404) {
      return undefined;
    }
    if (!response.ok) {
      throw new Error(`GET gs://${this.bucket}/${name} returned HTTP ${response.status}: ${await response.text()}`);
    }
    return { body: await response.text(), generation: response.headers.get("x-goog-generation") ?? "0" };
  }

  /**
   * Writes the object only if its generation still matches; "0" means it must not exist yet.
   * Throws PreconditionFailedError when another writer got there first.
   */
  async write(name: string, body: string, ifGenerationMatch: string): Promise<void> {
    const params = new URLSearchParams({ uploadType: "media", name, ifGenerationMatch });
    const url = `${this.baseUrl}/upload/storage/v1/b/${encodeURIComponent(this.bucket)}/o?${params}`;
    const response = await fetch(url, {
      method: "POST",
      headers: { ...(await this.headers()), "Content-Type": "application/json" },
      body,
      signal: AbortSignal.timeout(30_000),
    });
    if (response.status === 412) {
      throw new PreconditionFailedError(`gs://${this.bucket}/${name} changed concurrently (generation != ${ifGenerationMatch})`);
    }
    if (!response.ok) {
      throw new Error(`POST gs://${this.bucket}/${name} returned HTTP ${response.status}: ${await response.text()}`);
    }
  }

  private async headers(): Promise<Record<string, string>> {
    const token = await this.token();
    return token ? { Authorization: `Bearer ${token}` } : {};
  }
}

type ServiceAccountKey = { client_email: string; private_key: string; token_uri?: string };

/**
 * Exchanges a service account key for an access token (OAuth2 JWT bearer grant). The token is cached for the
 * lifetime of the process, which is far shorter than the token's one hour validity.
 */
export const serviceAccountToken = (keyFile: string, scope: string): (() => Promise<string>) => {
  let cached: Promise<string> | undefined;
  return () => {
    cached ??= (async () => {
      const key: ServiceAccountKey = JSON.parse(readFileSync(keyFile, "utf-8"));
      const tokenUri = key.token_uri ?? "https://oauth2.googleapis.com/token";
      const now = Math.floor(Date.now() / 1000);
      const encode = (value: object) => Buffer.from(JSON.stringify(value)).toString("base64url");
      const unsigned = `${encode({ alg: "RS256", typ: "JWT" })}.${encode({
        iss: key.client_email,
        scope,
        aud: tokenUri,
        iat: now,
        exp: now + 3600,
      })}`;
      const signature = createSign("RSA-SHA256").update(unsigned).sign(key.private_key, "base64url");
      const response = await fetch(tokenUri, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
          assertion: `${unsigned}.${signature}`,
        }),
        signal: AbortSignal.timeout(30_000),
      });
      if (!response.ok) {
        throw new Error(`token exchange for ${key.client_email} returned HTTP ${response.status}: ${await response.text()}`);
      }
      const json: { access_token?: string } = await response.json();
      if (!json.access_token) {
        throw new Error(`token exchange for ${key.client_email} returned no access_token`);
      }
      return json.access_token;
    })();
    return cached;
  };
};
