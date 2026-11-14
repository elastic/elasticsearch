// Read-only client for DRA snapshot builds (artifacts-snapshot.elastic.co), the same endpoints that
// DraSnapshotBuildIdValueSource uses in the Gradle build.

export type DraBuild = { buildId: string; version: string };

export type DraManifest = { buildId: string; startTime?: string; commit?: string };

export interface Dra {
  /** The newest snapshot build of a branch, or undefined if there is none. */
  latest(branch: string): Promise<DraBuild | undefined>;
  /** The manifest of a build, or undefined if the build does not exist (never built or removed by retention). */
  manifest(buildId: string): Promise<DraManifest | undefined>;
}

export class DraClient implements Dra {
  private readonly baseUrl: string;

  constructor(baseUrl = "https://artifacts-snapshot.elastic.co") {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  async latest(branch: string): Promise<DraBuild | undefined> {
    const response = await fetch(`${this.baseUrl}/elasticsearch/latest/${branch}.json`, { signal: AbortSignal.timeout(30_000) });
    if (response.status === 404) {
      return undefined;
    }
    if (!response.ok) {
      throw new Error(`DRA latest build for ${branch} returned HTTP ${response.status}`);
    }
    const json: { build_id?: string } = await response.json();
    if (!json.build_id) {
      return undefined;
    }
    return { buildId: json.build_id, version: buildVersion(json.build_id) };
  }

  async manifest(buildId: string): Promise<DraManifest | undefined> {
    const version = buildVersion(buildId);
    const response = await fetch(`${this.baseUrl}/elasticsearch/${buildId}/manifest-${version}-SNAPSHOT.json`, {
      signal: AbortSignal.timeout(30_000),
    });
    if (response.status === 404) {
      return undefined;
    }
    if (!response.ok) {
      throw new Error(`DRA manifest for ${buildId} returned HTTP ${response.status}`);
    }
    const json: { start_time?: string; projects?: { elasticsearch?: { commit_hash?: string } } } = await response.json();
    return { buildId, startTime: json.start_time, commit: json.projects?.elasticsearch?.commit_hash };
  }
}

/** "9.4.10-3710ab3e" => "9.4.10" */
export const buildVersion = (buildId: string): string => buildId.substring(0, buildId.lastIndexOf("-"));

/** "9.4.10-3710ab3e" => "3710ab3e", the value for -Dtests.bwc.dra.hash.<branch> */
export const buildHash = (buildId: string): string => buildId.substring(buildId.lastIndexOf("-") + 1);
