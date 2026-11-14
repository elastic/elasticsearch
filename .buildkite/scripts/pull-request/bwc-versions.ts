import { parse } from "yaml";
import { readFileSync } from "fs";
import { resolve } from "path";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../..`);

let BWC_VERSIONS_PATH = `${PROJECT_ROOT}/.ci/bwcVersions`;
let BWC_VERSIONS: any;

let SNAPSHOT_BWC_VERSIONS_PATH = `${PROJECT_ROOT}/.ci/snapshotBwcVersions`;
let SNAPSHOT_BWC_VERSIONS: any;

export const getSnapshotBwcVersions = () => {
  SNAPSHOT_BWC_VERSIONS = SNAPSHOT_BWC_VERSIONS ?? parse(readFileSync(SNAPSHOT_BWC_VERSIONS_PATH, "utf-8")).BWC_VERSION;

  return SNAPSHOT_BWC_VERSIONS;
};

export const getBwcVersions = () => {
  BWC_VERSIONS = BWC_VERSIONS ?? parse(readFileSync(BWC_VERSIONS_PATH, "utf-8")).BWC_VERSION;
  return BWC_VERSIONS;
};

export const setSnapshotBwcVersionsPath = (path: string) => {
  SNAPSHOT_BWC_VERSIONS_PATH = path;
};

export const setBwcVersionsPath = (path: string) => {
  BWC_VERSIONS_PATH = path;
};

// Snapshot bwc versions that have a known-good DRA build (plus the current version). A version newly added to
// .ci/snapshotBwcVersions stays out of PR and merge-queue bwc until intake promotes a build of it. Undefined: no gating.
let INCLUDED_SNAPSHOT_BWC_VERSIONS: string[] | undefined;

export const setIncludedSnapshotBwcVersions = (versions: string[] | undefined): void => {
  INCLUDED_SNAPSHOT_BWC_VERSIONS = versions;
};

export const getIncludedSnapshotBwcVersions = (): string[] => {
  const included = INCLUDED_SNAPSHOT_BWC_VERSIONS;
  const all: string[] = getSnapshotBwcVersions();
  return included ? all.filter((version) => included.includes(version)) : all;
};
