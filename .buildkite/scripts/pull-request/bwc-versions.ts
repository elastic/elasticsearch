import { parse } from "yaml";
import { readFileSync } from "fs";
import { resolve } from "path";

const PROJECT_ROOT = resolve(`${import.meta.dir}/../../..`);

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
