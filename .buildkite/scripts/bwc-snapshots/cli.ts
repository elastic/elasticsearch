// Command line entry point for the bwc known-good store.
//
//   node .buildkite/scripts/bwc-snapshots/cli.ts intake-pipeline
//       Generate intake's bwc-snapshots group; uploads it when running in CI.
//   node .buildkite/scripts/bwc-snapshots/cli.ts promote --branch 9.4 --build-id 9.4.10-3710ab3e --step <key> [--step <key>]
//       Promote a build after checking that the given Buildkite steps passed. --no-outcome-check skips that check;
//       use it only to seed the store by hand with a build you know passed bwc tests.
//   node .buildkite/scripts/bwc-snapshots/cli.ts pins
//       Print the -Dtests.bwc.dra.hash.* arguments for .ci/snapshotBwcVersions.
//   node .buildkite/scripts/bwc-snapshots/cli.ts show
//       Print the stored record of every snapshot bwc version's branch.

import { execFileSync } from "child_process";
import { parseArgs } from "util";
import { stringify } from "yaml";

import { getSnapshotBwcVersions } from "../pull-request/bwc-versions.ts";
import { DraClient } from "./dra.ts";
import { intakeBwcGroup, resolveIntakePlans, unpinnedIntakeBwcGroup } from "./intake-pipeline.ts";
import { branchForVersion, currentVersion, loadPinArgs, promote, storeFromEnv } from "./known-good.ts";

const dra = new DraClient(process.env["BWC_DRA_BASE_URL"]);

const stepOutcome = (key: string): string =>
  execFileSync("buildkite-agent", ["step", "get", "outcome", "--step", key], { encoding: "utf-8" }).trim();

const intakePipeline = async (): Promise<void> => {
  const versions: string[] = getSnapshotBwcVersions();
  const store = storeFromEnv();
  let group;
  if (!store) {
    console.error("BWC_KNOWN_GOOD_BUCKET is not set; bwc snapshot jobs test the moving branch tips");
    group = unpinnedIntakeBwcGroup(versions);
  } else {
    try {
      group = intakeBwcGroup(await resolveIntakePlans(versions, currentVersion(), store, dra));
    } catch (e) {
      console.error(`Could not read known-good bwc builds from ${store.location}, testing the moving branch tips: ${e}`);
      group = unpinnedIntakeBwcGroup(versions);
    }
  }
  const yaml = stringify({ steps: [group] });
  console.log(yaml);
  if (process.env["CI"]) {
    execFileSync("buildkite-agent", ["pipeline", "upload"], { input: yaml, stdio: ["pipe", "inherit", "inherit"] });
  }
};

const promoteCommand = async (args: string[]): Promise<void> => {
  const { values } = parseArgs({
    args,
    options: {
      branch: { type: "string" },
      "build-id": { type: "string" },
      step: { type: "string", multiple: true },
      "no-outcome-check": { type: "boolean" },
    },
  });
  const branch = values.branch;
  const buildId = values["build-id"];
  if (!branch || !buildId) {
    throw new Error("promote needs --branch and --build-id");
  }
  if (!values["no-outcome-check"]) {
    const steps = values.step ?? [];
    if (steps.length === 0) {
      throw new Error("promote needs at least one --step whose outcome proves the build passed");
    }
    for (const step of steps) {
      const outcome = stepOutcome(step);
      if (outcome !== "passed") {
        console.log(`Not promoting ${buildId}: step ${step} outcome is ${outcome || "unknown"}`);
        return;
      }
    }
  }
  const store = storeFromEnv();
  if (!store) {
    throw new Error("BWC_KNOWN_GOOD_BUCKET is not set");
  }
  const result = await promote(store, dra, {
    branch,
    buildId,
    sourceBuildUrl: process.env["BUILDKITE_BUILD_URL"],
    testedBranch: process.env["BUILDKITE_BRANCH"],
    testedCommit: process.env["BUILDKITE_COMMIT"],
  });
  console.log(`${result.promoted ? "Promoted" : "Did not promote"} ${buildId} for ${branch} in ${store.location}: ${result.reason}`);
};

const show = async (): Promise<void> => {
  const store = storeFromEnv();
  if (!store) {
    throw new Error("BWC_KNOWN_GOOD_BUCKET is not set");
  }
  for (const version of getSnapshotBwcVersions() as string[]) {
    const branch = branchForVersion(version);
    console.log(`${branch}: ${JSON.stringify(await store.read(branch))}`);
  }
};

const [command, ...rest] = process.argv.slice(2);
switch (command) {
  case "intake-pipeline":
    await intakePipeline();
    break;
  case "promote":
    await promoteCommand(rest);
    break;
  case "pins":
    console.log((await loadPinArgs(getSnapshotBwcVersions(), storeFromEnv(), dra)).trim());
    break;
  case "show":
    await show();
    break;
  default:
    console.error("usage: cli.ts intake-pipeline | promote --branch <b> --build-id <id> --step <key>... | pins | show");
    process.exit(2);
}
