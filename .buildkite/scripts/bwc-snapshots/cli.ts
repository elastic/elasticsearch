// Command line entry point for the bwc known-good store.
//
//   node .buildkite/scripts/bwc-snapshots/cli.ts intake-pipeline
//       Generate intake's bwc-snapshots group; uploads it when running in CI.
//   node .buildkite/scripts/bwc-snapshots/cli.ts promote --branch 9.4 --build-id 9.4.10-3710ab3e --step <key> [--step <key>]
//       Promote a build after checking that the given Buildkite steps passed. --no-outcome-check skips that check;
//       use it only to seed the store by hand with a build you know passed bwc tests.
//   node .buildkite/scripts/bwc-snapshots/cli.ts included
//       Print the versions of .ci/snapshotBwcVersions that PR and merge-queue bwc jobs test.
//   node .buildkite/scripts/bwc-snapshots/cli.ts show
//       Print the known-good build of every snapshot bwc version's branch.

import { execFileSync } from "child_process";
import { parseArgs } from "util";
import { stringify } from "yaml";

import { getSnapshotBwcVersions } from "../pull-request/bwc-versions.ts";
import { DraClient } from "./dra.ts";
import { type NotGreenAlert, pendingStoreFromEnv, trackPending } from "./gating.ts";
import { intakeBwcGroup, resolveIntakePlans, unpinnedIntakeBwcGroup } from "./intake-pipeline.ts";
import { branchForVersion, currentVersion, loadIncludedSnapshotBwcVersions, promote, storeFromEnv } from "./known-good.ts";

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
      const plans = await resolveIntakePlans(versions, currentVersion(), store, dra);
      const pendingStore = pendingStoreFromEnv();
      const alerts: NotGreenAlert[] = [];
      for (const plan of plans) {
        try {
          const alert = pendingStore && (await trackPending(pendingStore, plan));
          if (alert) {
            alerts.push(alert);
          }
        } catch (e) {
          // The clock only drives alerts; never let it break bwc testing.
          console.error(`Could not update the not-green clock of ${plan.branch}: ${e}`);
        }
      }
      group = intakeBwcGroup(plans, alerts);
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
  const result = await promote(store, dra, { branch, buildId });
  console.log(`${result.promoted ? "Promoted" : "Did not promote"} ${buildId} for ${branch} in ${store.location}: ${result.reason}`);
};

const show = async (): Promise<void> => {
  const store = storeFromEnv();
  if (!store) {
    throw new Error("BWC_KNOWN_GOOD_BUCKET is not set");
  }
  for (const version of getSnapshotBwcVersions() as string[]) {
    const branch = branchForVersion(version);
    console.log(`${branch}: ${(await store.read(branch)) ?? "none"}`);
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
  case "included":
    console.log(JSON.stringify(await loadIncludedSnapshotBwcVersions(getSnapshotBwcVersions(), storeFromEnv(), dra)));
    break;
  case "show":
    await show();
    break;
  default:
    console.error("usage: cli.ts intake-pipeline | promote --branch <b> --build-id <id> --step <key>... | included | show");
    process.exit(2);
}
