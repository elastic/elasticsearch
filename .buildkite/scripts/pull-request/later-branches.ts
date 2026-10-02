import { execSync } from "child_process";
import { resolve } from "path";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../..`);

export type DevelopmentBranch = {
  branch: string;
  version: string;
};

export type BranchesJson = {
  branches: DevelopmentBranch[];
};

let BRANCHES_JSON: BranchesJson | undefined;

// branches.json is only maintained on main, so it is not in the working tree of a release branch.
// Read it out of git rather than over http, since the raw.githubusercontent copy is cached and can
// be stale for several minutes after a branch is added or removed.
const readBranchesJson = (): BranchesJson => {
  execSync("git fetch --no-tags --quiet origin main", { cwd: PROJECT_ROOT });
  return JSON.parse(execSync("git show FETCH_HEAD:branches.json", { cwd: PROJECT_ROOT }).toString());
};

export const getBranchesJson = () => (BRANCHES_JSON = BRANCHES_JSON ?? readBranchesJson());

export const setBranchesJson = (branchesJson: BranchesJson | undefined) => {
  BRANCHES_JSON = branchesJson;
};

/**
 * The development branches that are ahead of the given branch, oldest first.
 *
 * A change merged into a release branch also has to keep the backwards compatibility tests of every
 * branch ahead of it passing, since those branches test against a snapshot built from it. Returns an
 * empty list for main, and for any branch that isn't a development branch at all.
 *
 * branches.json is written newest first, so everything listed before the target branch is ahead of
 * it. See UpdateBranchesJsonTask, which sorts by version descending when generating the file.
 */
export const getLaterBranches = (targetBranch: string | undefined): string[] => {
  if (!targetBranch) {
    return [];
  }

  const branches = getBranchesJson().branches;
  const target = branches.findIndex((branch) => branch.branch === targetBranch);
  if (target < 0) {
    return [];
  }

  return branches
    .slice(0, target)
    .map((branch) => branch.branch)
    .reverse();
};
