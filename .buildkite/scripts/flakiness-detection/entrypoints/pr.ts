import { execSync } from "child_process";
import { readFileSync, writeFileSync } from "fs";
import { resolve } from "path";

import { findUnmutedRefs } from "../detectors/unmutes.ts";
import { uploadResolvePipeline } from "../runners/buildkite.ts";
import { DEFAULT_AGENT_CONFIG, type FlakinessRef, type FlakinessRefsFile } from "../domain.ts";

// The refs file the Java resolver reads (contract 1). Uploaded as a build artifact by the bootstrap step
// so the resolve step can download it onto its fresh agent. Keep in sync with FLAKINESS_REFS_ARTIFACT in
// domain.ts, which is what the pipeline generator and orchestrate.sh both read.
const REFS_FILE = "flakiness-refs.json";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

type CommandRunner = (
  command: string,
  options: { cwd: string; stdio?: "inherit" | "ignore" }
) => Buffer;

export function resolveMergeBaseTarget(
  targetBranch: string,
  run: CommandRunner = (command, options) => execSync(command, options),
  projectRoot: string = PROJECT_ROOT
): string {
  try {
    run(`git rev-parse --verify ${targetBranch}^{commit}`, { cwd: projectRoot, stdio: "ignore" });
    return targetBranch;
  } catch {
    // Some target branches aren't present in the local checkout: ghstack synthetic
    // refs (gh/<user>/<n>/base) and serverless patch branches (patch/<name>). Fetch
    // the ref and use FETCH_HEAD so we don't depend on origin/<branch> naming.
    console.log(`  ${targetBranch} not present locally, fetching from origin...`);
    run(`git fetch --no-tags origin ${targetBranch}`, { cwd: projectRoot, stdio: "inherit" });
    return "FETCH_HEAD";
  }
}

// Cap the synchronous git invocations so a hung process (network credential
// prompt during a partial-clone lazy fetch, .git/index.lock contention, etc.)
// surfaces as a visible failure instead of silently consuming the step's
// timeout_in_minutes budget.
const GIT_COMMAND_TIMEOUT_MS = 60_000;

/**
 * Whether a changed file is worth handing to the resolver at all.
 *
 * This is a **cost gate, not a classifier**. It never decides what a ref means - the resolver still does all
 * of that against the real source-set model. It only decides whether starting the Gradle orchestration is
 * worth it, because every changed file becomes a ref and the compile phase's own guard only fires after
 * resolve has already run. Without this, a docs-only or build-script-only PR pays a whole resolve pass over
 * ~450 projects, plus a scan, to produce an empty plan.
 */
export function mayBeTestSource(path: string): boolean {
  return /(^|\/)src\//.test(path);
}

// Gather `unmute` refs from the muted-tests.yml diff.
function gatherUnmuteRefs(mergeBase: string, projectRoot: string): FlakinessRef[] {
  console.log(`  Reading muted-tests.yml at ${mergeBase}...`);
  let oldYaml = "";
  try {
    oldYaml = execSync(`git show ${mergeBase}:muted-tests.yml`, {
      cwd: projectRoot,
      stdio: ["ignore", "pipe", "pipe"],
      timeout: GIT_COMMAND_TIMEOUT_MS,
      encoding: "utf8",
    });
  } catch (err) {
    // File didn't exist at merge base; treat as empty. Log so the next time
    // git hangs or fails for a different reason we can see why.
    console.log(`  Could not read muted-tests.yml at ${mergeBase}: ${(err as Error).message}`);
  }

  console.log("  Reading muted-tests.yml from working tree...");
  let newYaml = "";
  try {
    newYaml = readFileSync(resolve(projectRoot, "muted-tests.yml"), "utf8");
  } catch {
    // File was deleted in the PR; treat as empty.
  }

  return findUnmutedRefs(oldYaml, newYaml);
}

export function run(): void {
  console.log("Computing merge base...");
  const targetBranch = process.env.GITHUB_PR_TARGET_BRANCH;
  if (!targetBranch) {
    throw new Error("GITHUB_PR_TARGET_BRANCH environment variable is required");
  }
  const targetRef = resolveMergeBaseTarget(targetBranch);
  const mergeBase = execSync(`git merge-base ${targetRef} HEAD`, { cwd: PROJECT_ROOT }).toString().trim();
  console.log(`Merge base: ${mergeBase}`);

  console.log("Getting changed files...");
  const changedFilesOutput = execSync(`git diff --diff-filter=d --name-only ${mergeBase}`, { cwd: PROJECT_ROOT })
    .toString()
    .trim();
  const changedFiles = changedFilesOutput.split("\n").map((f) => f.trim()).filter((f) => f);
  const sourceFiles = changedFiles.filter(mayBeTestSource);
  console.log(`Found ${changedFiles.length} changed files (${sourceFiles.length} under a source directory)`);
  // Every changed source file becomes a ref; the resolver decides which are test files it can act on and
  // silently ignores the rest. No path-shape classification lives here - see mayBeTestSource.
  const changedRefs: FlakinessRef[] = sourceFiles.map((path) => ({ source: "changed-file", path }));

  console.log("Gathering unmuted refs...");
  const unmuteRefs = gatherUnmuteRefs(mergeBase, PROJECT_ROOT);
  console.log(`Found ${unmuteRefs.length} unmuted refs`);

  const refs: FlakinessRef[] = [...changedRefs, ...unmuteRefs];

  if (refs.length === 0) {
    console.log("No changed source files or unmutes detected; not starting the resolver");
    if (process.env.CI) {
      try {
        execSync(
          `buildkite-agent annotate "No test changes or unmutes detected" --style "info" --context "flakiness-detection"`,
          { cwd: PROJECT_ROOT, stdio: "inherit" }
        );
      } catch {
        // Ignore annotation failures
      }
    }
    process.exit(0);
  }

  const refsFile: FlakinessRefsFile = { mergeBase, refs };
  writeFileSync(resolve(PROJECT_ROOT, REFS_FILE), JSON.stringify(refsFile, null, 2));
  console.log(`Wrote ${refs.length} refs (${changedRefs.length} changed, ${unmuteRefs.length} unmuted) to ${REFS_FILE}`);

  // Hand off to the Java resolver + generate steps.
  uploadResolvePipeline(DEFAULT_AGENT_CONFIG);
}

if (import.meta.main) run();
