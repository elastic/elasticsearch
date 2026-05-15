import { execSync } from "child_process";
import { readFileSync } from "fs";
import { resolve } from "path";

import { classifyChangedFiles } from "../detectors/changed-files";
import { findUnmutedTests, type UnmuteDetectionResult } from "../detectors/unmutes";
import { buildCommands, dedupeTests } from "../commands";
import { uploadBuildkitePipeline } from "../runners/buildkite";
import { DEFAULT_AGENT_CONFIG, DEFAULT_BATCHING_CONFIG } from "../domain";

const PROJECT_ROOT = resolve(`${import.meta.dir}/../../../..`);

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
    run(`git fetch --no-tags origin ${targetBranch}`, { cwd: projectRoot, stdio: "inherit" });
    return "FETCH_HEAD";
  }
}

function detectUnmutedTests(mergeBase: string, projectRoot: string): UnmuteDetectionResult {
  let oldYaml = "";
  try {
    oldYaml = execSync(`git show ${mergeBase}:muted-tests.yml`, {
      cwd: projectRoot,
      stdio: ["ignore", "pipe", "ignore"],
    }).toString();
  } catch {
    // File didn't exist at merge base; treat as empty.
  }

  let newYaml = "";
  try {
    newYaml = readFileSync(resolve(projectRoot, "muted-tests.yml"), "utf8");
  } catch {
    // File was deleted in the PR; treat as empty.
  }

  const repoFilesOutput = execSync("git ls-files", {
    cwd: projectRoot,
    maxBuffer: 256 * 1024 * 1024,
  }).toString();
  const repoFiles = repoFilesOutput
    .split("\n")
    .map((f) => f.trim())
    .filter((f) => f !== "");

  return findUnmutedTests(oldYaml, newYaml, repoFiles);
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
  const changedFilesOutput = execSync(`git diff --diff-filter=d --name-only ${mergeBase}`, { cwd: PROJECT_ROOT }).toString().trim();
  const changedFiles = changedFilesOutput.split("\n").map((f) => f.trim()).filter((f) => f);
  console.log(`Found ${changedFiles.length} changed files`);

  const changedTests = classifyChangedFiles(changedFiles);
  console.log(`Found ${changedTests.length} changed test files`);

  console.log("Detecting unmuted tests...");
  const unmuted = detectUnmutedTests(mergeBase, PROJECT_ROOT);
  console.log(`Found ${unmuted.located.length} unmuted tests`);
  if (unmuted.unlocated.length > 0) {
    console.log(`Skipping ${unmuted.unlocated.length} unmuted tests whose class files no longer exist:`);
    for (const e of unmuted.unlocated) {
      console.log(`  - ${e.className}${e.method !== undefined ? "." + e.method : ""}`);
    }
  }

  const tests = dedupeTests([...changedTests, ...unmuted.located]);
  console.log(`Total tests to run: ${tests.length} (${changedTests.length} changed, ${unmuted.located.length} unmuted)`);

  if (tests.length === 0) {
    console.log("No test changes or unmutes detected");
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

  if (tests.length > 30) {
    console.log(`Warning: ${tests.length} test files to re-run`);
    if (process.env.CI) {
      try {
        execSync(
          `buildkite-agent annotate "Warning: ${tests.length} test files to re-run (${changedTests.length} changed, ${unmuted.located.length} unmuted). This may take a while." --style "warning" --context "flakiness-detection"`,
          { cwd: PROJECT_ROOT, stdio: "inherit" }
        );
      } catch {
        // Ignore annotation failures
      }
    }
  }

  uploadBuildkitePipeline(
    buildCommands(tests, DEFAULT_BATCHING_CONFIG),
    DEFAULT_AGENT_CONFIG
  );
}

if (import.meta.main) run();
