import { execSync } from "child_process";
import { readFileSync, writeFileSync } from "fs";
import { resolve } from "path";

import { classifyChangedFiles } from "../detectors/changed-files.ts";
import { partitionByBwc, defaultBuildScriptReader } from "../detectors/bwc.ts";
import { findUnmutedTests, type UnmuteDetectionResult } from "../detectors/unmutes.ts";
import { buildCommands, dedupeTests } from "../commands.ts";
import { uploadBuildkitePipeline } from "../runners/buildkite.ts";
import { DEFAULT_AGENT_CONFIG, DEFAULT_BATCHING_CONFIG, type ClassifiedTest } from "../domain.ts";

// Bootstrap-written list of tests that cannot be re-run (BWC projects). The
// analyze step downloads and folds it into the outcomes as `not_applicable`.
// Keep in sync with FLAKINESS_SKIPPED_ARTIFACT in runners/buildkite.ts.
const SKIPPED_FILE = "flakiness-skipped.json";

function describeTest(t: ClassifiedTest): string {
  const target = t.yamlTest ? `${t.fqcn}.${t.yamlTest}` : (t.fqcn ?? t.suitePath ?? "(whole source set)");
  return `${t.gradleProject} [${t.kind}] ${target}`;
}

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

function detectUnmutedTests(mergeBase: string, projectRoot: string): UnmuteDetectionResult {
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

  console.log("  Listing tracked files...");
  const repoFilesOutput = execSync("git ls-files", {
    cwd: projectRoot,
    stdio: ["ignore", "pipe", "pipe"],
    timeout: GIT_COMMAND_TIMEOUT_MS,
    maxBuffer: 256 * 1024 * 1024,
    encoding: "utf8",
  });
  const repoFiles = repoFilesOutput
    .split("\n")
    .map((f) => f.trim())
    .filter((f) => f !== "");
  console.log(`  Indexed ${repoFiles.length} tracked files`);

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

  // BWC qa projects disable the bare test task, so the detector's plain
  // `:project:task` re-runs nothing (0 tests, exit 0). Split those out so they
  // are recorded as `not_applicable` instead of wasting jobs and polluting the
  // metric as `hang`s. Running them properly (version-qualified `v<ver>#bwcTest`)
  // is a separate follow-up.
  const { runnable, notApplicable } = partitionByBwc(tests, defaultBuildScriptReader(PROJECT_ROOT));
  if (notApplicable.length > 0) {
    console.log(`Skipping ${notApplicable.length} BWC test(s) - not re-runnable via the bare task (recorded as not_applicable):`);
    for (const t of notApplicable) {
      console.log(`  - ${describeTest(t)}`);
    }
    if (process.env.CI) {
      try {
        writeFileSync(resolve(PROJECT_ROOT, SKIPPED_FILE), JSON.stringify(notApplicable));
      } catch (err) {
        console.error(`Failed to write ${SKIPPED_FILE}:`, err);
      }
    }
  }

  if (runnable.length > 30) {
    console.log(`Warning: ${runnable.length} test files to re-run`);
    if (process.env.CI) {
      try {
        execSync(
          `buildkite-agent annotate "Warning: ${runnable.length} test files to re-run (${changedTests.length} changed, ${unmuted.located.length} unmuted). This may take a while." --style "warning" --context "flakiness-detection"`,
          { cwd: PROJECT_ROOT, stdio: "inherit" }
        );
      } catch {
        // Ignore annotation failures
      }
    }
  }

  uploadBuildkitePipeline(
    buildCommands(runnable, DEFAULT_BATCHING_CONFIG),
    DEFAULT_AGENT_CONFIG,
    { hasNotApplicable: notApplicable.length > 0 }
  );
}

if (import.meta.main) run();
