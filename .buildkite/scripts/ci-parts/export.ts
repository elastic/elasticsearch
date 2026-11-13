import { readFileSync, writeFileSync } from "fs";
import { resolve } from "path";
import { parseArgs } from "util";

import {
  buildDurations,
  formatDurations,
  isYamlSuite,
  splitCandidates,
  testSeconds,
  versionedExecutions,
  versionedItemRows,
  versionedItemSums,
  versionedTaskRows,
  versionKey,
  type ItemRow,
  type PlannerConfig,
  type ProjectWork,
} from "./durations.ts";
import { executionRows, fetchClient, itemRows, jobItemSums, taskRows, type JobSelection } from "./stats.ts";

// Writes a suite's .ci/test-parts durations.json from the last --days of its pull request CI jobs, for the CI part
// planner to turn into an assignment. Needs CI_STATS_URL and CI_STATS_API_KEY (a read-only key for the CI stats
// cluster's gradle-tasks and gradle-tests indices).
//
//   node .buildkite/scripts/ci-parts/export.ts --suite check|bc-upgrade|esql-release|bwc [--days 14] [--builds 197107,197162]
//
// --builds reads only those pull request builds (within --days), for when main doesn't run the part layout being
// planned yet, for example from builds of a pull request that changes it.
//
// Then regenerate the assignment: ./gradlew generateCiPartAssignment (generateBcUpgradeTestPartAssignment,
// generateEsqlReleaseTestPartAssignment, generateBwcTestPartAssignment).

interface Suite {
  dir: string;
  jobName: string;
  projectWork: ProjectWork;
  /** One assignment for every BWC version: task paths are keyed without their version (see durations.ts). */
  versioned?: boolean;
}

const SUITES: Record<string, Suite> = {
  // checkPartN in the pull request part jobs; arm, fips and windows variants run the same parts on other agents
  check: { dir: ".ci/test-parts", jobName: "part-[0-9]+", projectWork: "all" },
  // bcUpgradeTestPartN only run upgrade tests; the home part's assemble only happens in release builds
  "bc-upgrade": { dir: ".ci/test-parts/bc-upgrade", jobName: "pr-upgrade-part-[0-9]+", projectWork: "none" },
  // the ES|QL release test jobs ("releaseTestCheckEsqlPart1 / release-tests"; job names are stored as lowercase slugs)
  // also run tasks of projects outside the suite, which their parts do not own
  "esql-release": { dir: ".ci/test-parts/esql-release", jobName: "releasetestcheckesql.*_release-tests", projectWork: "withTests" },
  // v<version>#bwcTestPartN in the bwc-snapshots jobs ("9.1.0 / Part 2 / bwc-snapshots", stored as 9.1.0_part_2_bwc-snapshots),
  // one job per part and version; their non-test work is the shared build of the BWC distribution
  bwc: { dir: ".ci/test-parts/bwc", jobName: "[0-9.]+_part_[0-9]+_bwc-snapshots", projectWork: "none", versioned: true },
};

const PROJECT_ROOT = resolve(import.meta.dirname, "..", "..", "..");

const { values } = parseArgs({
  options: {
    suite: { type: "string", default: "check" },
    days: { type: "string", default: "14" },
    builds: { type: "string" },
  },
});

const suite = SUITES[values.suite];
if (!suite) {
  throw new Error(`unknown --suite ${values.suite}; one of ${Object.keys(SUITES).join(", ")}`);
}

const env = (name: string): string => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is not set`);
  }
  return value;
};

const dir = resolve(PROJECT_ROOT, suite.dir);
const config: PlannerConfig = JSON.parse(readFileSync(resolve(dir, "config.json"), "utf-8"));
const assignment: { splits?: Record<string, unknown> } = JSON.parse(readFileSync(resolve(dir, "assignment.json"), "utf-8"));
const currentSplits = new Set(Object.keys(assignment.splits ?? {}));
const builds = values.builds?.split(",").map((b) => b.trim()).filter((b) => b.length > 0);
const jobs: JobSelection = { jobName: suite.jobName, days: Number(values.days), ...(builds ? { builds } : {}) };
const client = fetchClient(env("CI_STATS_URL"), env("CI_STATS_API_KEY"));

const rawTasks = await taskRows(client, jobs);
const tasks = suite.versioned ? versionedTaskRows(rawTasks) : rawTasks;
const candidates = splitCandidates(testSeconds(tasks), config, currentSplits);
// the stats hold real task paths; for the BWC suite, a key stands for one path per version
const keyOf = suite.versioned ? versionKey : (path: string): string => path;
const pathsOf = (keys: string[]): string[] => {
  const wanted = new Set(keys);
  return [...new Set(rawTasks.map((t) => t.path))].filter((path) => wanted.has(keyOf(path)));
};
const foldItems = (rows: ItemRow[]): ItemRow[] => (suite.versioned ? versionedItemRows(rows) : rows);

// tasks with several classes split by class; a task with one class is a YAML suite if its tests are YAML tests
const classRows = foldItems(await itemRows(client, jobs, pathsOf(candidates), true));
const classesPerTask = new Map<string, number>();
for (const row of classRows) {
  classesPerTask.set(row.task, (classesPerTask.get(row.task) ?? 0) + 1);
}
const classTasks = candidates.filter((t) => (classesPerTask.get(t) ?? 0) > 1);
const singleClassTasks = candidates.filter((t) => classesPerTask.get(t) === 1);
const testRows = foldItems(await itemRows(client, jobs, pathsOf(singleClassTasks), false));
const yamlTasks = singleClassTasks.filter((t) => isYamlSuite(testRows.filter((r) => r.task === t)));

const [rawExecutions, rawClassSums, rawYamlSums] = await Promise.all([
  executionRows(client, jobs, pathsOf(candidates)),
  jobItemSums(client, jobs, pathsOf(classTasks), true),
  jobItemSums(client, jobs, pathsOf(yamlTasks), false),
]);
const executions = suite.versioned ? versionedExecutions(rawExecutions) : rawExecutions;
const classSums = suite.versioned ? versionedItemSums(rawClassSums) : rawClassSums;
const yamlSums = suite.versioned ? versionedItemSums(rawYamlSums) : rawYamlSums;
const items: ItemRow[] = [
  ...classRows.filter((r) => classTasks.includes(r.task)),
  ...testRows.filter((r) => yamlTasks.includes(r.task)),
];

const today = new Date().toISOString().substring(0, 10);
const durations = buildDurations({
  source:
    `median executed duration (s) in elasticsearch-pull-request jobs matching '${suite.jobName}' of PRs against main, ` +
    (builds ? `builds ${builds.join(", ")}` : `${jobs.days} days to ${today}`) +
    (suite.versioned ? ", the slowest BWC version's" : "") +
    `, retried jobs left out; exported by .buildkite/scripts/ci-parts/export.ts`,
  config,
  versioned: suite.versioned,
  projectWork: suite.projectWork,
  currentSplits,
  taskRows: tasks,
  executions,
  itemRows: items,
  itemSums: [...classSums, ...yamlSums],
});
const out = resolve(dir, "durations.json");
writeFileSync(out, formatDurations(durations));
console.log(
  `Wrote ${out}: ${Object.keys(durations.tests).length} test tasks, ${Object.keys(durations.projects).length} projects, ` +
    `items for ${Object.keys(durations.items).length} tasks, ${Object.keys(durations.overheads).length} measured fixed costs ` +
    `(from ${rawTasks.length} task paths, ${executions.length} executions of ${candidates.length} candidate tasks)`
);
