# CI test parts

`checkPart1..N` split `check` across CI jobs. The `elasticsearch.ci-parts` plugin
(`build-tools-internal/.../ci/CiPartsPlugin.java`) reads `assignment.json` to decide which part
runs each test task, and which part runs the rest of a project's `check` (precommit and other
non-test verification). Running every part runs exactly what `check` runs.

| File | Edited by | Content |
|---|---|---|
| `config.json` | hand | part count, effective workers per job, task groups that must not share a part, `runFirstSeconds` (test tasks at least this long in their part start before the part's other work), `heavyTestLimit` (cluster test tasks a part runs at once, see below), `shardOverheadSeconds` (the fixed cost every shard of a split task pays again, where `durations.json` has no measured `overheads` entry), and how to split long test tasks: `shardTargetSeconds` (longest a shard should take, fixed cost included), `maxShards`, and optional `pinnedSplits` (tasks always split as given) and `neverSplit` |
| `durations.json` | export script | measured only: median executed duration (s) of each test task, of each project's non-test tasks (without compilation and jars, which every part running its tests pays), the fixed cost of one execution of a test task (`overheads`), and for long test tasks the duration of each test class or YAML file they could be split by (`items`) |
| `assignment.json` | `./gradlew generateCiPartAssignment` | the generated assignment, including which test tasks are split and into how many shards; do not edit |

The planner splits a test task with `items` into the fewest shards whose longest one fits `shardTargetSeconds`,
estimating each shard from its items with the same hash the build uses to choose them, so one long class or
YAML file is never spread thin, and no shard is left without items. A task that fits as a whole is not split.

Test tasks or projects missing from `assignment.json` (for example ones added since it was generated)
go to a part chosen by a stable hash of their path, so the assignment only needs regenerating to
keep the parts balanced, never to keep them correct.

When the part count changes, every pipeline that runs `checkPartN` must list all parts.

When part tasks are requested, test tasks that start clusters (REST, cluster and spec tests) share a
throttle, by default half the Gradle workers (8 on PR agents); `-Dtests.ci.maxParallelHeavyTests=N`
overrides it and 0 turns it off. Keep `heavyTestLimit` in `config.json` in line with it.

## bcUpgradeTest parts

`bc-upgrade/` holds the same three files for `bcUpgradeTestPart1..N`, which split the `bcUpgradeTest`
tasks of all projects across the `pr-upgrade` and `bc-upgrade` jobs (`.buildkite/scripts/run-pr-upgrade-tests.sh`,
`run-bc-upgrade-tests.sh`). The long suites are split by test class; each upgrade test class starts and
upgrades its own cluster, so a class never spans shards. Durations come from `pr-upgrade-part-*` jobs, and the
planner's per-part estimate excludes the fixed cost of each job (about 5 minutes, mostly building the base
version from source). Regenerate with `./gradlew generateBcUpgradeTestPartAssignment`.

## ES|QL release test parts

`esql-release/` holds the same three files for `releaseTestCheckEsqlPart1..N`, the ES|QL release test jobs
(`.buildkite/pipelines/pull-request/esql-release-tests.yml`, release builds run on ES|QL-labelled PRs). They split
`esqlReleaseTestCheck`, which the root `build.gradle` registers on the projects those jobs cover. Durations come
from those jobs, because release builds turn feature flags off and test times differ from snapshot builds.
Regenerate with `./gradlew generateEsqlReleaseTestPartAssignment`.

## BWC test parts

`bwc/` holds the same three files for `v<version>#bwcTestPart1..N`, which split `v<version>#bwcTest` across the
`bwc-snapshots` jobs of the pull request and intake pipelines (one job per part and BWC version). One assignment serves
every version: its keys name the version `{V}`, as in `:qa:rolling-upgrade:v{V}#bwcTest`, and the exporter keeps the
slowest version's duration of each task. The long upgrade suites are split by test class; each upgrade test class
starts its own cluster, so a class never spans shards. Some BWC test tasks depend on another test task (rolling
upgrade's `bwcTest` on its `yamlBwcTest`); the plugin places those on their own and skips them in the other parts.
The jobs run with the configuration cache, which the planner assumes (a project's test tasks run in parallel).
Regenerate with `./gradlew generateBwcTestPartAssignment`.

## Refreshing

1. Export durations from the CI stats cluster with a read-only key for its `gradle-tasks` and `gradle-tests` indices:
   ```bash
   CI_STATS_URL=... CI_STATS_API_KEY=... node .buildkite/scripts/ci-parts/export.ts --suite check
   ```
   `--suite` is `check`, `bc-upgrade`, `esql-release` or `bwc`. By default it reads the last 14 days of the suite's
   pull request jobs (`--days`). When main doesn't run the part layout being planned yet, `--builds` reads
   only the given builds instead, for example cold builds of the pull request that changes the layout (ones
   that change build logic, so no test or precommit task comes from the cache).
2. Run `./gradlew generateCiPartAssignment` (or `generateBcUpgradeTestPartAssignment`,
   `generateEsqlReleaseTestPartAssignment`, `generateBwcTestPartAssignment`); it prints the planner's per-part estimate.
3. Review the diffs of `durations.json` and `assignment.json`. A test task that a mute makes short looks
   cheap and stops being split; pin it in `pinnedSplits` until it's unmuted.
