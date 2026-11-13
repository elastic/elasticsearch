# CI test parts

`checkPart1..N` split `check` across CI jobs. The `elasticsearch.ci-parts` plugin
(`build-tools-internal/.../ci/CiPartsPlugin.java`) reads `assignment.json` to decide which part
runs each test task, and which part runs the rest of a project's `check` (precommit and other
non-test verification). Running every part runs exactly what `check` runs.

| File | Edited by | Content |
|---|---|---|
| `config.json` | hand | part count, effective workers per job, test tasks to split, task groups that must not share a part, `runFirstSeconds` (test tasks at least this long in their part start before the part's other work), `heavyTestLimit` (cluster test tasks a part runs at once, see below) and `shardOverheadSeconds` (the fixed cost every shard of a split task pays again, where `durations.json` has no measured `overheads` entry) |
| `durations.json` | export script | median executed duration (s) of each test task, and of each project's non-test tasks (without compilation and jars, which every part running its tests pays), in PR `part-*` jobs |
| `assignment.json` | `./gradlew generateCiPartAssignment` | the generated assignment; do not edit |

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

## Refreshing

1. Export durations from the CI stats cluster (median executed duration of each Gradle task in
   `elasticsearch-pull-request` `part-*` jobs over the last 7 days) into `durations.json`. Durations
   from a cold build (one that changes build logic, so no test or precommit task comes from the cache)
   balance the parts for the slowest case; there, precommit tasks take much longer than usual because
   they compete with the tests for CPU.
2. Run `./gradlew generateCiPartAssignment` (or `generateBcUpgradeTestPartAssignment`,
   `generateEsqlReleaseTestPartAssignment`); it prints the
   planner's per-part estimate.
3. Review the diff of `assignment.json`.
