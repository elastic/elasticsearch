# CI test parts

`checkPart1..N` split `check` across CI jobs. The `elasticsearch.ci-parts` plugin
(`build-tools-internal/.../ci/CiPartsPlugin.java`) reads `assignment.json` to decide which part
runs each test task, and which part runs the rest of a project's `check` (precommit and other
non-test verification). Running every part runs exactly what `check` runs.

| File | Edited by | Content |
|---|---|---|
| `config.json` | hand | part count, effective workers per job, test tasks to split, task groups that must not share a part |
| `durations.json` | export script | median executed duration (s) of each test task, and of each project's non-test tasks, in PR `part-*` jobs |
| `assignment.json` | `./gradlew generateCiPartAssignment` | the generated assignment; do not edit |

Test tasks or projects missing from `assignment.json` (for example ones added since it was generated)
go to a part chosen by a stable hash of their path, so the assignment only needs regenerating to
keep the parts balanced, never to keep them correct.

When the part count changes, every pipeline that runs `checkPartN` must list all parts.

## Refreshing

1. Export durations from the CI stats cluster (median executed duration of each Gradle task in
   `elasticsearch-pull-request` `part-*` jobs over the last 7 days) into `durations.json`.
2. Run `./gradlew generateCiPartAssignment`; it prints the planner's per-part estimate.
3. Review the diff of `assignment.json`.
