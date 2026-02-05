# Elasticsearch

## Toolchain Snapshot
- **Java**: JDK 25 via `JAVA_HOME`; use the bundled Gradle wrapper (`./gradlew`).
- **Build tooling**: Gradle composite build with `build-conventions`, `build-tools`, and `build-tools-internal`; Docker is required for some packaging/tests.
- **OS packages**: Packaging and QA jobs expect ephemeral hosts; do not run packaging suites on your workstation.
- **Security**: Default dev clusters enable security; use `elastic-admin:elastic-password` or disable with `-Dtests.es.xpack.security.enabled=false`.
- **Cursor/Copilot rules**: None provided in repo; follow this guide plus CONTRIBUTING.md.

## Build & Run Commands
- Refer to CONTRIBUTING.md & TESTING.asciidoc for comprehensive build/test instructions.

## Verification & Lint Tasks
- `./gradlew spotlessJavaCheck` / `spotlessApply` (or `:server:spotlessJavaCheck`): enforce the Eclipse formatter profile in `build-conventions/formatterConfig.xml`.

## Testing Cheatsheet
- Standard suite: `./gradlew test` (respects cached results; add `-Dtests.timestamp=$(date +%s)` to bypass caches when reusing seeds).
- Single project: `./gradlew :server:test` (or other subproject path).
- Single class: `./gradlew :server:test --tests org.elasticsearch.package.ClassName`.
- Single package: `./gradlew :server:test --tests 'org.elasticsearch.package.*'`.
- Single method / repeated runs: `./gradlew :server:test --tests org.elasticsearch.package.ClassName.methodName -Dtests.iters=N`.
- Deterministic seed: append `-Dtests.seed=DEADBEEF` (each method uses derived seeds).
- JVM tuning knobs: `-Dtests.jvms=8`, `-Dtests.heap.size=4G`, `-Dtests.jvm.argline="-verbose:gc"`, `-Dtests.output=always`, etc.
- Debugging: append `--debug-jvm` to the Gradle test task and attach a debugger on port 5005.
- CI reproductions: copy the `REPRODUCE WITH` line from CI logs; it includes project path, seed, and JVM flags.
- Yaml REST tests: `./gradlew ":rest-api-spec:yamlRestTest" --tests "org.elasticsearch.test.rest.ClientYamlTestSuiteIT.test {yaml=<relative_test_file_path>}"`

## Dependency Hygiene
- Never add a dependency without checking for existing alternatives in the repo.

## Formatting & Imports
- Run Spotless to auto-format; Java indent is 4 spaces, max width 140 columns.
- Absolutely no wildcard imports; keep existing import order and avoid reordering untouched lines.

## Types, Generics, and Suppressions
- Prefer type-safe constructs; avoid raw types and unchecked casts.
- If suppressing warnings, scope `@SuppressWarnings` narrowly (ideally a single statement or method).
- Document non-obvious casts or type assumptions via Javadoc/comments for reviewers.

## Naming Conventions
- REST handlers typically use the `Rest*Action` pattern; transport-layer handlers mirror them with `Transport*Action` classes.
- REST classes expose routes via `RestHandler#routes`; when adding endpoints ensure naming matches existing REST/Transport patterns to aid discoverability.
- Transport `ActionType` strings encode scope (`indices:data/read/...`, `cluster:admin/...`, etc.); align new names with these conventions to integrate with privilege resolution.

## Logging & Error Handling
- Elasticsearch should prefer its own logger `org.elasticsearch.logging.LogManager` & `org.elasticsearch.logging.Logger`; declare `private static final Logger logger = LogManager.getLogger(Class.class)`.
- Always use parameterized logging (`logger.debug("operation [{}]", value)`); never build strings via concatenation.
- Wrap expensive log-message construction in `() -> Strings.format(...)` suppliers when logging at `TRACE`/`DEBUG` to avoid unnecessary work.
- Log levels:
  - `TRACE`: highly verbose developer diagnostics; usually read alongside code.
  - `DEBUG`: detailed production troubleshooting; ensure volume is bounded.
  - `INFO`: default-enabled operational milestones; prefer factual language.
  - `WARN`: actionable problems users must investigate; include context and, if needed, exception stack traces.
  - `ERROR`: reserve for unrecoverable states (e.g., storage health failures); prefer `WARN` otherwise.
- Only log client-caused exceptions when the cluster admin can act on them; otherwise rely on API responses.
- Tests can assert logging via `MockLog` for complex flows.

## Javadoc & Comments
- New packages/classes/public or abstract methods require Javadoc explaining the "why" rather than the implementation details.
- Avoid documenting trivial getters/setters; focus on behavior, preconditions, or surprises.
- Use `@link` for inline references; highlight performance caveats or contract obligations directly in docs.
- For tests, Javadoc can describe scenario setup/expectations to aid future contributors.

## License Headers
- Default header (outside `x-pack`): Elastic License 2.0, SSPL v1, or AGPL v3—they are already codified at the top of Java files; copy from existing sources.
- Files under `x-pack` require the Elastic License 2.0-only header; IDEs configured per CONTRIBUTING.md can insert correct text automatically.

## Runtime & Cluster Debugging
- Start Elasticsearch via `./gradlew run --debug-jvm` and attach IntelliJ's "Debug Elasticsearch" configuration (auto-generated per CONTRIBUTING.md).
- To customize the dev cluster without touching tracked files, supply an init script (`./gradlew run -I ~/custom.gradle ...`) that registers extra test-cluster config (e.g., custom certs, node roles, additional nodes).
- Multi-node debug mode: each node consumes incremental debug ports (5007, 5008, ...); configure IDE listeners with auto-restart.
- Cross-cluster search manual testing: `./gradlew run-ccs` brings up querying and fulfilling clusters; send requests like `my_remote_cluster:*/_search`.

## Testing Parameters Reference
- Persistent data directories: add `--data-dir=/tmp/foo --preserve-data` to `run` if you need state between restarts.
- Heap overrides: `-Dtests.heap.size=512m` for tests; `-Dtests.heap.size=4G` for runtime nodes when reproducing memory issues.
- JVM args: `-Dtests.jvm.argline="-XX:HeapDumpPath=/tmp/heap`"; combine multiple args inside the quoted string.
- License toggles: `-Drun.license_type=trial` enables full-stack features plus superuser credentials.

## Best Practices for Automation Agents
- Never edit unrelated files; keep diffs tightly scoped to the task at hand.
- Prefer Gradle tasks over ad-hoc scripts.
- When scripting CLI sequences, leverage `gradlew` task.
- Unrecognized changes: assume other agent; keep going; focus your changes. If it causes issues, stop + ask user.

Stay aligned with `CONTRIBUTING.md`, `BUILDING.md`, and `TESTING.asciidoc`; this AGENTS guide summarizes—but does not replace—those authoritative docs.
