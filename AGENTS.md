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
- `./gradlew spotlessJavaCheck` / `spotlessApply` (or `:server:spotlessJavaCheck`): enforce formatter profile in `build-conventions/formatterConfig.xml`.

## Project Structure
The repository is organized into several key directories:
*   `server`: The core Elasticsearch server.
*   `modules`: Features shipped with Elasticsearch by default.
*   `plugins`: Officially supported plugins.
*   `libs`: Internal libraries used by other parts of the project.
*   `qa`: Integration and multi-version tests.
*   `docs`: Project documentation.
*   `distribution`: Logic for building distribution packages.
*   `x-pack`: Additional code modules and plugins under Elastic License.
*   `build-conventions`, `build-tools`, `build-tools-internal`: Gradle build logic.

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
- Use the Elasticsearch testing framework where possible for unit and yaml tests and be consistent in style with other elasticsearch tests.
- Use real classes over mocks or stubs for unit tests, unless the real class is complex then either a simplified subclass should be created within the test or, as a last resort, a mock or stub can be used. Unit tests must be as close to real-world scenarios as possible.
- Ensure mocks or stubs are well-documented and clearly indicate why they were necessary. 

### Test Types
- Unit Tests: Preferred. Extend `ESTestCase`.
- Single Node: Extend `ESSingleNodeTestCase` (lighter than full integ test).
- Integration: Extend `ESIntegTestCase`.
- REST API: Extend `ESRestTestCase` or `ESClientYamlSuiteTestCase`. **YAML based REST tests are preferred** for integration/API testing.

## Dependency Hygiene
- Never add a dependency without checking for existing alternatives in the repo.

## Formatting & Imports
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
- For tests, Javadoc can describe scenario setup/expectations to aid future contributors.
- Do not remove existing comments from code unless the code is also being removed or the comment has become incorrect.

## License Headers
- Default header (outside `x-pack`): Elastic License 2.0, SSPL v1, or AGPL v3—they are already codified at the top of Java files; copy from existing sources.
- Files under `x-pack` require the Elastic License 2.0-only header; IDEs configured per CONTRIBUTING.md can insert correct text automatically.

## Best Practices for Automation Agents
- Never edit unrelated files; keep diffs tightly scoped to the task at hand.
- Prefer Gradle tasks over ad-hoc scripts.
- When scripting CLI sequences, leverage `gradlew` task.
- Unrecognized changes: assume other agent; keep going; focus your changes. If it causes issues, stop + ask user.
- Do not add "Co-Authored-By" lines to commit messages. commit messages should adhere to the 50/72 rule: use a maximum of 50 columns for the commit summary

## Backwards compatibility
- For changes to a `Writeable` implementation (`writeTo` and constructor from `StreamInput`), add a new `public static final <UNIQUE_DESCRIPTIVE_NAME> = TransportVersion.fromName("<unique_descriptive_name>")` and use it in the new code paths. Confirm the backport branches and then generate a new version file with `./gradlew generateTransportVersion`.

Stay aligned with `CONTRIBUTING.md`, `BUILDING.md`, and `TESTING.asciidoc`; this AGENTS guide summarizes—but does not replace—those authoritative docs.
