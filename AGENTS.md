# AGENTS GUIDE

This file briefs agentic contributors on how to build, test, lint, and extend the Elasticsearch repo. Keep it handy while scripting automated workflows.

## Toolchain Snapshot
- **Java**: JDK 21 via `JAVA_HOME`; use the bundled Gradle wrapper (`./gradlew`).
- **Build tooling**: Gradle composite build with `build-conventions`, `build-tools`, and `build-tools-internal`; Docker is required for some packaging/tests.
- **OS packages**: Packaging and QA jobs expect ephemeral hosts; do not run packaging suites on your workstation.
- **Security**: Default dev clusters enable security; use `elastic-admin:elastic-password` or disable with `-Dtests.es.xpack.security.enabled=false`.
- **Cursor/Copilot rules**: None provided in repo; follow this guide plus CONTRIBUTING.md.

## Build & Run Commands
- `./gradlew assemble`: full artifact build (requires Docker); avoid unless CI parity needed.
- `./gradlew localDistro`: fastest way to build the current platform distro.
- `./gradlew :distribution:archives:<platform>:assemble`: targeted packages (e.g., `linux-tar`, `darwin-tar`, `windows-zip`).
- `./gradlew run [-Drun.distribution=oss] [-Drun.license_type=trial] [--debug-jvm]`: boot dev cluster directly from source; combine with `--debug-cli-jvm` to debug launcher.
- `./gradlew run --https` or `--with-apm-server`: enable optional fixtures; use init scripts (`-I`) to avoid committing local tweaks.
- `./gradlew run-ccs`: launches two clusters wired for cross-cluster search testing.
- `./gradlew :distribution:packages:<platform>:assemble`: build system packages (deb/rpm) when verifying installer changes.
- `./gradlew -p docs check`: doc-only verification; use `-Dtests.output=always` when iterating on doc snippets.

## Verification & Lint Tasks
- `./gradlew precommit`: baseline static analysis (Spotless, forbidden APIs, etc.); run before every PR.
- `./gradlew check`: superset verification (unit, integ, precommit); required before merging.
- `./gradlew internalClusterTest`: run only in-memory cluster integration tests while debugging.
- `./gradlew spotlessJavaCheck` / `spotlessApply` (or `:server:spotlessJavaCheck`): enforce the Eclipse formatter profile in `build-conventions/formatterConfig.xml`.
- `./gradlew configureIdeCheckstyle -Didea.active=true`: regenerates IntelliJ Checkstyle config when missing.

### Project Structure
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

## Packaging & QA
- Packaging tests live under `qa/packaging`; only run on ephemeral machines because they install/uninstall system packages.
- Use `assumeTrue(distribution.packaging.compatible);` or `Platforms` helpers to gate platform-specific logic.

## Dependency Hygiene
- All third-party artifacts must appear in `gradle/verification-metadata.xml`; run `./gradlew --write-verification-metadata sha256 precommit` after adding/updating dependencies.
- Never add a dependency without checking for existing alternatives in the repo.

## Formatting & Imports
- Run Spotless to auto-format; Java indent is 4 spaces, max width 140 columns.
- Doc-snippet regions marked with `// tag::NAME` / `// end::NAME` must stay ≤76 characters; Spotless will not reflow them.
- Absolutely no wildcard imports; keep existing import order and avoid reordering untouched lines.
- Use `// tag::noformat` sparingly; only when benefit outweighs reduced consistency.
- Negative boolean checks should read `foo == false` (Checkstyle-enforced); avoid `!foo` unless positive form is impossible.

## Types, Generics, and Suppressions
- Prefer type-safe constructs; avoid raw types and unchecked casts.
- If suppressing warnings, scope `@SuppressWarnings` narrowly (ideally a single statement or method).
- Use `Types.forciblyCast` only when the compiler cannot reason about types and there is no safe alternative.
- Document non-obvious casts or type assumptions via Javadoc/comments for reviewers.

## Naming Conventions
- REST handlers typically use the `Rest*Action` pattern; transport-layer handlers mirror them with `Transport*Action` classes.
- REST classes expose routes via `RestHandler#routes`; when adding endpoints ensure naming matches existing REST/Transport patterns to aid discoverability.
- Transport `ActionType` strings encode scope (`indices:data/read/...`, `cluster:admin/...`, etc.); align new names with these conventions to integrate with privilege resolution.
- Packaging/Naming docs (e.g., `modules/apm/NAMING.md`) describe domain-specific metric/attribute naming; consult relevant module docs before inventing new identifiers.

## Logging & Error Handling
- Elasticsearch uses Log4j; declare `private static final Logger logger = LogManager.getLogger(Class.class);`.
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
- Use `@link` over `@see` for inline references; highlight performance caveats or contract obligations directly in docs.
- For tests, Javadoc can describe scenario setup/expectations to aid future contributors.

## License Headers
- Default header (outside `x-pack`): Elastic License 2.0, SSPL v1, or AGPL v3—they are already codified at the top of Java files; copy from existing sources.
- Files under `x-pack` require the Elastic License 2.0-only header; IDEs configured per CONTRIBUTING.md can insert correct text automatically.

## IntelliJ & IDE Tips
- Import project via root `build.gradle`; ensure the JDK named `21` is available so Gradle toolchain detection works.
- Configure Eclipse Code Formatter plugin with `build-conventions/formatterConfig.xml` and uncheck "Optimize Imports".
- Enable Actions on Save → Reformat Code for Java to match Spotless output.
- Regenerate IDE Checkstyle config with `./gradlew -Didea.active=true configureIdeCheckstyle` if IntelliJ complains about missing files.

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
- HTTPS dev clusters: `./gradlew run --https` plus init-script-provided certs from `testClusters.register(...)` or extra config via the `-I` approach.

## Pull Request Checklist (Automation Friendly)
- Rebase on `main`; avoid force-push after opening the PR unless reviewers request a rebase.
- Run `./gradlew check` (or `./gradlew -p docs check` for doc-only changes) before requesting review.
- Ensure new dependencies have verification metadata and that `.backportrc.json` entries still match `buildParams.bwcVersions` (run `./gradlew verifyVersions` when touching release files).
- Confirm tests relevant to your change pass (unit, integration, QA) and document any intentionally skipped suites in the PR description.
- Use the PR template prompts (CLA, supported OS/arch, etc.) to avoid review delays.
- Document reproductions for CI flakes (copy/paste the `REPRODUCE WITH` line) inside PR comments when requesting re-runs.
- Keep branches small; multi-feature branches complicate Buildkite matrix selection and backports.
- Avoid committing generated artifacts (`build/`, `node_modules/`, distro zips); rely on `.gitignore` but double-check before staging.

## Best Practices for Automation Agents
- Never edit unrelated files; keep diffs tightly scoped to the task at hand.
- When touching tests, add or update corresponding documentation snippets if they are referenced via `// tag::` markers; docs are built from source files.
- Prefer Gradle tasks over ad-hoc scripts.
- When scripting CLI sequences, leverage `gradlew` task.
- For long-running commands, note that CI sometimes toggles bwc tests via `checkPart*` tasks; follow the pattern defined in `build.gradle` if you add new check partitions.
- Maintain deterministic behavior: pass explicit seeds (`-Dtests.seed`), disable caches (`-Dtests.timestamp`) when comparing outputs, and log node roles/settings changes in PR descriptions.
- Buildkite pipelines are sensitive to extra env vars; prefer Gradle system props (e.g., `-Dtests.es.logger.level=TRACE`) over exported env toggles.
- Use `./gradlew dependencies --configuration <name>` to inspect dependency graphs before adding new artifacts; attach the output to PRs when the change is controversial.

Stay aligned with `CONTRIBUTING.md`, `BUILDING.md`, and `TESTING.asciidoc`; this AGENTS guide summarizes—but does not replace—those authoritative docs.
