---
id: gradle-build-logic
description: Reviews Elasticsearch Gradle plugins, tasks, convention plugins, and build-logic changes for build-cache, configuration-cache, isolated-projects, and test coverage requirements
apply_to:
  - "build-conventions/**"
  - "build-tools/**"
  - "build-tools-internal/**"
  - "**/*.gradle"
  - "**/*.gradle.kts"
  - "settings.gradle"
  - "settings.gradle.kts"
can_block: true
---

Review changed Elasticsearch build logic and Gradle build scripts as a Delivery/Build reviewer. The rules below describe important review checks; many are enforced by `build-tools-internal` ArchUnit specs or Gradle `validatePlugins`.

## Scope and severity

- Apply these checks to newly added or modified build logic and Gradle build-script behavior. Do not ask authors to migrate unrelated pre-existing patterns in files they happened to touch.
- Use Libra's normal severity rubric. Reserve blocking findings for concrete correctness, CI, build-cache, configuration-cache, isolated-projects, compatibility, or maintainability risks.
- For Gradle build scripts outside `build-tools*`, distinguish new violations from existing local conventions. Suggest broader migrations only when the PR itself introduces the migration or depends on it.

## Layering and placement

- Elasticsearch uses included builds layered as `build-conventions` -> `build-tools` -> `build-tools-internal`.
- Default new Elasticsearch-only build logic to `build-tools-internal`.
- Put code in `build-tools` only when it must be part of the published third-party plugin API (`org.elasticsearch.gradle:build-tools`). Public APIs carry compatibility and deprecation-cycle obligations.
- The dependency direction must stay one-way: higher layers may depend on lower layers, not the reverse.
- Production plugins and tasks should be written in Java. Integration tests are Groovy + Spock. New build scripts should use the plugin DSL (`plugins { id '...' }`) where the surrounding build supports it; do not flag existing `apply plugin:` usage unless the PR adds or depends on it.
- Do not add ad-hoc repositories in subprojects; repository setup is centralized in `RepositoriesSetupPlugin`.

## Lazy configuration and task avoidance

- Register tasks with `tasks.register(name, Type.class, ...)`; do not use `tasks.create(...)` or `project.task(...)`.
- Register and configure domain objects lazily. Do not resolve providers or configurations at configuration time unless the value is truly needed then.
- Untyped lifecycle tasks are acceptable only while they remain simple aggregators. If a task needs `doFirst` or `doLast`, prefer a dedicated typed task class.
- Do not use `Project.afterEvaluate`; model dependencies with providers, lazy properties, `configureEach`, configurations, or build services.

## Task modeling and build cache correctness

- Task types must be `abstract` and expose all inputs/outputs as managed getter properties (`Property<T>`, `RegularFileProperty`, `DirectoryProperty`, `ConfigurableFileCollection`, etc.).
- Every task property getter needs the correct Gradle input/output annotation (`@Input`, `@InputFiles`, `@Classpath`, `@OutputFile`, `@Nested`, `@Optional`, etc.).
- Cacheable tasks should use `@CacheableTask` only when outputs are a pure function of declared inputs.
- File and directory inputs need stable normalization: use `@PathSensitive(PathSensitivity.RELATIVE)` unless `@Classpath` is appropriate or the code has a concrete reason for `NAME_ONLY`/`NONE`.
- Task work should happen in a single `@TaskAction` and read only from declared properties and injected services.

## Configuration-cache and isolated-projects hygiene

- Task classes must not call `getProject()` and must not hold `Project`, `Gradle`, or `Configuration` fields.
- Use injected Gradle services (`ObjectFactory`, `ProjectLayout`, `ExecOperations`, `FileSystemOperations`, `WorkerExecutor`, etc.) instead of captured project state.
- Do not read files, execute processes, or register ad-hoc build listeners during configuration.
- Model shared mutable state with `BuildService` and `usesService`, not static fields or captured mutable state.
- Production build logic must not cross project boundaries at configuration time. Watch for forbidden `Project` calls such as `getRootProject()`, `getRootDir()`, `getAllprojects()`, `getSubprojects()`, `allprojects()`, `subprojects()`, `getParent()`, `evaluationDependsOn()`, and `getProperties()`.
- Do not resolve cross-project tasks with `TaskContainer.findByPath()` or `getByPath()`. Use configurations/artifacts, extensions, or build services to model relationships.
- Compiled plugin code should not hard-code Gradle project path literals such as `:test:framework`; pass relationships through extension/configuration inputs instead.

## Gradle API, plugins, naming, and logging

- Do not import `org.gradle...internal...` packages. Avoid deprecated Gradle APIs; the build treats deprecations as failures.
- Do not introduce new usage of deprecated Elasticsearch build plugins such as `elasticsearch.testclusters`, `elasticsearch.internal-testclusters`, or legacy REST test plugins. Prefer JUnit-rule-based `ElasticsearchCluster` and current `internal-*` REST test infrastructure.
- A concrete top-level class named `*Plugin` must implement `Plugin`, and every `Plugin` implementation must be named `*Plugin`.
- Log through Gradle logging (`org.gradle.api.logging.Logging.getLogger(...)`). Do not use `System.out`, `System.err`, or `Throwable#printStackTrace()`.

## Tests and validation expectations

- New or modified plugin/task behavior needs matching tests. Do not accept additions to shrinking ArchUnit `KNOWN_*` baselines as a substitute for tests.
- Every new plugin needs either a ProjectBuilder-backed unit spec extending `AbstractProjectBuilderPluginSpec` or a TestKit functional test extending `AbstractGradleInternalPluginFuncTest`, depending on whether the behavior is configuration-time wiring or execution output.
- Every new task needs a unit test or functional test. Prefer functional tests when asserting task execution, produced files, or other runtime effects.
- Do not call `disableConfigurationCache(...)` in a functional test to hide a configuration-cache failure; fix the incompatibility.
- Build-logic dependency changes need version catalog wiring, SHA-256 dependency verification metadata, and explicit transitive dependency control through the repository's component metadata rules.
- Relevant validation commands include `:build-tools-internal:archunitTest`, `:build-tools-internal:test`, `:build-tools-internal:validatePlugins`, `:build-tools-internal:integTest`, and, for public build-tools changes, `:build-tools:check`.
- Do not report missing local validation evidence by itself. Report validation concerns only when the PR changes CI/build-job wiring, disables checks, changes test coverage requirements, or introduces code that is very likely to fail the relevant validation tasks.
