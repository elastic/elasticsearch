# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Elasticsearch is a distributed search and analytics engine built with Gradle on Java. The codebase consists of a minimal core with functionality delivered through a plugin/module architecture, separating open-source (OSS) and commercial (X-Pack) features under different licenses.

## Build Commands

### Building

```bash
# Build a distribution for your local OS
./gradlew localDistro

# Build platform-specific distributions
./gradlew :distribution:archives:linux-tar:assemble
./gradlew :distribution:archives:darwin-tar:assemble
./gradlew :distribution:archives:darwin-aarch64-tar:assemble
./gradlew :distribution:archives:windows-zip:assemble

# Build Docker images
./gradlew buildDockerImage
./gradlew buildAarch64DockerImage
```

### Running

```bash
# Run Elasticsearch from source
./gradlew run

# Run with remote debugging (port 5005)
./gradlew run --debug-jvm

# Debug the CLI launcher (port 5107+)
./gradlew run --debug-cli-jvm

# Run with different distribution (default is full, not OSS)
./gradlew run -Drun.distribution=oss
```

### Testing

```bash
# Run all tests for a specific project
./gradlew :server:test

# Run a single test class
./gradlew :server:test --tests org.elasticsearch.cluster.SnapshotsInProgressTests

# Run a single test method
./gradlew :server:test --tests "org.elasticsearch.cluster.SnapshotsInProgressTests.testClone"

# Run with specific seed for reproducibility
./gradlew :server:test --tests "..." -Dtests.seed=<seed>

# Run integration tests
./gradlew :server:internalClusterTest

# Run REST API tests
./gradlew :rest-api-spec:yamlRestTest

# Run all precommit checks (formatting, license headers, etc.)
./gradlew precommit

# Check for forbidden API usage
./gradlew forbiddenApis

# Run spotless to format code
./gradlew spotlessApply
```

### Dependency Management

```bash
# Update dependency verification metadata after adding/updating dependencies
./gradlew --write-verification-metadata sha256 precommit

# Resolve all dependencies (useful for verification)
./gradlew resolveAllDependencies
```

## Architecture Overview

### Directory Structure

- **server/** - Core Elasticsearch engine with minimal functionality
  - Primary packages: `action`, `cluster`, `discovery`, `index`, `search`, `ingest`, `gateway`, `transport`, `http`
  - Contains base `Plugin` class and core Module classes for dependency injection
  - Source structure: `src/main/java`, `src/test/java`, `src/internalClusterTest/java`, `src/javaRestTest/java`

- **modules/** - Built-in modules (32+) that ship with all distributions
  - Examples: `ingest-common`, `reindex`, `lang-painless`, `repository-s3`, `analysis-common`
  - Always loaded; cannot be disabled
  - Cannot contain bin/ directories or packaging files

- **plugins/** - Optional plugins (18+) that can be installed separately
  - Examples: `analysis-icu`, `discovery-ec2`, `repository-hdfs`
  - Include example plugins for developers

- **x-pack/** - Commercial features under Elastic License
  - 68+ plugins in `x-pack/plugin/` (security, ml, sql, monitoring, watcher, etc.)
  - Core x-pack utilities in `x-pack/plugin/core/`
  - Separate licensing and test infrastructure in `x-pack/qa/`

- **libs/** - Shared libraries (28+) used across the codebase
  - `core`, `x-content`, `geo`, `grok`, `plugin-api`, `plugin-analysis-api`, etc.
  - Published to Maven with "elasticsearch-" prefix

- **build-tools/** - Three-tier build system
  - `build-conventions/` - Checkstyle and basic conventions
  - `build-tools/` - Public build plugins for third-party plugin authors
  - `build-tools-internal/` - Internal Elasticsearch build logic

- **distribution/** - Packaging and distributions
  - `archives/` - TAR/ZIP for Linux, macOS, Windows, ARM64
  - `docker/` - Docker image variants (standard, Cloud ESS, Ironbank, Wolfi)
  - `packages/` - DEB and RPM packages
  - `bwc/` - Backward compatibility testing infrastructure

- **test/** - Shared test infrastructure
  - `framework/` - Core test utilities, base test classes
  - `fixtures/` - Test fixtures for external services (AWS, Azure, GCS)

- **qa/** - Cross-cutting integration tests (34+ suites)
  - Upgrade tests: `rolling-upgrade`, `full-cluster-restart`, `mixed-cluster`
  - Smoke tests, cross-cluster search tests, packaging tests

### Plugin System Architecture

The plugin system is the primary extensibility mechanism:

1. **Base Plugin Class** - All plugins extend `org.elasticsearch.plugins.Plugin`

2. **Specialized Plugin Interfaces** - Plugins implement one or more:
   - `ActionPlugin` - Add custom REST/transport actions
   - `AnalysisPlugin` - Add analyzers, tokenizers, filters
   - `ClusterPlugin` - Add cluster-level functionality
   - `DiscoveryPlugin` - Add node discovery mechanisms
   - `EnginePlugin` - Customize indexing/search engine behavior
   - `IndexStorePlugin` - Customize index storage
   - `IngestPlugin` - Add ingest processors
   - `MapperPlugin` - Add custom field mappers
   - `NetworkPlugin` - Customize network layer
   - `RepositoryPlugin` - Add snapshot repository types
   - `ScriptPlugin` - Add scripting languages
   - `SearchPlugin` - Add aggregations, queries, scoring functions

3. **Module vs Plugin vs X-Pack Plugin**:
   - **Modules**: Built-in, always loaded, minimal structure, in `modules/`
   - **Plugins**: Optional, installable, can have bin/ files, in `plugins/`
   - **X-Pack Plugins**: Commercial, Elastic License, in `x-pack/plugin/`

4. **Gradle Plugin Declaration** - Use `esplugin` DSL block:
   ```gradle
   esplugin {
     name 'plugin-name'
     description 'Plugin description'
     classname 'org.elasticsearch.plugin.PluginClass'
   }
   ```

### Dependency Injection Pattern

Elasticsearch uses custom Module classes (not Spring/Guice) for wiring:
- Server has `*Module.java` classes: `ActionModule`, `ClusterModule`, `SearchModule`, `IndicesModule`, etc.
- Plugins contribute to modules via specialized interfaces
- Components registered in registries: `NamedWriteableRegistry`, `NamedXContentRegistry`

### Settings System

- Configuration uses `Setting<T>` classes with validation
- Settings are strongly typed and declared statically
- Settings can be static (require restart) or dynamic (updateable via API)
- Plugin settings must be registered via `Plugin.getSettings()`

### Build System Details

**Gradle Composite Build**:
- Three included builds: `build-conventions`, `build-tools`, `build-tools-internal`
- Version catalog in `gradle/build.versions.toml`
- Custom toolchain resolvers for JDK management
- Dependency verification in `gradle/verification-metadata.xml` (required)

**Custom Gradle Plugins**:
- `elasticsearch.esplugin` - Build Elasticsearch plugins
- `elasticsearch.testclusters` - Set up test clusters
- `elasticsearch.internal-es-plugin` - Internal plugin development
- `elasticsearch.yaml-rest-test` - YAML REST test infrastructure
- `elasticsearch.internal-test-artifact` - Share test fixtures between projects

**Key Build Concepts**:
- Use task avoidance API: `tasks.register()` not `task`
- Lazy test cluster creation: `testClusters.register()`
- Component metadata rules manage transitive dependencies (see `ComponentMetadataRulesPlugin`)
- All dependency versions managed in `build-tools-internal/version.properties`

### Test Infrastructure

**Test Types**:
1. **Unit Tests** - `src/test/java` - Standard JUnit tests with randomized testing framework
2. **Internal Cluster Tests** - `src/internalClusterTest/java` - Multi-node cluster tests in same JVM
3. **Java REST Tests** - `src/javaRestTest/java` - REST API tests using Java client
4. **YAML REST Tests** - `src/yamlRestTest/resources` - Declarative REST tests

**Test Framework Features**:
- Randomized testing with seeds for reproducibility
- `ESTestCase` base class with utilities
- `@TestLogging` for verbose logging on failures
- Test clusters managed by `testclusters` plugin
- Multiple test types can coexist in same project

**QA Test Organization**:
- Module/plugin-specific: `<module>/qa/` subdirectories
- Cross-cutting: `qa/` directory at root
- X-Pack: `x-pack/qa/` for commercial feature integration tests

### Code Organization Patterns

**Dependency Flow**: `libs/` → `server/` → `modules/` → `plugins/`/`x-pack/`

**Key Server Packages**:
- `action` - Request/response handling, REST/transport actions
- `cluster` - Cluster state management, master node logic
- `index` - Index-level operations, shards, segments
- `search` - Search request handling, query execution
- `ingest` - Document preprocessing pipeline
- `transport` - Inter-node communication
- `http` - HTTP/REST layer
- `discovery` - Cluster formation and node discovery
- `gateway` - Cluster metadata persistence
- `indices` - Cross-index operations, templates

**Common Patterns**:
- Immutable cluster state with copy-on-write updates
- Async operations with `ActionListener` callbacks
- Streaming serialization with `StreamInput`/`StreamOutput`
- XContent (JSON/YAML) parsing with `XContentParser`
- Thread pools for different operation types

### Licensing

- **Server, modules, libs**: Dual-licensed (Server Side Public License v1, Elastic License 2.0, AGPL v3)
- **X-Pack**: Elastic License 2.0
- License headers required on all source files
- Check with `./gradlew precommit`

## Development Workflow

### Code Style

- Use `./gradlew spotlessApply` to auto-format code
- Follow existing patterns in the codebase
- Do not reformat unchanged lines
- Checkstyle enforced via `precommit` task

### Making Changes

1. **Before coding**: Discuss on GitHub issue first
2. **Add tests**: Unit tests required; integration tests for user-facing changes
3. **Run precommit**: `./gradlew precommit` before committing
4. **Run relevant tests**: Test your specific module/plugin
5. **Check forbidden APIs**: Build enforces forbidden API usage rules
6. **Update verification metadata**: If dependencies changed, run with `--write-verification-metadata`

### Adding Dependencies

1. Add dependency to appropriate `build.gradle`
2. Add version to `build-tools-internal/version.properties` or `gradle/build.versions.toml`
3. Run `./gradlew --write-verification-metadata sha256 precommit`
4. Manually verify checksums and update `origin` in `gradle/verification-metadata.xml`
5. Add component metadata rules if transitive dependencies need exclusion (see `ComponentMetadataRulesPlugin`)

### Creating a New Plugin/Module

1. Create directory in `modules/` or `plugins/` or `x-pack/plugin/`
2. Add `build.gradle` with `esplugin` block
3. Create plugin class extending `Plugin` and implementing specialized interfaces
4. Create `src/main/resources/plugin-descriptor.properties` (generated by gradle)
5. Add tests in `src/test/`, `src/internalClusterTest/`, etc.
6. Plugin is auto-discovered by `settings.gradle` (uses `addSubProjects()`)

## Common Gotchas

- **Gradle build fails on deprecation warnings** - Build configured to fail on deprecated API usage
- **Dependency verification failures** - All dependency checksums must be in `gradle/verification-metadata.xml`
- **Test failures with randomization** - Tests use random seeds; use `-Dtests.seed=<seed>` to reproduce
- **Module vs Plugin confusion** - Modules are always loaded and simpler; plugins are optional and installable
- **X-Pack requires special build** - Commercial features in x-pack/ have separate licensing
- **Task avoidance** - Always use `tasks.register()` not direct task creation for performance
- **Test source sets** - Different test types require different source sets and gradle plugins
- **Spotless formatting** - CI enforces code formatting; run `spotlessApply` before committing

## Claude Code Tips

### Reviewing GitHub PRs
- `gh` CLI may not be authenticated. Use `WebFetch` to fetch PR metadata from `github.com/...` and raw diffs from `patch-diff.githubusercontent.com/raw/...`
- WebFetch summarizes large files. For complete code, fetch individual files via `raw.githubusercontent.com/{org}/{repo}/{sha}/{path}`
- Get the PR's head SHA from the commits page (`/pull/N/commits`) to fetch exact file versions
- Always compare both the old (main) and new (PR head) versions of key files to understand the full diff
- For large PRs, use parallel `WebFetch` and background `Task` agents to fetch files concurrently
