# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is the Elasticsearch 9.2.4 repository with custom plugins:
- **lance-vector**: Vector field type plugin using Lance format for external vector storage
- **security-realm-cloud-iam**: Custom security realm for Aliyun RAM authentication

Elasticsearch is a distributed search and analytics engine built with Java 21 and Gradle.

## Build and Test Commands

### Core Build Commands

```bash
# Build local distribution (fastest for development)
./gradlew localDistro

# Build specific plugin
./gradlew :plugins:lance-vector:assemble
./gradlew :plugins:security-realm-cloud-iam:assemble

# Format code (runs Spotless)
./gradlew spotlessApply

# Check formatting
./gradlew spotlessJavaCheck

# Full build check (includes tests)
./gradlew check

# Pre-commit checks (formatting, license headers, etc.)
./gradlew precommit
```

### Running Elasticsearch

```bash
# Run Elasticsearch from source
./gradlew run

# Run with security disabled
./gradlew run -Dtests.es.xpack.security.enabled=false

# Run with specific distribution
./gradlew run -Drun.distribution=oss

# Run with trial license (enables security and paid features)
./gradlew run -Drun.license_type=trial
```

### ⚠️ CRITICAL: Starting ES with OSS Support (Lance Vector Plugin)

**IMPORTANT**: When using the lance-vector plugin with OSS (Alibaba Cloud Object Storage), environment variables MUST be set in the parent shell BEFORE starting Elasticsearch. The native Lance Rust code reads OSS credentials from the process environment, not from Java's System.getenv().

**DO NOT**:
```bash
# ❌ WRONG - This doesn't work for native Lance code
./bin/elasticsearch -d -p elasticsearch.pid
# Then try to set env vars programmatically - won't work!
```

**CORRECT PROCEDURE**:
```bash
# ✅ CORRECT - Set environment variables BEFORE starting ES

# 1. Read credentials from ~/.oss/credentials.json
export OSS_ACCESS_KEY_ID=$(grep '"access_key_id"' ~/.oss/credentials.json | cut -d'"' -f4)
export OSS_ACCESS_KEY_SECRET=$(grep '"access_key_secret"' ~/.oss/credentials.json | cut -d'"' -f4)

# 2. Set OSS endpoint (match bucket region - Singapore for denny-test-lance)
export OSS_ENDPOINT="oss-ap-southeast-1.aliyuncs.com"

# 3. NOW start ES - it will inherit these environment variables
cd build/distribution/local/elasticsearch-9.2.4-SNAPSHOT
./bin/elasticsearch -d -p elasticsearch.pid

# 4. Verify environment variables are set in ES process
ES_PID=$(cat elasticsearch.pid)
cat /proc/$ES_PID/environ | tr '\0' '\n' | grep OSS
```

**Using the startup script** (automates the above):
```bash
cd build/distribution/local/elasticsearch-9.2.4-SNAPSHOT
./start_es_with_oss.sh -d
```

**Why This Matters**:
- Lance Rust SDK (native code) uses Opendal for object storage
- Opendal reads OSS configuration from C/C++ `getenv()` function
- Java's `System.getenv()` map is immutable and separate from process environment
- Environment variables must be set in parent shell to be inherited by child Java process
- This is the ONLY way native Lance code can access OSS credentials

### Other Running Options

```bash
# Run with remote debugging (port 5005-5007 for multiple nodes)
./gradlew run --debug-jvm

# Run cross-cluster search (2 clusters)
./gradlew run-ccs
```

### Test Commands

```bash
# Run all tests
./gradlew test

# Run single test class
./gradlew :server:test --tests org.elasticsearch.package.ClassName

# Run single test method
./gradlew :server:test --tests org.elasticsearch.package.ClassName.methodName

# Run tests for specific plugin
./gradlew :plugins:lance-vector:test
./gradlew :plugins:security-realm-cloud-iam:test

# Run with specific seed (for reproducibility)
./gradlew test -Dtests.seed=DEADBEEF

# Run tests N times
./gradlew :server:test -Dtests.iters=N --tests org.elasticsearch.package.ClassName

# Run test with custom heap size
./gradlew test -Dtests.heap.size=4G

# Run integration tests
./gradlew :plugins:lance-vector:integTest

# Run YAML REST tests
./gradlew :plugins:lance-vector:yamlRestTest
```

### Platform-Specific Builds

```bash
# Linux tar
./gradlew :distribution:archives:linux-tar:assemble

# macOS tar (x86_64)
./gradlew :distribution:archives:darwin-tar:assemble

# macOS tar (ARM64)
./gradlew :distribution:archives:darwin-aarch64-tar:assemble

# Windows zip
./gradlew :distribution:archives:windows-zip:assemble
```

## Architecture

### High-Level Structure

- **`server/`**: Core Elasticsearch server code
  - Main entry point, REST handlers, cluster coordination
  - `org.elasticsearch.plugins` - Plugin infrastructure
  - `org.elasticsearch.index` - Index management and mappings
  - `org.elasticsearch.search` - Search query execution

- **`libs/`**: Internal libraries (not meant for external use)
  - `plugin-api` - Plugin development API
  - `core` - Core utilities (Types, Strings, etc.)
  - `x-content` - XContent (JSON/YAML/CBOR) parsing

- **`modules/`**: Shipped but optional features
  - `lang-painless` - Painless scripting language
  - `reindex` - Reindex API
  - `repository-s3` - S3 repository
  - `repository-azure` - Azure repository
  - `repository-gcs` - GCS repository

- **`plugins/`**: Officially supported plugins
  - `analysis-icu` - ICU analysis components
  - `discovery-ec2` - EC2 discovery
  - `discovery-gce` - GCE discovery
  - `repository-hdfs` - HDFS repository
  - `lance-vector` - Custom vector field type
  - `security-realm-cloud-iam` - Aliyun RAM authentication

- **`x-pack/`**: Commercial features (Elastic License)
  - `plugin/core` - Core X-Pack functionality
  - `plugin/security` - Security, authentication, authorization
  - `plugin/ml` - Machine learning
  - `plugin/esql` - ESQL query language

- **`test/`**: Testing infrastructure
  - `framework` - Test framework (ESTestCase, ElasticsearchTestCase)
  - `fixtures/*` - Test fixtures for external services (MinIO, HDFS, GCS, etc.)

- **`qa/`**: Cross-plugin/component tests
  - Multi-version tests (BWC - backwards compatibility)
  - Full cluster restart tests
  - Rolling upgrade tests

### Plugin Architecture

Elasticsearch plugins extend the server through:

1. **Plugin Class**: Extends `org.elasticsearch.plugins.Plugin`
   - Registers custom mappers, queries, ingest processors, etc.

2. **Build Configuration**: Uses `elasticsearch.esplugin` Gradle plugin
   ```gradle
   esplugin {
     name = 'plugin-name'
     description = 'Plugin description'
     classname = 'org.elasticsearch.plugin.PluginClass'
     extendedPlugins = ['x-pack-security']  // For X-Pack extensions
   }
   ```

3. **Plugin Types**:
   - **Mapper plugins**: Add new field types (e.g., `lance_vector`)
   - **Search plugins**: Add query types or scoring
   - **Ingest plugins**: Add ingest processors
   - **Security realm plugins**: Add authentication realms
   - **Repository plugins**: Add snapshot/restore repositories

### Key Extension Points

- **`org.elasticsearch.index.mapper`**: Custom field types and mappers
- **`org.elasticsearch.search.query`**: Custom query types
- **`org.elasticsearch.index.query`**: Query parsing and execution
- **`org.elasticsearch.plugins`**: Plugin lifecycle and component registration
- **`org.elasticsearch.xpack.security.authc`**: Custom authentication realms
- **`org.elasticsearch.ingest`**: Ingest processors

### Custom Plugins (This Repository)

#### Lance Vector Plugin (`plugins/lance-vector/`)

- **Purpose**: Vector field type using Lance format for external vector storage
- **Key Classes**:
  - `LanceVectorPlugin` - Main plugin class
  - `LanceVectorFieldMapper` - Field type and mapper
  - `LanceVectorQuery` - kNN query implementation
  - `LanceVectorQueryParser` - Query DSL parser
  - `OssStorageAdapter` - Alibaba Cloud OSS integration

- **Dependencies**:
  - `com.lancedb:lance-core:1.0.0-beta.2` - Lance Java SDK
  - `org.apache.arrow:*:15.0.0` - Apache Arrow for columnar data
  - Excludes Guava from compile classpath, includes at runtime only

- **Storage Backends**:
  - Local filesystem: `file:///path/to/dataset.lance`
  - Alibaba Cloud OSS: `oss://bucket/path/to/dataset.lance`

- **JVM Requirements**:
  ```
  --add-opens=java.base/java.nio=ALL-UNNAMED  # For Arrow memory management
  ```

#### Cloud IAM Realm Plugin (`plugins/security-realm-cloud-iam/`)

- **Purpose**: Custom security realm for Aliyun RAM authentication
- **Key Classes**:
  - `CloudIamRealmPlugin` - Main plugin class
  - `CloudIamRealm` - Authentication realm implementation
  - `CloudIamRealmSettings` - Realm settings

- **Test Configuration**:
  - Uses mock mode for testing (`auth.mode: mock`)
  - Requires trial license (`xpack.license.self_generated.type: trial`)

## Code Conventions

### Java Formatting

- **Indent**: 4 spaces (no tabs)
- **Line width**: 140 characters
- **Formatter**: Spotless with Eclipse formatter
- **Import order**: Organized automatically, no wildcard imports
- **Negative booleans**: Use `foo == false` instead of `!foo`

### License Headers

All Java files must include license header:

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
```

For x-pack code (Elastic License):

```java
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
```

### Javadoc Guidelines

- Always add Javadoc to new public APIs
- Document the "why", not the "how"
- Don't document trivial methods (getters/setters)
- Use `@link` for cross-references

### Logging

Use SLF4J with parameterized logging:

```java
private static final Logger logger = LogManager.getLogger(MyClass.class);

// Good
logger.debug("operation failed [{}] times in [{}]ms", failureCount, elapsedMillis);

// Bad (string concatenation)
logger.debug("operation failed " + failureCount + " times");
```

**Log Levels**:
- **TRACE**: Very verbose, for debugging complex behaviors
- **DEBUG**: Diagnostic information for troubleshooting
- **INFO**: Important lifecycle events
- **WARN**: Issues requiring investigation
- **ERROR**: Degraded state requiring recovery

## Dependency Management

### Adding/Updating Dependencies

1. Add dependency to `build.gradle`
2. Update `gradle/verification-metadata.xml`:
   ```bash
   ./gradlew --write-verification-metadata sha256 precommit
   ```
3. For transitive dependency issues, use component metadata rules:
   ```gradle
   components.withModule("group:artifact", ExcludeAllTransitivesRule.class);
   ```

### Version Management

- All versions in `build-tools-internal/version.properties`
- Use component metadata rules to manage transitive dependencies
- Prefer `compileOnly` for Elasticsearch-provided dependencies
- Use `runtimeOnly` for transitive dependencies not used in code

## Testing Patterns

### Test Structure

- **Unit tests**: `src/test/java/`
- **Integration tests**: `src/test/java/` (uses test clusters)
- **YAML REST tests**: `src/yamlRestTest/java/` (REST API tests)

### Test Framework

- **ESTestCase**: Base class for integration tests
- **ElasticsearchTestCase**: Base class for unit tests
- **AbstractXContentTestCase**: Test XContent serialization
- **AbstractQueryTestCase**: Test custom queries

### Test Clusters

```java
// Create test cluster
testClusters.register("my-cluster") {
  setting("key", "value")
  plugin("lance-vector")
}

// Access in tests
@ElasticsearchNodesAware
void testSomething(Collection<Client> clients) {
  Client client = clients.get(0);
  // Use client
}
```

### Randomized Testing

Tests are randomized by default. Use specific seeds for reproducibility:

```bash
./gradlew test -Dtests.seed=DEADBEEF
```

## Git Workflow

### Commit Guidelines

- Include Javadoc, license headers
- Format code with `./gradlew spotlessApply`
- Run `./gradlew check` before committing
- Write descriptive commit messages

### Pull Request Process

1. Discuss in GitHub issue first
2. Fork and create branch
3. Make changes with tests
4. Run full test suite
5. Sign CLA (Contributor License Agreement)
6. Submit PR with clear description
7. Address review feedback
8. Squash commits (if requested)

## Common Pitfalls

### Security/Entitlements

- ES uses entitlements/security manager that restricts reflection
- Use `--add-opens` for Java 21+ module access (Arrow, JNI)
- Set `es.entitlement.enableForTests=false` in tests if needed

### Memory Management

- Arrow requires careful memory management
- Use `--add-opens=java.base/java.nio=ALL-UNNAMED` for Arrow
- JNI libraries (Lance) need proper resource cleanup

### ClassLoader Issues

- Plugin dependencies may conflict with ES classpath
- Use `runtimeOnly` for dependencies only needed at runtime
- Exclude transitive dependencies that conflict with ES

## Development Workflow

### Typical Plugin Development

1. Create plugin in `plugins/my-plugin/`
2. Implement plugin class extending `Plugin`
3. Add `build.gradle` with `esplugin` configuration
4. Implement custom functionality (mapper, query, etc.)
5. Add unit tests in `src/test/java/`
6. Add integration tests if needed
7. Build with `./gradlew :plugins:my-plugin:assemble`
8. Test locally with `./gradlew run`
9. Format code with `./gradlew spotlessApply`
10. Run full test suite

### Debugging

- Use `./gradlew run --debug-jvm` for remote debugging
- Set breakpoints in IntelliJ IDEA
- Use `logger.debug()` for diagnostic output
- Check logs in `build/testclusters/*/logs/`

## Resources

- **Elasticsearch source**: https://github.com/elastic/elasticsearch
- **Plugin docs**: `CONTRIBUTING.md`
- **Build guide**: `BUILDING.md`
- **Testing guide**: `TESTING.asciidoc`
- **Validation guide**: `VALIDATION_GUIDE.md` (Lance plugin)

## Environment-Specific Notes

### Java Version
- **Required**: JDK 21
- **JAVA_HOME**: Must point to JDK 21 installation

### Gradle
- **Wrapper version**: Defined in `gradle/wrapper/gradle-wrapper.properties`
- **Parallel builds**: Enabled by default (`org.gradle.parallel=true`)
- **Configuration cache**: Enabled (`enableFeaturePreview "STABLE_CONFIGURATION_CACHE"`)

### Docker
- **Required**: For certain test fixtures and package builds
- **Used by**: Repository tests (S3, GCS, Azure), package builds

## IntelliJ IDEA Setup

1. Open project (File > Open > `build.gradle`)
2. Import as Gradle project
3. Set JDK 21 as project SDK (name it "21")
4. Checkstyle configuration auto-applied via `configureIdeCheckstyle`
5. Eclipse formatter auto-applied (in `.idea/eclipseCodeFormatter.xml`)
6. Run configurations created automatically

### IntelliJ Options

| Property | Values | Description |
|----------|--------|-------------|
| `org.elasticsearch.idea-configuration-cache` | `true`/`false` | Enable Gradle configuration cache |
| `org.elasticsearch.idea-delegate-to-gradle` | `true`/`false` | Use Gradle for all run/test configs |

## Repository-Specific Conventions

### REST API Conventions
- Use singular nouns in URLs: `/_ingest/pipeline` not `/_ingest/pipelines`
- Follow existing REST API patterns in `server/src/main/java/org/elasticsearch/rest/`

### Code Organization
- Keep changes minimal and focused
- Don't format unrelated code
- Don't reorder imports in unchanged code
- Add Javadoc for new public APIs
- Include license headers on all new files
