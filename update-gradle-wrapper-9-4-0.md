# Gradle Wrapper Update Plan: 9.4.0

**Generated:** 2026-02-10  
**Current Version:** 9.3.1  
**Target Version:** 9.4.0 (GA)  
**Current RC:** 9.4.0-rc-1

## Overview

This plan outlines the steps to update the Gradle wrapper from 9.3.1 to 9.4.0 in the Elasticsearch repository. The update follows Elasticsearch's standard process with incremental RC testing before the GA release.

## Current State

### Wrapper Files

| File | Purpose | Update Method |
|------|---------|---------------|
| `gradle/wrapper/gradle-wrapper.properties` | Main wrapper | Manual update via `./gradlew wrapper` |
| `build-tools-internal/gradle/wrapper/gradle-wrapper.properties` | IDE integration | Auto-copied by wrapper task |
| `plugins/examples/gradle/wrapper/gradle-wrapper.properties` | Example plugins | Auto-copied by wrapper task |
| `build-tools-internal/src/main/resources/minimumGradleVersion` | Minimum version check | Auto-updated by wrapper task |

### Current Configuration

```properties
distributionBase=GRADLE_USER_HOME
distributionPath=wrapper/dists
distributionSha256Sum=17f277867f6914d61b1aa02efab1ba7bb439ad652ca485cd8ca6842fccec6e43
distributionUrl=https\://services.gradle.org/distributions/gradle-9.3.1-all.zip
networkTimeout=10000
validateDistributionUrl=true
zipStoreBase=GRADLE_USER_HOME
zipStorePath=wrapper/dists
```

## Phase 1: RC Testing

### Task 1.1: Update to Release Candidate

Update the Gradle wrapper to the latest available RC version.

**Current RC: 9.4.0-rc-1**
- SHA256: `18a78a784ea8cec01093a2a4edfac41a5617c87d0846a49cd223c7e817936b01`

```bash
# Update wrapper to RC version
./gradlew wrapper --gradle-version=9.4.0-rc-1

# Run wrapper again to ensure all files are properly updated
./gradlew wrapper
```

**Verification:**
- [ ] `gradle/wrapper/gradle-wrapper.properties` updated with correct version and SHA256
- [ ] `build-tools-internal/gradle/wrapper/gradle-wrapper.properties` synced
- [ ] `plugins/examples/gradle/wrapper/gradle-wrapper.properties` synced
- [ ] `build-tools-internal/src/main/resources/minimumGradleVersion` contains `9.4.0-rc-1`

### Task 1.2: Build Validation with RC

Run the core validation tasks to ensure the build works with the new Gradle version.

```bash
# Primary validation - precommit checks
./gradlew precommit

# Build tools validation
./gradlew :build-tools:check :build-tools-internal:check
```

**Success Criteria:**
- [ ] `precommit` task passes without errors
- [ ] `:build-tools:check` passes without errors
- [ ] `:build-tools-internal:check` passes without errors

### Task 1.3: Identify and Address Build Failures

If any tasks fail during RC testing:

1. Analyze the failure output and stack traces
2. Check if the failure is related to Gradle changes (see Breaking Changes section below)
3. Fix the issue in the codebase
4. Re-run validation tasks to confirm fix

## Phase 2: Breaking Changes Analysis

### Potential Breaking Changes (from 9.3.0 → 9.4.0)

Based on the Gradle upgrade documentation, the following changes may affect Elasticsearch:

#### 2.1 Referential Equality for Project Instances

**Impact:** `Project` instances representing the same logical project may no longer be referentially equal.

**What to check:**
- [ ] Search for `project === ` (Kotlin) or `project.is(` (Groovy) comparisons
- [ ] Replace with `project.path ==` or `project.buildTreePath ==`

```bash
# Search for potential issues
grep -rn "project === \|project.is(" --include="*.gradle*" --include="*.kt"
```

#### 2.2 TestNG Output Changes (versions < 6.9.13.3)

**Impact:** Test output may be degraded with older TestNG versions.

**What to check:**
- [ ] Verify TestNG version used in the project (if applicable)
- [ ] If using TestNG < 6.9.13.3, consider upgrading

#### 2.3 Dependency Library Upgrades

The following embedded libraries are upgraded in Gradle 9.3.0+:

| Library | From | To | Potential Impact |
|---------|------|-----|------------------|
| Kotlin | 2.2.20 | 2.2.21 | Kotlin DSL build scripts |
| Jansi | 1.18 | 2.4.2 | Console output handling |
| ASM | 9.8 | 9.9 | Bytecode manipulation, Java 26 support |
| Groovy | 4.0.28 | 4.0.29 | Groovy build scripts |
| JaCoCo | 0.8.13 | 0.8.14 | Code coverage |

**What to check:**
- [ ] Review any custom ASM usage for compatibility
- [ ] Check Kotlin DSL scripts compile correctly
- [ ] Verify JaCoCo coverage reports generate correctly

### Deprecations Becoming Errors in Gradle 10

Review and address deprecation warnings that will become errors:

#### 2.4 Publishing Dependencies on Unpublished Projects
- Projects that publish must have all their project dependencies also published
- Check: `grep -rn "maven-publish\|ivy-publish" --include="*.gradle*"`

#### 2.5 Legacy Usage Attribute Values
- Legacy `Usage` values (`java-api-jars`, `java-api-classes`, etc.) must be replaced
- These are automatically mapped but should be updated proactively

#### 2.6 Module Coordinates for Self-Dependencies
- Using `group:name:version` to depend on current project will resolve from repositories
- Must use `project` dependency instead

## Phase 3: Deprecation Warning Resolution

### Task 3.1: Identify Deprecation Warnings

Run the build with deprecation warnings enabled:

```bash
./gradlew help --warning-mode=all --scan
```

**Actions:**
- [ ] Review the build scan deprecations view
- [ ] Create a list of all deprecation warnings
- [ ] Prioritize warnings that will become errors in Gradle 10

### Task 3.2: Address High-Priority Deprecations

For each deprecation warning:
1. Identify the source (build script, plugin, etc.)
2. Check the Gradle upgrade guide for the recommended fix
3. Apply the fix
4. Verify the warning is resolved

**Common deprecations to address:**

| Deprecation | Resolution |
|-------------|------------|
| `Wrapper.getAvailableDistributionTypes()` | Use `Wrapper.DistributionType.values()` |
| `Project.container()` | Use managed properties or `ObjectFactory.domainObjectContainer()` |
| Multi-string dependency notation | Use single-string format `"group:name:version"` |
| `ReportingExtension.file(String)` | Use `getBaseDirectory().file(String)` |
| `setAllJvmArgs()` | Use `jvmArgs()` or `setJvmArgs()` |

## Phase 4: GA Release Update

### Task 4.1: Update to GA Version

Once Gradle 9.4.0 GA is released:

1. Check availability:
```bash
curl -sL "https://services.gradle.org/distributions/gradle-9.4.0-all.zip.sha256"
```

2. Update wrapper:
```bash
./gradlew wrapper --gradle-version=9.4.0
./gradlew wrapper
```

### Task 4.2: Final Validation

```bash
# Full validation suite
./gradlew precommit
./gradlew :build-tools:check :build-tools-internal:check

# Optional: Run full test suite
./gradlew check
```

## Phase 5: Pull Request Creation

### Task 5.1: Create Branch and Commit

```bash
# Create feature branch
git checkout -b gradle-wrapper-9.4.0

# Stage wrapper changes
git add gradle/wrapper/
git add build-tools-internal/gradle/wrapper/
git add build-tools-internal/src/main/resources/minimumGradleVersion
git add plugins/examples/gradle/wrapper/

# Stage any additional fixes
git add -p  # Review and stage changes

# Commit
git commit -m "Upgrade Gradle wrapper to 9.4.0"
```

### Task 5.2: Create Pull Request

Use the `gh` CLI to create a pull request:

```bash
gh pr create --title "Upgrade Gradle wrapper to 9.4.0" --body "## Summary

Upgrades the Gradle wrapper from 9.3.1 to 9.4.0.

## Changes

- Updated \`gradle/wrapper/gradle-wrapper.properties\`
- Updated minimum Gradle version in build-tools-internal
- [List any additional changes/fixes required]

## Gradle 9.4.0 Highlights

[Add key changes from release notes]

## Testing

- [x] \`./gradlew precommit\` passes
- [x] \`./gradlew :build-tools:check :build-tools-internal:check\` passes
- [x] CI checks pass

## References

- [Gradle 9.4.0 Release Notes](https://docs.gradle.org/9.4/release-notes.html)
- [Gradle 9.x Upgrade Guide](https://docs.gradle.org/current/userguide/upgrading_version_9.html)
"
```

### Task 5.3: Monitor CI Checks

- [ ] Wait for all CI checks to complete
- [ ] Address any CI failures
- [ ] Request review when all checks pass

## Phase 6: RC Update Procedure

If a new RC is released before GA:

### Task 6.1: Check for New RCs

```bash
# Check for rc-2, rc-3, etc.
for i in 2 3 4 5; do
  result=$(curl -sI "https://services.gradle.org/distributions/gradle-9.4.0-rc-${i}-all.zip" | head -1)
  echo "RC-${i}: ${result}"
done
```

### Task 6.2: Update to Latest RC

```bash
# Get new SHA256
curl -sL "https://services.gradle.org/distributions/gradle-9.4.0-rc-N-all.zip.sha256"

# Update wrapper
./gradlew wrapper --gradle-version=9.4.0-rc-N
./gradlew wrapper
```

### Task 6.3: Re-validate and Update PR

```bash
# Run validation
./gradlew precommit
./gradlew :build-tools:check :build-tools-internal:check

# Commit and push update
git add -A
git commit -m "Update to Gradle 9.4.0-rc-N"
git push
```

## Definition of Done

All of the following must be complete:

- [ ] Gradle wrapper updated to 9.4.0 GA version
- [ ] All wrapper property files synced correctly
- [ ] `minimumGradleVersion` file updated
- [ ] `./gradlew precommit` passes locally
- [ ] `./gradlew :build-tools:check :build-tools-internal:check` passes locally
- [ ] All Gradle deprecation warnings reviewed (critical ones addressed)
- [ ] Pull request created with GA wrapper version
- [ ] All CI checks on Pull Request pass
- [ ] PR reviewed and approved

## Useful Commands Reference

```bash
# Check current Gradle version
./gradlew --version

# Update wrapper with specific version
./gradlew wrapper --gradle-version=VERSION

# Run with deprecation warnings
./gradlew help --warning-mode=all

# Generate build scan with deprecations
./gradlew help --warning-mode=all --scan

# Validate build tools
./gradlew :build-tools:check :build-tools-internal:check

# Full precommit validation
./gradlew precommit
```

## Links and References

- [Gradle Releases](https://gradle.org/releases/)
- [Gradle Release Candidates](https://gradle.org/release-candidate/)
- [Gradle 9.x Upgrade Guide](https://docs.gradle.org/current/userguide/upgrading_version_9.html)
- [Gradle Release Notes](https://docs.gradle.org/current/release-notes.html)
- [Gradle Compatibility Matrix](https://docs.gradle.org/current/userguide/compatibility.html)
