---
description: Generate a plan for updating the Gradle wrapper to a specified version
agent: build
---

## Gradle Wrapper Update Plan Generator

Generate a detailed plan for updating the Gradle wrapper to version **$1**.

### Step 1: Gather Current State

Find all `gradle-wrapper.properties` files in the repository:

```bash
find . -name "gradle-wrapper.properties" -type f
```

For each file found, extract the current version from `distributionUrl`.

### Step 2: Fetch Version Information

1. Fetch the SHA256 checksum from:
   `https://services.gradle.org/distributions/gradle-$1-all.zip.sha256`

2. Fetch release notes to understand potential breaking changes:
   - `https://docs.gradle.org/$1/release-notes.html`
   - Fall back to: `https://docs.gradle.org/current/release-notes.html` and search for the version

### Step 3: Generate Plan Document

Create a markdown plan file at `specs/gradle-wrapper-update-$1.md` with the following structure:

```markdown
# Gradle Wrapper Update Plan: $1

**Generated:** [current date]
**Target Version:** $1
**SHA256:** [fetched checksum]

## Current State

| File | Current Version | Action |
|------|-----------------|--------|
| [path] | [version] | Update / Skip (reason) |

## Update Tasks

### Task 1: Update Wrapper Files

For each file requiring update:
- [ ] Update `[file path]`
  - Change `distributionUrl` to `gradle-$1-all.zip`
  - Set `distributionSha256Sum` to `[checksum]`

### Task 2: Validate Changes

- [ ] Run `./gradlew precommit`
- [ ] Address any failures

### Task 3: Handle Breaking Changes

[List relevant breaking changes from release notes that may affect this codebase]

#### Potential Issues
- [Issue 1 from release notes]
- [Issue 2 from release notes]

#### Resolution Steps
- [ ] [Specific fix if needed]

### Task 4: Final Validation

- [ ] Confirm all wrapper files updated
- [ ] Confirm precommit passes
- [ ] Review changes for unintended side effects

## Release Notes Summary

[Summarize key changes from the release notes relevant to this codebase]

## Execution

To execute this plan, run:
\`\`\`
/execute-gradle-update specs/gradle-wrapper-update-$1.md
\`\`\`
```

### Step 4: Save and Report

1. Ensure the `specs/` directory exists
2. Write the plan to `specs/gradle-wrapper-update-$1.md`
3. Report the location of the generated plan file

### Output

Provide a summary including:
- Location of the generated plan file
- Number of wrapper files that will be updated
- Key breaking changes identified from release notes
- Recommendation on whether this update appears safe or requires careful review
