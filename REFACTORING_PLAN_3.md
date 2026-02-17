# Refactoring Plan 3: Auto-Provision GraalVM for Native Image Compilation

## Goal

Automatically download and manage the Oracle GraalVM JDK for native-image compilation, so
Elasticsearch developers do not need to install GraalVM manually. The GraalVM version is
derived from the bundled JDK version, ensuring they always stay in sync.

## Background

The `nativeCompile` task in `server-launcher/build.gradle` previously expected `native-image`
to already exist in `$JAVA_HOME/bin/` or on `PATH`, requiring developers to manually install a
GraalVM JDK. This plan automates that provisioning.

## Approach Considered and Abandoned: Gradle Java Toolchain API

The initial plan was to add a custom `JavaToolchainResolver` (following the pattern of
`OracleOpenJdkToolchainResolver`, `AdoptiumJdkToolchainResolver`, etc.) that would plug into
Gradle's `javaToolchains.launcherFor` API to auto-download GraalVM on demand.

This approach was abandoned because of a fundamental incompatibility in Gradle's vendor
matching. Oracle GraalVM distributions report `IMPLEMENTOR="Oracle Corporation"` in their
`release` file, which causes Gradle to classify them as the `ORACLE` vendor (same as Oracle
OpenJDK). Requesting `JvmVendorSpec.GRAAL_VM` causes Gradle to download the archive
successfully but then reject it during post-download validation because the vendor doesn't
match. There is no way to distinguish Oracle GraalVM from Oracle OpenJDK using Gradle's
`JvmVendorSpec` system.

## Approach Implemented: Ivy Repository with Artifact Transforms

Instead, the `server-launcher/build.gradle` now declares an Ivy repository pointing to
Oracle's GraalVM download site, downloads the GraalVM archive as a standard Gradle dependency,
and uses the existing `SymbolicLinkPreservingUntarTransform`/`UnzipTransform` artifact
transforms to extract it. The `nativeCompile` task then locates `native-image` inside the
extracted directory.

This approach:
- Requires no changes outside of `server-launcher/build.gradle` (plus a trust entry in
  `gradle/verification-metadata.xml`)
- Reuses existing build infrastructure (Ivy repos, artifact transforms)
- Automatically downloads, caches, and extracts GraalVM — no manual install needed
- Derives the GraalVM version from `VersionProperties.getBundledJdkVersion()`, so GraalVM
  always matches the JDK version used by the rest of the build

### Key design decision: Version derivation

Oracle GraalVM releases track JDK version numbers exactly — Oracle GraalVM for JDK 25.0.2
*is* version 25.0.2. The build script parses the base version (e.g., `25.0.2`) from the
bundled JDK version string (`25.0.2+10@hash`), stripping the build number and hash. When the
bundled JDK is updated, GraalVM automatically tracks it with no separate version to maintain.

## Oracle GraalVM Download URL Pattern

```
https://download.oracle.com/graalvm/{major}/archive/graalvm-jdk-{version}_{os}-{arch}_bin.{ext}
```

For example, with `bundled_jdk = 25.0.2+10@b1e0dfa218384cb9959bdcb897162d4e`:
```
https://download.oracle.com/graalvm/25/archive/graalvm-jdk-25.0.2_macos-aarch64_bin.tar.gz
```

## What Changed

- `distribution/tools/server-launcher/build.gradle` — replaced manual `native-image` PATH
  lookup with Ivy-based GraalVM download, artifact transform extraction, and automatic
  `native-image` path resolution
- `gradle/verification-metadata.xml` — added `<trust group="oracle_graalvm"/>` entry

## What Did NOT Change

- `version.properties` and `VersionProperties.java` — no changes needed
- `settings.gradle` — no changes needed
- `build-tools-internal` toolchain resolvers — no changes needed
- The `nativeCompile` task remains opt-in (not part of the default build)
- The native binary output location and format are unchanged
- The `server-launcher-common` library is unaffected
- The startup scripts are unaffected
