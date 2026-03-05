# Refactoring Plan 6: Native Image Build and Docker Executable Resolution

## Goal

Two related refactors:

1. **Native image build**: Fix incorrect assumptions in the server-launcher native image
   packaging so that (a) native binaries are built only for Linux (x86_64 and aarch64),
   (b) the build runs inside Docker so any host can produce both architectures, (c) the
   distribution resolves native binaries as Gradle artifacts per architecture and includes
   them only in Linux distributions, and (d) no implicit task dependency forces
   native-image to run for every distribution.

2. **Docker executable resolution**: Centralize "where is the docker binary?" in
   `DockerSupportService` and have Docker-based tasks (DockerBuildTask, NativeImageBuildTask)
   inject the service via `@ServiceReference`, resolve the path in the task, and pass it
   into the worker so docker is found even when the worker has a minimal PATH (e.g. IDE).

## Background: Problems Addressed

### Native image

- The distribution `libFiles` copy spec referred directly to the `nativeCompile` task and
  passed it to `from(nativeCompileTask)`, creating an implicit task dependency. The
  native-image task ran for every distribution build, not only when needed.
- The same native output was copied into every distribution (e.g. building `windowsZip`
  on Linux included a Linux binary).
- The setup assumed building for the current host; the Elasticsearch build is designed to
  produce any platform distribution from any host.

### Docker executable

- Gradle workers can have a minimal or empty PATH. Both `DockerBuildTask` and
  `NativeImageBuildTask` used the literal `"docker"` when running the CLI, so they failed
  with "Cannot run program 'docker'" when the worker could not find the binary (e.g. when
  Gradle was started from an IDE). `DockerSupportService` already had a notion of docker
  path (private `getDockerPath()` and `DockerAvailability.path`) but only checked a fixed
  list of two paths and was not used when executing docker in tasks.

## Key Design Decisions

### Native image

- **Linux only**: Native images are built only for Linux x86_64 and aarch64. Darwin and
  Windows distributions use the normal JAR packaging; the startup script already falls
  back to the JVM launcher when no native binary is present.
- **Docker-based build**: The native-image compile runs inside a Linux container
  (`ghcr.io/graalvm/native-image-community`). Image tag is derived from the bundled JDK
  major (e.g. `:25`); if that tag does not exist, use `-PnativeImageTag=latest` (or
  document the fallback). No GraalVM download or Ivy in the build; the container
  supplies the toolchain.
- **Custom task, not Exec**: A `NativeImageBuildTask` in build-tools-internal uses the
  Gradle Worker API (so the two architecture builds can run in parallel) and is
  `@CacheableTask` for remote build cache. It runs `docker run ... <image>` with
  native-image arguments; the image ENTRYPOINT is already `native-image`, so we do not
  pass `native-image` as a command argument.
- **Artifact-based packaging**: server-launcher exposes two consumable configurations
  (`nativeLinuxX64`, `nativeLinuxAarch64`), each with a single artifact (the binary)
  built by the corresponding task. The distribution declares two resolvable
  configurations (`libsServerLauncherNativeX64`, `libsServerLauncherNativeAarch64`) and
  in `libFiles` includes the native binary only when `os == 'linux'` and the
  architecture matches. No direct task dependency from distribution to native-image;
  dependency flows via configuration resolution.
- **Dependencies**: The native-image classpath uses `sourceSets.main.output` and a
  project dependency on `:libs:server-launcher-common` (resolved via a configuration).
  No explicit dependency on `jar` or `:libs:server-launcher-common:jar` tasks.

### Docker executable

- **Single place**: `DockerSupportService` gets a public instance method
  `getResolvedDockerExecutable()` that (1) searches `PATH` for an executable named
  `docker`, (2) falls back to an ordered list of paths (`/usr/local/bin/docker`,
  `/usr/bin/docker`, `/opt/homebrew/bin/docker`) with `canExecute()` checked. Returns
  the absolute path if found, otherwise `"docker"`. No static methods.
- **Injection and worker param**: Tasks that run docker (`DockerBuildTask`,
  `NativeImageBuildTask`) declare `@ServiceReference(DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME)`
  and obtain a `Provider<DockerSupportService>`. In the task action they call
  `getDockerSupport().get().getResolvedDockerExecutable()` and pass that string into the
  worker via `WorkParameters.getDockerExecutable()`. The worker uses only the passed path
  for every `docker` invocation; it does not call the service.
- **Availability**: Private `getDockerPath()` was refactored to use the same logic as
  `getResolvedDockerExecutable()` so `getDockerAvailability()` benefits (finds docker in
  PATH and in the extended fallback list). `DOCKER_BINARIES` was extended to include
  `/opt/homebrew/bin/docker` and reordered for consistency.

## What Changed

### build-tools-internal

- **`DockerSupportService`**
  - Extended `DOCKER_BINARIES` to include `/opt/homebrew/bin/docker` and use a single
    ordered list for resolution and availability.
  - Added public instance method `getResolvedDockerExecutable()` (PATH search then
    fallbacks, with `canExecute()`). Returns absolute path or `"docker"`.
  - Refactored private `getDockerPath()` to use `getResolvedDockerExecutable()` and
    return `Optional.of(path)` when not `"docker"`, else `Optional.empty()`.

- **`DockerBuildTask`**
  - Added `@ServiceReference(DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME)` and
    `Property<DockerSupportService> getDockerSupport()`.
  - In `@TaskAction`, resolve `dockerExecutable` with
    `getDockerSupport().get().getResolvedDockerExecutable()` and pass it into the worker
    via `params.getDockerExecutable().set(dockerExecutable)`.
  - Added `Property<String> getDockerExecutable()` to the worker `Parameters` interface.
  - In `DockerBuildAction`, use `parameters.getDockerExecutable().get()` for every docker
    call (main build, `pullBaseImage`, `getImageChecksum`).

- **`NativeImageBuildTask`**
  - Removed the **DockerExecutable** helper class (PATH + fallbacks had been duplicated
    there).
  - Added `@ServiceReference(DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME)` and
    `getDockerSupport()`.
  - Resolve path in task action and pass to worker via `params.getDockerExecutable()`.
  - Added `getDockerExecutable()` to worker `Parameters`; the action uses it when
    setting the executable for the `docker run` command.

- **`DockerSupportPlugin`**
  - In the `whenReady` block that collects tasks requiring Docker, the filter now
    includes `NativeImageBuildTask` in addition to `DockerBuildTask`, so running a
    native-image task when Docker is unavailable produces a clear error.

### distribution/tools/server-launcher/build.gradle

- Removed the host-based `nativeCompile` Exec task and all GraalVM Ivy repository,
  artifact transforms, and `graalvm` configuration.
- Added configuration `nativeImageClasspath` with
  `project(':libs:server-launcher-common')` (project dependency only).
- Registered two tasks `nativeImageLinuxX64` and `nativeImageLinuxAarch64` (type
  `NativeImageBuildTask`) with classpath `sourceSets.main.output + configurations.nativeImageClasspath`,
  image `ghcr.io/graalvm/native-image-community:${nativeImageTag}` (tag from bundled JDK
  major or `-PnativeImageTag`), platforms `linux/amd64` and `linux/arm64`, and outputs
  under `build/native/linux-x86_64/` and `build/native/linux-aarch64/`.
- Declared consumable configurations `nativeLinuxX64` and `nativeLinuxAarch64` with
  artifacts built by the corresponding tasks.

### distribution/build.gradle

- Added configurations `libsServerLauncherNativeX64` and `libsServerLauncherNativeAarch64`.
- Added dependencies on
  `project(path: ':distribution:tools:server-launcher', configuration: 'nativeLinuxX64')`
  and `nativeLinuxAarch64` respectively.
- Removed all references to `nativeCompile` and `from(nativeCompileTask)` in the
  server-launcher copy spec.
- In `libFiles`, inside `into('tools/server-launcher')`, the native binary is included
  only when `os == 'linux'`: `from(configurations.libsServerLauncherNativeX64)` when
  `architecture == 'x64'`, `from(configurations.libsServerLauncherNativeAarch64)` when
  `architecture == 'aarch64'`. Executable permission (e.g. unix 0755) is applied to the
  native binary as before.

## What Did NOT Change

- Startup scripts: existing logic that checks for the native binary and falls back to JVM
  is unchanged.
- Windows and Darwin distributions: they do not reference the native configurations and
  only ship the JARs.
- Root `build.gradle`: `elasticsearch.docker-support` is already applied there; no
  plugin application changes were required for NativeImageBuildTask to use the service.
- `gradle/verification-metadata.xml`: the `<trust group="oracle_graalvm"/>` entry was
  left as-is (unused after removing Ivy-based GraalVM; can be removed in a follow-up).

## Architecture (native image)

```
server-launcher
  ├── sourceSets.main.output + configurations.nativeImageClasspath
  ├── nativeImageLinuxX64 (NativeImageBuildTask)  → build/native/linux-x86_64/server-launcher
  └── nativeImageLinuxAarch64 (NativeImageBuildTask) → build/native/linux-aarch64/server-launcher
        ↓ artifacts
  consumable configs: nativeLinuxX64, nativeLinuxAarch64

distribution (libFiles)
  ├── Always: libsServerLauncher (JARs), libsServerLauncherCommon
  └── When os == 'linux': from(libsServerLauncherNativeX64) or from(libsServerLauncherNativeAarch64)
        → resolves server-launcher native artifact for that architecture only
```

## Architecture (Docker executable)

```
DockerSupportPlugin (root)
  └── registers DockerSupportService

DockerSupportService
  └── getResolvedDockerExecutable() → PATH then /usr/local/bin, /usr/bin, /opt/homebrew; canExecute()

DockerBuildTask / NativeImageBuildTask
  ├── @ServiceReference → getDockerSupport().get()
  ├── In task action: dockerExe = getDockerSupport().get().getResolvedDockerExecutable()
  └── params.getDockerExecutable().set(dockerExe) → worker uses it for all docker invocations
```

## Future Considerations

- **Tests for getResolvedDockerExecutable**: Unit-testing the method in isolation would
  require either a full Gradle project (with DockerSupportPlugin applied) or mocking
  the filesystem/PATH. Applying the plugin from a test pulls in GlobalBuildInfoPlugin
  and version.properties, which are not available in the plain unit-test environment.
  The resolution logic is exercised by the Docker-based tasks at runtime.
- **Removing oracle_graalvm trust**: Now that server-launcher no longer uses the Ivy-based
  GraalVM download, the verification-metadata entry could be removed in a separate change.
