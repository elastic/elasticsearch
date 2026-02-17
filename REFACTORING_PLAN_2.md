# Refactoring Plan 2: GraalVM Native Image for ServerLauncher

## Goal

Compile the `ServerLauncher` into a GraalVM native-image binary, reducing its memory
footprint from ~32 MB RSS (JVM) to ~1–3 MB (native), while keeping the Java source code
and falling back to the JVM-based launcher when the native binary is unavailable.

## Why This Is Straightforward

The ServerLauncher code is an ideal native-image candidate. It has:

- Zero Elasticsearch dependencies (only `server-launcher-common`, which is JDK-only)
- No reflection, no dynamic class loading, no serialization frameworks
- Only standard JDK types: `ProcessBuilder`, `Thread`, `CountDownLatch`, `DataInputStream`,
  `Files`, `Runtime.addShutdownHook`
- A simple `public static void main` entry point

## Key Design Decisions

- **Build integration**: Add a Gradle `Exec` task in `server-launcher/build.gradle` that
  invokes `native-image` directly. No external Gradle plugin needed — keeps it simple and
  avoids adding a new plugin to the build infrastructure.
- **Opt-in build**: The native-image compile is slow (~30–60s) and requires a GraalVM JDK.
  Make it a separate task (`nativeCompile`), not part of the default build. Gate it on
  `native-image` being available on PATH or in `$JAVA_HOME/bin/`.
- **Startup script**: Detect the native binary at runtime; fall back to JVM launcher if absent.
- **Platform-specific**: Native images are platform-specific binaries. Initially target the
  current dev platform only. Cross-platform distribution packaging can come later.

## Architecture

```
bin/elasticsearch (startup script)
  │
  ├── Runs preparer (server-cli JVM)
  │     └── Writes launch-descriptor.bin
  │
  └── Checks for native launcher binary
        │
        ├── [native binary exists] ──> exec server-launcher (native, ~1–3 MB RSS)
        │                                └── Spawns Elasticsearch Server JVM
        │
        └── [no native binary] ──> exec java ServerLauncher (JVM, ~32 MB RSS)
                                     └── Spawns Elasticsearch Server JVM
```

## Detailed Steps

### Phase 1: Add `nativeCompile` task to server-launcher build

File: `distribution/tools/server-launcher/build.gradle`

Add a Gradle `Exec` task that:

- Depends on `jar` and `:libs:server-launcher-common:jar`
- Builds the classpath from both JARs
- Invokes `native-image` with:
  - `--no-fallback` (pure native, no JVM fallback embedded in the binary)
  - `-o server-launcher` (output name)
  - Entry point: `org.elasticsearch.server.launcher.ServerLauncher`
- Outputs to `build/native/server-launcher` (or `server-launcher.exe` on Windows)
- Skips gracefully if `native-image` is not found

Key consideration: the `compileOnly` dependency on `server-launcher-common` means it won't
appear in `runtimeClasspath`. The task needs to explicitly gather both JARs for the
native-image classpath.

### Phase 2: Update startup scripts to prefer the native binary

#### Linux / macOS (`bin/elasticsearch`)

Before the current JVM-based launcher invocation, check for the native binary:

```bash
NATIVE_LAUNCHER="$ES_HOME/lib/tools/server-launcher/server-launcher"
if [ -x "$NATIVE_LAUNCHER" ]; then
  exec "$NATIVE_LAUNCHER" "$DESCRIPTOR_PATH"
else
  # Fall back to JVM-based launcher
  exec "$JAVA" $CLI_JAVA_OPTS -cp "$LAUNCHER_LIBS" \
    org.elasticsearch.server.launcher.ServerLauncher "$DESCRIPTOR_PATH"
fi
```

#### Windows (`bin/elasticsearch.bat`)

Similar logic checking for `server-launcher.exe`.

### Phase 3: Package the native binary in the distribution

File: `distribution/build.gradle`

Add a new configuration (e.g., `libsServerLauncherNative`) and wire it so the native binary
lands in `lib/tools/server-launcher/` alongside the JARs.

This step can be deferred initially — for local testing, the binary can be manually copied
into the distribution's `lib/tools/server-launcher/` directory.

### Phase 4: Verify native-image compatibility

The code should compile cleanly with no additional configuration. Items to verify:

- `record` types (`LaunchDescriptor`) — supported since GraalVM 21
- `Thread` subclass (`ErrorPumpThread`) — supported
- `Runtime.addShutdownHook` — supported
- `ProcessBuilder.start()` — supported (uses JNI internally, handled by native-image)
- `synchronized` blocks — supported

If any issues arise, add a `META-INF/native-image/` resource directory with configuration
files (`reflect-config.json`, etc.), but this is unlikely given the simplicity of the code.

### Phase 5: Smoke test

After building, verify:

- `./build/native/server-launcher --dump <descriptor>` works
- Full startup works end-to-end with the native launcher
- Check RSS: `ps -o pid,rss -p <pid>` — expect ~1–3 MB
- Signal handling (Ctrl+C / SIGTERM) properly shuts down the server
- Daemonize mode works correctly

## What Does NOT Change

- **`WindowsServiceDaemon`** — it directly spawns the server process and does not use the
  launcher binary. It is unaffected.
- **`server-launcher-common`** — no changes needed. It is just a library; its classes get
  compiled into the native binary.
- **JVM-based launcher JARs** — they stay in the distribution as a fallback.

## Future Considerations

- **Cross-platform CI builds**: Native images are platform-specific. A full distribution
  would need native binaries for linux-x64, linux-aarch64, darwin-x64, darwin-aarch64,
  and windows-x64. This requires either cross-compilation or per-platform CI jobs.
- **Static linking on Linux**: Using `--static` (with musl libc) produces a fully
  self-contained binary with no shared library dependencies, ideal for minimal container
  images.
- **Rewrite as native C program**: If the native-image approach proves successful, a
  further step could replace the Java launcher entirely with a small C program, eliminating
  even the native-image build dependency.
