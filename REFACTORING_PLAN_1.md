# Refactoring Plan: Split `server-cli` into Preparer + Launcher

## Goal

Split the current `server-cli` into two programs:

1. **Preparer** (existing `server-cli`, slimmed down) — does all the heavy lifting, then writes a launch descriptor and exits.
2. **Launcher** (new `server-launcher`) — a minimal program that reads the launch descriptor, spawns the server JVM, and waits for it.

Both programs share a small **utility library** (`server-common` or similar) that contains the binary descriptor format and basic helpers. This library has no ES dependencies — only JDK classes.

## Current Flow (single program)

```
ServerCli.execute()
  ├── parse options, validate config
  ├── load secure settings
  ├── auto-configure security (optional)
  ├── sync plugins
  ├── create ServerArgs
  ├── setup temp dir
  ├── determine JVM options (JvmOptionsParser)
  ├── build process command line (ServerProcessBuilder)
  ├── spawn server JVM process
  ├── send ServerArgs over stdin (binary Writeable protocol)
  ├── pump stderr, wait for READY marker
  └── waitFor() / detach()
```

## Proposed Flow (two programs + shared library)

```
PROGRAM 1: server-cli (Preparer)
  ├── parse options, validate config
  ├── load secure settings
  ├── auto-configure security
  ├── sync plugins
  ├── create ServerArgs
  ├── setup temp dir
  ├── determine JVM options
  ├── compute full command line + environment
  ├── serialize ServerArgs to byte[] (using existing Writeable protocol)
  ├── build LaunchDescriptor (includes ServerArgs bytes)
  ├── write LaunchDescriptor to a single binary file (using shared library)
  └── exit

PROGRAM 2: server-launcher (Launcher) [JDK + shared library only]
  ├── read LaunchDescriptor from binary file (using shared library)
  ├── build ProcessBuilder from descriptor
  ├── spawn the server JVM process
  ├── stream the embedded ServerArgs bytes to process stdin (raw copy)
  ├── pump stderr, watch for READY marker (char \u0018)
  ├── handle daemonize (detach) vs foreground (waitFor)
  ├── handle shutdown (send \u001B to stdin on signal)
  └── exit with server's exit code

SHARED LIBRARY: server-common (JDK-only)
  ├── LaunchDescriptor record
  ├── Binary serialization (DataOutputStream / DataInputStream)
  ├── Human-readable dump (section-based text format)
  └── ProcessUtil
```

## Detailed Steps

### Phase 1: Create the shared utility library

New Gradle project (e.g., `libs/server-common` or `libs/server-launcher-common`). **Zero ES dependencies**, only JDK classes.

Contains:

- **`LaunchDescriptor`** — a record holding all fields needed to launch the server:
  - `command` (String) — path to java binary
  - `jvmOptions` (List\<String\>) — JVM flags
  - `jvmArgs` (List\<String\>) — module-path, `-m`, etc.
  - `environment` (Map\<String, String\>) — env vars for the server process
  - `workingDir` (String) — logs directory (used as process working dir)
  - `serverArgsBytes` (byte[]) — opaque blob of serialized `ServerArgs`
  - `daemonize` (boolean)
  - `tempDir` (String) — path to temp directory

- **Binary serialization** using `DataOutputStream` / `DataInputStream`:
  - `LaunchDescriptor.writeTo(DataOutputStream)` — serialize to binary
  - `LaunchDescriptor.readFrom(DataInputStream)` — deserialize from binary
  - Strings via `writeUTF`/`readUTF`, lists as length-prefixed, maps as length-prefixed key-value pairs, byte arrays as length-prefixed raw bytes

- **Human-readable dump** — a `LaunchDescriptor.toHumanReadable()` method that formats the descriptor in a section-based text format for debugging:

```
[command]
/usr/lib/jvm/java-21/bin/java

[working_dir]
/var/log/elasticsearch

[temp_dir]
/tmp/elasticsearch-kR3x9f

[daemonize]
false

[server_args_bytes]
<1437 bytes>

[jvm_options]
-Xms4g
-Xmx4g
-XX:+UseG1GC
-XX:G1HeapRegionSize=4m
-XX:InitiatingHeapOccupancyPercent=30
-XX:G1ReservePercent=15
-Djava.io.tmpdir=/tmp/elasticsearch-kR3x9f

[jvm_args]
--module-path
/opt/elasticsearch/lib
--add-modules=jdk.net
--add-modules=jdk.management.agent
--add-modules=ALL-MODULE-PATH
-m
org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch

[environment]
HOME=/home/elasticsearch
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin
ES_HOME=/opt/elasticsearch
ES_PATH_CONF=/etc/elasticsearch
LIBFFI_TMPDIR=/tmp/elasticsearch-kR3x9f
```

- **`ProcessUtil`** — copy of the existing JDK-only helper (non-interruptible wrappers).

### Phase 2: Create the new `server-launcher` Gradle project

New project at `distribution/tools/server-launcher`.

Dependencies: **JDK + shared utility library only**. No ES server, no ES cli library.

Key classes:

- **`ServerLauncher`** — `main()` entry point:
  - Accepts descriptor file path as command-line argument
  - Supports `--dump` flag: reads the descriptor and prints it in human-readable section-based format (via `LaunchDescriptor.toHumanReadable()`), then exits
  - Normal mode: reads descriptor, spawns server, manages lifecycle

- **`ErrorPumpThread`** — simplified re-implementation:
  - Reads stderr lines, writes to `System.err`
  - Watches for `\u0018` (ready marker)
  - No ES `Terminal` dependency

- **`ServerProcess`** — reimplemented using only JDK:
  - Wraps `Process`, provides `waitFor()`, `detach()`, `stop()`
  - Sends `\u001B` shutdown marker on stdin

- **Shutdown hook** — registered via `Runtime.addShutdownHook()`:
  - On SIGTERM/SIGINT, sends shutdown marker to server's stdin

### Phase 3: Refactor `server-cli` (the Preparer)

Modify `ServerCli.execute()`: after computing JVM options, instead of calling `ServerProcessBuilder.start()`:

1. Serialize `ServerArgs` to `byte[]` in memory (using the existing `Writeable` / `OutputStreamStreamOutput` protocol)
2. Build a `LaunchDescriptor` with all computed values (extracted from current `ServerProcessBuilder` logic: `getCommand()`, `getJvmArgs()`, `getEnvironment()`) and the ServerArgs bytes
3. Write the `LaunchDescriptor` to a binary file in the temp dir (using the shared library)
4. Exit (startup script takes over)

Classes to **remove** from `server-cli` (responsibility moves to the launcher):
- `ServerProcessBuilder`
- `ServerProcess`
- `ErrorPumpThread`

Classes to **keep** in `server-cli` (preparer responsibilities):
- `ServerCli` (modified — no longer starts/waits for the server)
- `JvmOptionsParser`, `SystemJvmOptions`, `JvmErgonomics`, `JvmOption`, `MachineDependentHeap`, `APMJvmOptions`
- `ServerProcessUtils` (temp dir setup)
- `SecureSettingsLoader`, `KeyStoreLoader`, `KeystorePasswordTerminal`
- Memory info classes (`SystemMemoryInfo`, `DefaultSystemMemoryInfo`, etc.)

New dependency: the shared utility library.

### Phase 4: Handle the stdin/stderr protocol in the Launcher

The launcher handles the server process lifecycle using only JDK classes and the shared library:

- **stdin to server**: write the raw `serverArgsBytes` from the descriptor, then keep stdin open to send shutdown marker later
- **stderr from server**: pump lines to `System.err`, watch for `\u0018` (ready marker)
- **shutdown**: on JVM shutdown hook / signal, write `\u001B` to server's stdin

The launcher never interprets the `ServerArgs` bytes — it just pipes them through. This keeps it completely decoupled from the `ServerArgs` format and ES serialization.

### Phase 5: Wire up the handoff via startup scripts

The handoff between the preparer and the launcher is done by the platform startup scripts. The preparer runs first, writes the launch descriptor, and exits. The startup script then invokes the launcher with the descriptor.

#### Linux / macOS (`bin/elasticsearch`)

The shell script runs the preparer JVM first. When it exits successfully, the script uses `exec` to replace itself with the launcher JVM:

```bash
# Run the preparer
java ... -m org.elasticsearch.server.cli ...
if [ $? -ne 0 ]; then exit $?; fi

# Replace this shell with the launcher — launcher becomes the process
exec java ... -m org.elasticsearch.server.launcher <descriptor-path>
```

Using `exec` is critical for containerized deployments (Docker/Kubernetes). It ensures the launcher becomes PID 1 (or inherits the PID of the shell), which means:
- `docker stop` sends SIGTERM directly to the launcher
- The launcher handles graceful shutdown by sending the shutdown marker to the server
- No intermediate shell process lingers in the process tree

#### Windows (`bin/elasticsearch.bat`)

Windows has no equivalent to `exec`. The batch script runs the two programs sequentially:

```bat
rem Run the preparer
java ... -m org.elasticsearch.server.cli ...
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

rem Run the launcher (bat script waits for it)
java ... -m org.elasticsearch.server.launcher <descriptor-path>
exit /b %ERRORLEVEL%
```

This is fine for Windows because:
- Windows is not used in container deployments, so PID 1 semantics don't apply
- Ctrl+C console control events are delivered to all processes in the console group, so the launcher receives them directly
- The batch script staying alive as a parent is harmless

#### Platform summary

| Platform | Startup script | Handoff mechanism | Launcher becomes PID 1? |
|---|---|---|---|
| Linux/macOS | `bin/elasticsearch` (shell) | Run preparer; `exec` launcher | Yes (via `exec`) |
| Windows | `bin/elasticsearch.bat` | Run preparer; run launcher sequentially | No (bat script is parent, harmless) |

## What Moves Where

| Responsibility | Current Location | After Refactoring |
|---|---|---|
| CLI option parsing | `ServerCli` | `ServerCli` (preparer) |
| Secure settings loading | `ServerCli` | `ServerCli` (preparer) |
| Security auto-config | `ServerCli` | `ServerCli` (preparer) |
| Plugin sync | `ServerCli` | `ServerCli` (preparer) |
| Temp dir setup | `ServerProcessUtils` | `ServerCli` (preparer) |
| JVM options computation | `JvmOptionsParser` | `ServerCli` (preparer) |
| Command line construction | `ServerProcessBuilder` | `ServerCli` (preparer, writes to descriptor) |
| Launch descriptor format | *(new)* | **Shared library** |
| Process spawning | `ServerProcessBuilder` | **Launcher** |
| ServerArgs stdin send | `ServerProcessBuilder` | **Launcher** (raw byte copy) |
| Stderr pump + ready marker | `ErrorPumpThread` | **Launcher** (reimplemented, JDK-only) |
| Wait / detach / stop | `ServerProcess` | **Launcher** |

## New Project Structure

```
libs/
  server-launcher-common/          <-- NEW: shared utility library (JDK-only)
    build.gradle
    src/main/java/.../
      LaunchDescriptor.java
      ProcessUtil.java

distribution/tools/
  server-cli/                      <-- MODIFIED: preparer only, no longer spawns server
    build.gradle                   (add dependency on server-launcher-common)
    src/main/java/.../
      ServerCli.java               (modified)
      JvmOptionsParser.java        (unchanged)
      ... (other preparer classes unchanged)

  server-launcher/                 <-- NEW: minimal launcher
    build.gradle                   (depends only on server-launcher-common)
    src/main/java/.../
      ServerLauncher.java
      ErrorPumpThread.java
      ServerProcess.java
```

## Debugging Support

The launcher supports a `--dump` flag for inspecting the launch descriptor:

```bash
java -m org.elasticsearch.server.launcher --dump <descriptor-path>
```

This reads the binary descriptor file and prints it in a human-readable section-based text format (see Phase 1 example), then exits without launching the server. Useful for diagnosing startup issues.

## Future Improvement: Avoid the Temp File

The current plan uses a temp file to pass the descriptor from the preparer to the launcher. A potential improvement is to eliminate the file entirely by passing the descriptor as a **base64-encoded environment variable**:

```bash
ES_LAUNCH_DESCRIPTOR=$(java ... preparer ...)
if [ $? -ne 0 ]; then exit $?; fi
export ES_LAUNCH_DESCRIPTOR
exec java ... launcher
```

The launcher would read `System.getenv("ES_LAUNCH_DESCRIPTOR")`, base64-decode it (using `java.util.Base64`, JDK-only), and deserialize. This works cleanly with `exec` since environment variables are inherited across `exec`.

On Windows (where `exec` is not needed), a simple pipe would work instead:

```bat
java ... preparer ... | java ... launcher
```

This approach eliminates temp file creation and cleanup. The descriptor (~15-35KB base64-encoded) fits comfortably within Linux/macOS environment variable limits. Windows avoids the concern entirely by using a pipe.
