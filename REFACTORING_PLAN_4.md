# Refactoring Plan 4: Launcher Spawns the Preparer

## Goal

Move the preparer orchestration out of bash and into the server-launcher. Bash becomes a thin
shim that exports environment variables and execs the launcher. The launcher spawns the preparer
as a child process, reads the resulting launch descriptor file, and launches the server.

## Current vs New Flow

**Current:** bash runs preparer, checks result, execs launcher with descriptor path.

**New:** bash execs launcher immediately. Launcher runs preparer, reads descriptor, launches
server.

### Current process relationships

```
bin/elasticsearch (bash)
  ├── fork/exec: Preparer (server-cli JVM)
  │     └── Writes launch-descriptor.bin, exits
  ├── Check exit code, descriptor existence
  └── exec: Launcher (server-launcher, native or JVM)
        ├── Reads launch-descriptor.bin
        └── fork/exec: Elasticsearch Server JVM
```

### New process relationships

```
bin/elasticsearch (bash)
  └── exec: Launcher (server-launcher, native or JVM)
        ├── Sets up ES_TMPDIR
        ├── fork/exec: Preparer (server-cli JVM, inheritIO)
        │     └── Writes launch-descriptor.bin, exits
        ├── Checks exit code, descriptor existence
        ├── Reads launch-descriptor.bin
        └── fork/exec: Elasticsearch Server JVM
```

The launcher becomes PID 1 (in containers) immediately, before any preparation work begins.
The bash script is reduced to environment setup and a single `exec`.

## Alternatives Considered: Avoiding the Descriptor File

Since the launcher now spawns the preparer as a child process, we explored whether the
descriptor file could be eliminated entirely — passing the descriptor through an in-process
communication channel instead. Each approach was evaluated and dismissed. The reasoning is
recorded here so future readers don't re-derive it.

### Option 1: Pipe the descriptor through stdout

Have the preparer write the binary descriptor to stdout; the launcher captures it from the
child's output pipe.

**Problem: the preparer already writes to stdout.** `ServerCli.printVersion()` calls
`terminal.println()`, which writes to `System.out`. `Command.printHelp()` does the same. The
auto-config and sync-plugins tools also receive the Terminal and may write to stdout. These
user-visible messages would corrupt a binary descriptor stream.

### Option 2: Redirect Terminal to stderr, reserve stdout for the descriptor

Call `System.setOut(new PrintStream(System.err))` early in `CliToolLauncher`, save the original
`System.out`, and have the preparer write the descriptor to the saved stream at the end. The
launcher reads the descriptor from the child's stdout pipe while stderr (carrying all
user-visible output) is inherited.

**Problem: fragile.** The preparer dynamically loads and executes tools via
`CliToolProvider.load()` (auto-configure-node, sync-plugins). These tools — and any of their
transitive dependencies — might write to `System.out` directly, bypassing Terminal. Even if
nothing does so today, a single `System.out.println()` added anywhere in the dependency tree
in the future would silently corrupt the descriptor stream. We could add a safety net
(`System.setOut()` redirect), but any code that captured a reference to the original
`System.out` before the redirect would still bypass it.

This approach also requires changes to `CliToolLauncher` (to perform the redirect) and
`ServerCli` (to write to the saved stream), expanding the scope of the refactoring beyond the
launcher.

### Option 3: Named pipes (FIFOs)

Create a FIFO on the filesystem, pass its path to the preparer via environment variable. The
preparer writes the descriptor to the FIFO; the launcher reads from it.

**Problem: Unix-only.** Windows does not have POSIX FIFOs. Creating a FIFO from Java requires
exec'ing `mkfifo` (no JDK API exists). Named pipes also require careful open ordering to avoid
deadlock — one end blocks until the other end opens. And a FIFO is still a filesystem path, so
this is really just a different kind of file with more complexity.

### Option 4: Extra file descriptors

Create a pipe (e.g., fd 3), pass the write end to the child process. The preparer writes the
descriptor to fd 3; the launcher reads from the corresponding read end. This is the idiomatic
Unix approach for out-of-band IPC with child processes.

**Problem: not supported by Java's ProcessBuilder.** `ProcessBuilder` only supports fd 0
(stdin), fd 1 (stdout), and fd 2 (stderr). There is no API to pass additional file descriptors
to child processes. Implementing this would require JNI/JNA or a shell wrapper trick
(`bash -c 'exec 3>...'`), neither of which is cross-platform, and both of which contradict the
JDK-only philosophy of the launcher.

### Conclusion: the temp file is the right answer

The temp file approach is simple, robust, and cross-platform. It is ephemeral (~15–35 KB,
deleted immediately after reading) and the coupling is minimal (a shared constant for the
filename in the shared library). None of the alternatives offer a clear improvement, and all
introduce either fragility, platform-specificity, or scope expansion.

## Detailed Changes

### 1. Simplify `bin/elasticsearch`

**File:** `distribution/src/bin/elasticsearch`

The script shrinks from ~60 lines of orchestration to a simple env setup and exec:

- Source `elasticsearch-env` (unchanged)
- Set `CLI_JAVA_OPTS` (unchanged)
- Export environment variables needed by the launcher: `JAVA`, `ES_HOME`, `ES_PATH_CONF`,
  `ES_DISTRIBUTION_TYPE`, `JAVA_TYPE`, `CLI_JAVA_OPTS`
- Determine native vs JVM launcher
- `exec` the launcher, passing `"$@"` through

Remove: the preparer invocation, `ES_TMPDIR` creation, descriptor path logic, preparer exit
code check, descriptor existence check.

### 2. Simplify `bin/elasticsearch.bat`

**File:** `distribution/src/bin/elasticsearch.bat`

Same simplification for Windows. Remove preparer invocation and descriptor logic. Just set
variables and run the launcher with `%*`.

### 3. Add `DESCRIPTOR_FILENAME` to `LaunchDescriptor`

**File:** `libs/server-launcher-common/src/main/java/.../LaunchDescriptor.java`

Move the `DESCRIPTOR_FILENAME = "launch-descriptor.bin"` constant from `ServerCli` to
`LaunchDescriptor` in the shared library, since both the preparer and launcher need to agree
on this name.

### 4. Update `ServerCli` to use shared constant

**File:** `distribution/tools/server-cli/src/main/java/.../ServerCli.java`

Replace the local `DESCRIPTOR_FILENAME` with `LaunchDescriptor.DESCRIPTOR_FILENAME`.

### 5. Restructure `ServerLauncher.main()`

**File:** `distribution/tools/server-launcher/src/main/java/.../ServerLauncher.java`

New main flow:

```
main(args)
  if args[0] == "--dump" and args[1] is a path:
    read descriptor from path, dump, exit       // existing diagnostic mode

  setupTempDir()                                // create or validate ES_TMPDIR
  runPreparer(args)                             // spawn preparer, pass through user args
    if preparer exit != 0: exit with that code
    if no descriptor file: exit 0               // e.g. --version was used

  readDescriptor()                              // same as before
  registerShutdownHook()                        // same as before
  startServer()                                 // same as before (unchanged)
  waitFor() or detach()                         // same as before
```

Key new methods:

- **`setupTempDir()`** — reads `ES_TMPDIR` from environment; if not set, creates a temp
  directory (mirroring the logic currently in bash and `ServerProcessUtils`). On non-Windows,
  uses `Files.createTempDirectory("elasticsearch-")`; on Windows, uses
  `$java.io.tmpdir/elasticsearch`. Returns the path string.

- **`runPreparer(String[] userArgs)`** — builds the preparer command line from environment
  variables and runs it:
  - Reads `JAVA`, `ES_HOME`, `ES_PATH_CONF`, `ES_DISTRIBUTION_TYPE`, `JAVA_TYPE`,
    `CLI_JAVA_OPTS` from `System.getenv()`
  - Constructs the command:
    `$JAVA $CLI_JAVA_OPTS -Dcli.name=server -Dcli.libs=lib/tools/server-cli
    -Des.path.home=$ES_HOME -Des.path.conf=$ES_PATH_CONF
    -Des.distribution.type=$ES_DISTRIBUTION_TYPE -Des.java.type=$JAVA_TYPE
    -cp $ES_HOME/lib/*:$ES_HOME/lib/cli-launcher/*
    org.elasticsearch.launcher.CliToolLauncher <userArgs...>`
  - Sets `ES_TMPDIR` in the child process environment via `ProcessBuilder.environment()`
  - Uses `ProcessBuilder.inheritIO()` so the preparer's stdin/stdout/stderr pass through
    (important for keystore password prompts and error output)
  - Waits for the preparer process to exit
  - Returns the exit code

- **`startServer()`** — existing code, unchanged. Already handles building the ProcessBuilder
  from the descriptor, spawning the server, pumping stderr, etc.

### 6. No changes needed elsewhere

- **`ErrorPumpThread`**, **`ServerProcess`** — unchanged
- **`server-launcher-common`** — only the `DESCRIPTOR_FILENAME` constant is added
- **`server-cli` preparer logic** — unchanged (still writes descriptor to
  `$ES_TMPDIR/launch-descriptor.bin`)
- **`WindowsServiceDaemon`** — unchanged (bypasses the launcher entirely, directly spawns
  server)
- **Native image build** — unchanged (launcher remains JDK-only; `ProcessBuilder`,
  `System.getenv()`, `Files.createTempDirectory()` are all supported by native-image)
- **`ServerCliTests`** — unchanged (the `TestServerCli` overrides `prepareLaunch()` and never
  actually writes a descriptor)

## Key Design Decisions

- **Environment variables for preparer info**: The launcher reads `JAVA`, `ES_HOME`,
  `ES_PATH_CONF`, `ES_DISTRIBUTION_TYPE`, `JAVA_TYPE`, `CLI_JAVA_OPTS` from the environment.
  Bash exports these before exec. This is the simplest approach and works identically for the
  native binary and JVM launcher cases.

- **`cli.script` is omitted**: The preparer uses `cli.name=server`, so `cli.script` (which was
  set to `$0` in the bash script) is never read by `CliToolLauncher.getToolName()`. It can be
  safely omitted from the preparer command.

- **`ES_TMPDIR` setup moves to Java**: The launcher creates the temp directory if `ES_TMPDIR`
  is not set, matching the existing behavior. The launcher sets `ES_TMPDIR` in the preparer's
  environment via `ProcessBuilder.environment()`.

- **`inheritIO()` for the preparer**: The preparer's stdin/stdout/stderr are connected directly
  to the launcher's. This preserves terminal interaction (e.g., keystore password prompts) and
  error output.

- **`--dump` mode preserved**: `server-launcher --dump <descriptor-path>` continues to work as
  a standalone diagnostic tool for inspecting existing descriptor files.
