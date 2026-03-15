# Windows Service Control: Replacing Procrun for Service Operations

## Background

Elasticsearch can run as a Windows service, using Apache Commons Daemon (procrun) for support.
The `windows-service-cli` module provides CLI subcommands for install, remove, start, stop, and
a GUI manager, all of which currently delegate to the procrun executable (`elasticsearch-service-x64.exe`).

Procrun has proven unreliable for service control operations, particularly during stop:
- It may return before the service has fully stopped.
- It may incorrectly report errors during state transitions.
- It suffers from race conditions with the Windows Service Control Manager (SCM).

These issues were documented and partially addressed in commit 45e38c1, which switched the
**packaging tests** from procrun to `sc.exe` for start, stop, and delete, and added a busy-wait
polling loop for stop.

The goal is to replace procrun for the **production** service control commands (start, stop, remove)
while keeping procrun for install and manager, which are inherently procrun-specific operations.

## Options Considered

### Option 1: Shell out to `sc.exe`

Replace the procrun-based start, stop, and remove commands with calls to the standard Windows
`sc.exe` command-line tool.

**Pros:**
- Simple implementation, mirrors what the tests already do.
- `sc.exe` is always available on Windows (`C:\Windows\System32`).
- No native code or FFI complexity.

**Cons:**
- Still spawns external processes.
- `sc.exe stop` returns immediately with STOP_PENDING, so a polling loop is still needed.
- The polling loop itself requires spawning additional `sc.exe query` processes.
- Output parsing is locale-dependent.

**Status:** Implemented on branch `spacetime/windows-service-control-sc`.

### Option 2: Panama FFI direct calls to Windows SCM APIs

Use the Java Foreign Function & Memory API (Panama FFI) to call the Windows Service Control Manager
APIs from `advapi32.dll` directly, without spawning any external processes.

**Pros:**
- No process spawning; direct API calls are faster and more reliable.
- Full access to `SERVICE_STATUS_PROCESS` fields (`dwWaitHint`, `dwCheckPoint`) for a proper
  SCM-compliant polling loop during stop.
- Better error handling via `GetLastError` codes; no locale-dependent output parsing.
- Consistent with existing patterns in the codebase (`JdkKernel32Library` in `libs/native`).

**Cons:**
- More code to write (struct layouts, method handles, handle lifecycle management).
- Requires careful handling of JDK version differences (Panama was preview in JDK 21, finalized in 22).

**Status:** Chosen approach. Implemented on branch `spacetime/windows-service-control-ffi`.

### Option 3: Add SCM bindings to `libs/native` (alongside `JdkKernel32Library`)

Same as Option 2, but placing the `advapi32.dll` bindings into the shared `libs/native` library.

**Rejected** because:
- The `NativeLibrary` interface is sealed; adding `Advapi32Library` would require modifying the
  sealed permits list, the `NativeLibraryProvider` infrastructure, and `JdkNativeLibraryProvider`.
- `NativeLibraryProvider` validates that all permitted subclasses have implementations, so every
  platform (Linux, Mac) would need to provide a stub for a Windows-only library.
- The SCM APIs are a narrow concern of the service CLI, not used by the server or any other module.

## Chosen Plan: Option 2 with a Separate MRJAR Library

### Architecture

Create a new library project `libs/windows-service-control` containing the Panama FFI bindings
for the Windows SCM APIs. The `windows-service-cli` module depends on this library.

The library is built as a Multi-Release JAR (using the `elasticsearch.mrjar` Gradle plugin) to
handle differences between JDK 21 (where Panama is a preview API) and JDK 22+ (where it is final).

### Windows APIs Required (from `advapi32.dll`)

| Function                 | Signature                                                    | Purpose                          |
|--------------------------|--------------------------------------------------------------|----------------------------------|
| `OpenSCManagerW`         | `(LPCWSTR, LPCWSTR, DWORD) -> SC_HANDLE`                    | Open SCM database                |
| `OpenServiceW`           | `(SC_HANDLE, LPCWSTR, DWORD) -> SC_HANDLE`                   | Open handle to existing service  |
| `StartServiceW`          | `(SC_HANDLE, DWORD, LPCWSTR*) -> BOOL`                       | Start a service                  |
| `ControlService`         | `(SC_HANDLE, DWORD, SERVICE_STATUS*) -> BOOL`                | Send stop control code           |
| `DeleteService`          | `(SC_HANDLE) -> BOOL`                                        | Mark service for deletion        |
| `QueryServiceStatusEx`   | `(SC_HANDLE, SC_STATUS_TYPE, LPBYTE, DWORD, LPDWORD) -> BOOL`| Poll service status              |
| `CloseServiceHandle`     | `(SC_HANDLE) -> BOOL`                                        | Close SCM/service handles        |

Plus `GetLastError` from `kernel32.dll` for error reporting.

### Key Data Structure

`SERVICE_STATUS_PROCESS` (36 bytes):
- `dwServiceType`, `dwCurrentState`, `dwControlsAccepted`, `dwWin32ExitCode`,
  `dwServiceSpecificExitCode`, `dwCheckPoint`, `dwWaitHint`, `dwProcessId`, `dwServiceFlags`

The `dwCurrentState` field provides the service state (STOPPED, RUNNING, STOP_PENDING, etc.).
The `dwWaitHint` and `dwCheckPoint` fields enable a proper SCM-compliant wait loop during stop.

### Project Structure

```
libs/windows-service-control/
  build.gradle                                    # elasticsearch.build + elasticsearch.mrjar
  src/main/java/org/elasticsearch/service/windows/
    WindowsServiceControl.java                    # Public API: start, stop, delete, queryStatus
    ScmHandle.java                                # AutoCloseable wrapper for SC_HANDLE pairs
    Advapi32.java                                 # Panama FFI bindings for advapi32.dll
    PanamaUtil.java                               # Small utility methods for Arena/VarHandle compat
  src/main22/java/org/elasticsearch/service/windows/
    PanamaUtil.java                               # JDK 22+ overrides for changed Panama APIs
```

### JDK 21 vs 22+ Compatibility

The Panama FFI API changed between JDK 21 (preview) and JDK 22 (final) in a few ways that
affect us:

| API                          | JDK 21 (preview)              | JDK 22+ (final)                     |
|------------------------------|-------------------------------|--------------------------------------|
| `Arena.allocateArray`        | `arena.allocateArray(layout, count)` | `arena.allocate(layout, count)` |
| `MemoryLayout.varHandle`     | Returns `VarHandle` directly  | Returns `VarHandle` with extra offset coordinate |
| `MemorySegment.getUtf8String`| `segment.getUtf8String(off)`  | `segment.getString(off)`             |

These are handled by `PanamaUtil` in the base source set (JDK 21 preview API) with overrides
in `src/main22/` for the finalized API. This is the same pattern used by `libs/native` with its
`ArenaUtil` and `MemorySegmentUtil` classes. The utility methods are copied rather than shared,
since they are trivial and avoid coupling to the `libs/native` module.

### Scope of Changes to `windows-service-cli`

- `WindowsServiceStartCommand`, `WindowsServiceStopCommand`, `WindowsServiceRemoveCommand` are
  rewritten to use the new library instead of extending `ProcrunCommand`.
- A new `ScmCommand` base class (or similar) replaces `ProcrunCommand` for these three commands.
- `WindowsServiceInstallCommand` and `WindowsServiceManagerCommand` remain unchanged (still use procrun).
- `ProcrunCommand` is kept for install and manager.
- Tests are updated to mock at the `WindowsServiceControl` API level rather than at the process level.

### What Stays with Procrun

- **Install** (`//IS/`): Procrun's install writes JVM configuration (classpath, start/stop class,
  JVM options, etc.) to the Windows Registry under its own keys. `CreateServiceW` alone cannot
  replace this; it only registers the service binary path with the SCM.
- **Manager** (`prunmgr.exe`): A GUI tool that must be launched as a process.
