/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.server.cli.ProcessUtil.nonInterruptible;

/**
 * A helper to control a {@link Process} running the main Elasticsearch server.
 *
 * <p> The process can be started by calling {@link #start(Terminal, ProcessInfo, ServerArgs, KeyStoreWrapper)}.
 * The process is controlled by internally sending arguments and control signals on stdin,
 * and receiving control signals on stderr. The start method does not return until the
 * server is ready to process requests and has exited the bootstrap thread.
 *
 * <p> The caller starting a {@link ServerProcess} can then do one of several things:
 * <ul>
 *     <li>Block on the server process exiting, by calling {@link #waitFor()}</li>
 *     <li>Detach from the server process by calling {@link #detach()}</li>
 *     <li>Tell the server process to shutdown and wait for it to exit by calling {@link #stop()}</li>
 * </ul>
 */
public class ServerProcess {

    // the actual java process of the server
    private final Process jvmProcess;

    // the thread pumping stderr watching for state change messages
    private final ErrorPumpThread errorPump;

    // a flag marking whether the streams of the java subprocess have been closed
    private volatile boolean detached = false;

    ServerProcess(Process jvmProcess, ErrorPumpThread errorPump) {
        this.jvmProcess = jvmProcess;
        this.errorPump = errorPump;
    }

    // this allows mocking the process building by tests
    interface OptionsBuilder {
        List<String> getJvmOptions(ServerArgs args, KeyStoreWrapper keyStoreWrapper, Path configDir, Path tmpDir, String envOptions)
            throws InterruptedException, IOException, UserException;
    }

    // this allows mocking the process building by tests
    interface ProcessStarter {
        Process start(ProcessBuilder pb) throws IOException;
    }

    /**
     * Start a server in a new process.
     *
     * @param terminal        A terminal to connect the standard inputs and outputs to for the new process.
     * @param processInfo     Info about the current process, for passing through to the subprocess.
     * @param args            Arguments to the server process.
     * @param keystore        A keystore for accessing secrets.
     * @return A running server process that is ready for requests
     * @throws UserException If the process failed during bootstrap
     */
    public static ServerProcess start(Terminal terminal, ProcessInfo processInfo, ServerArgs args, KeyStoreWrapper keystore)
        throws UserException {
        return start(terminal, processInfo, args, keystore, JvmOptionsParser::determineJvmOptions, ProcessBuilder::start);
    }

    // package private so tests can mock options building and process starting
    static ServerProcess start(
        Terminal terminal,
        ProcessInfo processInfo,
        ServerArgs args,
        KeyStoreWrapper keystore,
        OptionsBuilder optionsBuilder,
        ProcessStarter processStarter
    ) throws UserException {
        Process jvmProcess = null;
        ErrorPumpThread errorPump;

        boolean success = false;
        try {
            jvmProcess = createProcess(args, keystore, processInfo, args.configDir(), optionsBuilder, processStarter);
            errorPump = new ErrorPumpThread(terminal.getErrorWriter(), jvmProcess.getErrorStream());
            errorPump.start();
            sendArgs(args, jvmProcess.getOutputStream());

            String errorMsg = errorPump.waitUntilReady();
            if (errorMsg != null) {
                // something bad happened, wait for the process to exit then rethrow
                int exitCode = jvmProcess.waitFor();
                throw new UserException(exitCode, errorMsg);
            }
            success = true;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (success == false && jvmProcess != null && jvmProcess.isAlive()) {
                jvmProcess.destroyForcibly();
            }
        }

        return new ServerProcess(jvmProcess, errorPump);
    }

    /**
     * Return the process id of the server.
     */
    public long pid() {
        return jvmProcess.pid();
    }

    /**
     * Detaches the server process from the current process, enabling the current process to exit.
     *
     * @throws IOException If an I/O error occurred while reading stderr or closing any of the standard streams
     */
    public synchronized void detach() throws IOException {
        errorPump.drain();
        IOUtils.close(jvmProcess.getOutputStream(), jvmProcess.getInputStream(), jvmProcess.getErrorStream());
        detached = true;
    }

    /**
     * Waits for the subprocess to exit.
     */
    public int waitFor() {
        errorPump.drain();
        return nonInterruptible(jvmProcess::waitFor);
    }

    /**
     * Stop the subprocess.
     *
     * <p> This sends a special code, {@link BootstrapInfo#SERVER_SHUTDOWN_MARKER} to the stdin
     * of the process, then waits for the process to exit.
     *
     * <p> Note that if {@link #detach()} has been called, this method is a no-op.
     */
    public synchronized void stop() {
        if (detached) {
            return;
        }

        sendShutdownMarker();
        waitFor(); // ignore exit code, we are already shutting down
    }

    private static void sendArgs(ServerArgs args, OutputStream processStdin) {
        // DO NOT close the underlying process stdin, since we need to be able to write to it to signal exit
        var out = new OutputStreamStreamOutput(processStdin);
        try {
            args.writeTo(out);
            out.flush();
        } catch (IOException ignore) {
            // A failure to write here means the process has problems, and it will die anyway. We let this fall through
            // so the pump thread can complete, writing out the actual error. All we get here is the failure to write to
            // the process pipe, which isn't helpful to print.
        }
        args.keystorePassword().close();
    }

    private void sendShutdownMarker() {
        try {
            OutputStream os = jvmProcess.getOutputStream();
            os.write(BootstrapInfo.SERVER_SHUTDOWN_MARKER);
            os.flush();
        } catch (IOException e) {
            // process is already effectively dead, fall through to wait for it, or should we SIGKILL?
        }
    }

    private static Process createProcess(
        ServerArgs args,
        KeyStoreWrapper keystore,
        ProcessInfo processInfo,
        Path configDir,
        OptionsBuilder optionsBuilder,
        ProcessStarter processStarter
    ) throws InterruptedException, IOException, UserException {
        Map<String, String> envVars = new HashMap<>(processInfo.envVars());
        Path tempDir = setupTempDir(processInfo, envVars.remove("ES_TMPDIR"));
        if (envVars.containsKey("LIBFFI_TMPDIR") == false) {
            envVars.put("LIBFFI_TMPDIR", tempDir.toString());
        }

        List<String> jvmOptions = optionsBuilder.getJvmOptions(args, keystore, configDir, tempDir, envVars.remove("ES_JAVA_OPTS"));
        // also pass through distribution type
        jvmOptions.add("-Des.distribution.type=" + processInfo.sysprops().get("es.distribution.type"));

        Path esHome = processInfo.workingDir();
        Path javaHome = PathUtils.get(processInfo.sysprops().get("java.home"));
        List<String> command = new ArrayList<>();
        boolean isWindows = processInfo.sysprops().get("os.name").startsWith("Windows");
        command.add(javaHome.resolve("bin").resolve("java" + (isWindows ? ".exe" : "")).toString());
        command.addAll(jvmOptions);
        command.add("--module-path");
        command.add(esHome.resolve("lib").toString());
        command.add("--add-modules=jdk.net"); // very special circumstance; explicit modules should typically not be added here
        command.add("-m");
        command.add("org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch");

        var builder = new ProcessBuilder(command);
        builder.environment().putAll(envVars);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        return processStarter.start(builder);
    }

    /**
     * Returns the java.io.tmpdir Elasticsearch should use, creating it if necessary.
     *
     * <p> On non-Windows OS, this will be created as a subdirectory of the default temporary directory.
     * Note that this causes the created temporary directory to be a private temporary directory.
     */
    private static Path setupTempDir(ProcessInfo processInfo, String tmpDirOverride) throws UserException, IOException {
        final Path path;
        if (tmpDirOverride != null) {
            path = Paths.get(tmpDirOverride);
            if (Files.exists(path) == false) {
                throw new UserException(ExitCodes.CONFIG, "Temporary directory [" + path + "] does not exist or is not accessible");
            }
            if (Files.isDirectory(path) == false) {
                throw new UserException(ExitCodes.CONFIG, "Temporary directory [" + path + "] is not a directory");
            }
        } else {
            if (processInfo.sysprops().get("os.name").startsWith("Windows")) {
                /*
                 * On Windows, we avoid creating a unique temporary directory per invocation lest
                 * we pollute the temporary directory. On other operating systems, temporary directories
                 * will be cleaned automatically via various mechanisms (e.g., systemd, or restarts).
                 */
                path = Paths.get(processInfo.sysprops().get("java.io.tmpdir"), "elasticsearch");
                Files.createDirectories(path);
            } else {
                path = createTempDirectory("elasticsearch-");
            }
        }
        return path;
    }

    @SuppressForbidden(reason = "Files#createTempDirectory(String, FileAttribute...)")
    private static Path createTempDirectory(final String prefix, final FileAttribute<?>... attrs) throws IOException {
        return Files.createTempDirectory(prefix, attrs);
    }
}
