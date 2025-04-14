/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * This class is used to create a {@link ServerProcess}.
 * Each ServerProcessBuilder instance manages a collection of process attributes. The {@link ServerProcessBuilder#start()} method creates
 * a new {@link ServerProcess} instance with those attributes.
 *
 * Each process builder manages these process attributes:
 * - a temporary directory
 * - process info to pass through to the new Java subprocess
 * - the command line arguments to run Elasticsearch
 * - a list of JVM options to be passed to the Elasticsearch Java process
 * - a {@link Terminal} to read input and write output from/to the cli console
 */
public class ServerProcessBuilder {
    private Path tempDir;
    private ServerArgs serverArgs;
    private ProcessInfo processInfo;
    private List<String> jvmOptions;
    private Terminal terminal;

    // this allows mocking the process building by tests
    interface ProcessStarter {
        Process start(ProcessBuilder pb) throws IOException;
    }

    /**
     * Specifies the temporary directory to be used by the server process
     */
    public ServerProcessBuilder withTempDir(Path tempDir) {
        this.tempDir = tempDir;
        return this;
    }

    /**
     * Specifies the process info to pass through to the new Java subprocess
     */
    public ServerProcessBuilder withProcessInfo(ProcessInfo processInfo) {
        this.processInfo = processInfo;
        return this;
    }

    /**
     * Specifies the command line arguments to run Elasticsearch
     */
    public ServerProcessBuilder withServerArgs(ServerArgs serverArgs) {
        this.serverArgs = serverArgs;
        return this;
    }

    /**
     * Specifies the JVM options to be passed to the Elasticsearch Java process
     */
    public ServerProcessBuilder withJvmOptions(List<String> jvmOptions) {
        this.jvmOptions = jvmOptions;
        return this;
    }

    /**
     * Specifies the {@link Terminal} to use for reading input and writing output from/to the cli console
     */
    public ServerProcessBuilder withTerminal(Terminal terminal) {
        this.terminal = terminal;
        return this;
    }

    private Map<String, String> getEnvironment() {
        Map<String, String> envVars = new HashMap<>(processInfo.envVars());

        envVars.remove("ES_TMPDIR");
        if (envVars.containsKey("LIBFFI_TMPDIR") == false) {
            envVars.put("LIBFFI_TMPDIR", tempDir.toString());
        }
        envVars.remove("ES_JAVA_OPTS");

        return envVars;
    }

    private List<String> getJvmArgs() {
        Path esHome = processInfo.workingDir();
        return List.of(
            "--module-path",
            esHome.resolve("lib").toString(),
            // Special circumstances require some modules (not depended on by the main server module) to be explicitly added:
            "--add-modules=jdk.net", // needed to reflectively set extended socket options
            "--add-modules=jdk.management.agent", // needed by external debug tools to grab thread and heap dumps
            // we control the module path, which may have additional modules not required by server
            "--add-modules=ALL-MODULE-PATH",
            "-m",
            "org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch"
        );
    }

    private String getCommand() {
        Path javaHome = PathUtils.get(processInfo.sysprops().get("java.home"));

        boolean isWindows = processInfo.sysprops().get("os.name").startsWith("Windows");
        return javaHome.resolve("bin").resolve("java" + (isWindows ? ".exe" : "")).toString();
    }

    /**
     * Start a server in a new process.
     *
     * @return A running server process that is ready for requests
     * @throws UserException        If the process failed during bootstrap
     */
    public ServerProcess start() throws UserException {
        return start(ProcessBuilder::start);
    }

    private void ensureWorkingDirExists() throws UserException {
        Path workingDir = serverArgs.logsDir();
        try {
            Files.createDirectories(workingDir);
        } catch (FileAlreadyExistsException e) {
            throw new UserException(ExitCodes.CONFIG, "Logs dir [" + workingDir + "] exists but is not a directory", e);
        } catch (IOException e) {
            throw new UserException(ExitCodes.CONFIG, "Unable to create logs dir [" + workingDir + "]", e);
        }
    }

    private static void checkRequiredArgument(Object argument, String argumentName) {
        if (argument == null) {
            throw new IllegalStateException(
                Strings.format("'%s' is a required argument and needs to be specified before calling start()", argumentName)
            );
        }
    }

    // package private for testing
    ServerProcess start(ProcessStarter processStarter) throws UserException {
        checkRequiredArgument(tempDir, "tempDir");
        checkRequiredArgument(serverArgs, "serverArgs");
        checkRequiredArgument(processInfo, "processInfo");
        checkRequiredArgument(jvmOptions, "jvmOptions");
        checkRequiredArgument(terminal, "terminal");

        ensureWorkingDirExists();

        Process jvmProcess = null;
        ErrorPumpThread errorPump;

        boolean success = false;
        try {
            jvmProcess = createProcess(getCommand(), getJvmArgs(), jvmOptions, getEnvironment(), serverArgs.logsDir(), processStarter);
            errorPump = new ErrorPumpThread(terminal, jvmProcess.getErrorStream());
            errorPump.start();
            sendArgs(serverArgs, jvmProcess.getOutputStream());

            boolean serverOk = errorPump.waitUntilReady();
            if (serverOk == false) {
                // something bad happened, wait for the process to exit then rethrow
                int exitCode = jvmProcess.waitFor();
                throw new UserException(exitCode, "Elasticsearch died while starting up");
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

    private static Process createProcess(
        String command,
        List<String> jvmArgs,
        List<String> jvmOptions,
        Map<String, String> environment,
        Path workingDir,
        ProcessStarter processStarter
    ) throws InterruptedException, IOException {

        var builder = new ProcessBuilder(Stream.concat(Stream.of(command), Stream.concat(jvmOptions.stream(), jvmArgs.stream())).toList());
        builder.environment().putAll(environment);
        setWorkingDir(builder, workingDir);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        return processStarter.start(builder);
    }

    @SuppressForbidden(reason = "ProcessBuilder takes File")
    private static void setWorkingDir(ProcessBuilder builder, Path path) {
        builder.directory(path.toFile());
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
    }
}
