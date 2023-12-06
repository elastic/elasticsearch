/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Holds options and arguments needed to start a server process
 */
public class ServerProcessBuilder {
    private final Terminal terminal;
    private final String command;
    private final List<String> jvmOptions;
    private final List<String> jvmArgs;
    private final Map<String, String> environment;
    private final ServerArgs serverArgs;
    private final ServerProcess.ProcessStarter processStarter;

    private ServerProcessBuilder(
        Terminal terminal,
        String command,
        List<String> jvmOptions,
        List<String> otherOptions,
        Map<String, String> environment,
        ServerArgs serverArgs,
        ServerProcess.ProcessStarter processStarter
    ) {
        this.terminal = terminal;
        this.command = command;
        this.jvmOptions = jvmOptions;
        this.jvmArgs = otherOptions;
        this.environment = environment;
        this.serverArgs = serverArgs;
        this.processStarter = processStarter;
    }

    List<String> jvmOptions() {
        return jvmOptions;
    }

    List<String> jvmArgs() {
        return jvmArgs;
    }

    Map<String, String> environment() {
        return environment;
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

    interface ServerProcessBuilderOptions {
        ServerProcessBuilderOptions withOptionsBuilder(ServerProcess.OptionsBuilder optionsBuilder);

        ServerProcessBuilderOptions withProcessStarter(ServerProcess.ProcessStarter processStarter);
    }

    /**
     * Returns a ServerProcessBuilder instance by using the provided arguments, process info, environment and options builder.
     * The environment and temp directories are checked and, if needed, adjusted/created to be compliant with what the
     * server process is expecting.
     *
     * @param terminal        A terminal to connect the standard inputs and outputs to for the new process.
     * @param processInfo     Info about the current process, for passing through to the subprocess.
     * @param serverArgs      Arguments to the server process.
     * @return A builder to construct a ServerProcess
     * @throws UserException    if there is a problem with the configuration (e.g. files or directory permissions)
     */
    public static ServerProcessBuilder create(Terminal terminal, ProcessInfo processInfo, ServerArgs serverArgs) throws UserException {
        return create(terminal, processInfo, serverArgs, options -> {});
    }

    static ServerProcessBuilder create(
        Terminal terminal,
        ProcessInfo processInfo,
        ServerArgs serverArgs,
        Consumer<ServerProcessBuilderOptions> additionalOptionsSupplier
    ) throws UserException {
        var serverProcessBuilderOptions = new ServerProcessBuilderOptions() {
            private ServerProcess.OptionsBuilder optionsBuilder = JvmOptionsParser::determineJvmOptions;
            private ServerProcess.ProcessStarter processStarter = ProcessBuilder::start;

            @Override
            public ServerProcessBuilderOptions withOptionsBuilder(ServerProcess.OptionsBuilder optionsBuilder) {
                this.optionsBuilder = optionsBuilder;
                return this;
            }

            @Override
            public ServerProcessBuilderOptions withProcessStarter(ServerProcess.ProcessStarter processStarter) {
                this.processStarter = processStarter;
                return this;
            }
        };
        additionalOptionsSupplier.accept(serverProcessBuilderOptions);

        try {
            Map<String, String> envVars = new HashMap<>(processInfo.envVars());

            Path tempDir = setupTempDir(processInfo, envVars.remove("ES_TMPDIR"));
            if (envVars.containsKey("LIBFFI_TMPDIR") == false) {
                envVars.put("LIBFFI_TMPDIR", tempDir.toString());
            }

            List<String> jvmOptions = serverProcessBuilderOptions.optionsBuilder.getJvmOptions(
                serverArgs,
                serverArgs.configDir(),
                tempDir,
                envVars.remove("ES_JAVA_OPTS")
            );
            // also pass through distribution type
            jvmOptions.add("-Des.distribution.type=" + processInfo.sysprops().get("es.distribution.type"));

            Path esHome = processInfo.workingDir();
            Path javaHome = PathUtils.get(processInfo.sysprops().get("java.home"));

            boolean isWindows = processInfo.sysprops().get("os.name").startsWith("Windows");
            String command = javaHome.resolve("bin").resolve("java" + (isWindows ? ".exe" : "")).toString();

            var jvmArgs = List.of(
                "--module-path", esHome.resolve("lib").toString(),
                // Special circumstances require some modules (not depended on by the main server module) to be explicitly added:
                "--add-modules=jdk.net", // needed to reflectively set extended socket options
                // we control the module path, which may have additional modules not required by server
                "--add-modules=ALL-MODULE-PATH", "-m", "org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch"
            );

            return new ServerProcessBuilder(
                terminal,
                command,
                Collections.unmodifiableList(jvmOptions),
                jvmArgs,
                Collections.unmodifiableMap(envVars),
                serverArgs,
                serverProcessBuilderOptions.processStarter
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Start a server in a new process.
     *
     * @return A running server process that is ready for requests
     * @throws UserException        If the process failed during bootstrap
     */
    public ServerProcess start() throws UserException {
        Process jvmProcess = null;
        ErrorPumpThread errorPump;

        boolean success = false;
        try {
            jvmProcess = createProcess();
            errorPump = new ErrorPumpThread(terminal.getErrorWriter(), jvmProcess.getErrorStream());
            errorPump.start();
            sendArgs(serverArgs, jvmProcess.getOutputStream());

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

    private Process createProcess() throws InterruptedException, IOException {

        var builder = new ProcessBuilder(
            Stream.concat(Stream.of(command), Stream.concat(jvmOptions.stream(), jvmArgs.stream())).toList()
        );
        builder.environment().putAll(environment);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        return processStarter.start(builder);
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
