/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.launcher;

import org.elasticsearch.cli.terminal.Terminal;
import org.elasticsearch.server.launcher.common.LaunchDescriptor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Minimal launcher for the Elasticsearch server process.
 *
 * <p> This program is exec'd directly by the startup script. It spawns the preparer (server-cli)
 * as a child process, reads the resulting {@link LaunchDescriptor}, spawns the server JVM process,
 * pipes the serialized ServerArgs bytes to the server's stdin, pumps stderr for the ready marker,
 * and waits for the server to exit.
 *
 * <p> This program has zero Elasticsearch dependencies beyond the shared launcher-common and cli-terminal libraries.
 *
 * <p> Subclasses (e.g. {@code ServerlessServerLauncher}) can override lifecycle hooks to customize
 * behavior without duplicating shared logic.
 *
 * @param <D> the descriptor type, must extend {@link LaunchDescriptor}
 */
public class ServerLauncher<D extends LaunchDescriptor> {

    private final Terminal terminal = Terminal.DEFAULT;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private volatile ServerProcess server;

    public static void main(String[] args) throws Exception {
        new ServerLauncher<LaunchDescriptor>().run(args);
    }

    protected void run(String[] args) throws Exception {
        Process preparerProcess = startPreparer(args);
        PreparerOutputPump outputPump = new PreparerOutputPump(preparerProcess.getErrorStream(), System.out, System.err);
        outputPump.start();
        D descriptor = readDescriptorFromPreparer(preparerProcess);
        outputPump.drain();
        if (descriptor == null) {
            return;
        }

        installShutdownHook();

        Process[] rawProcessHolder = new Process[1];
        server = startServer(descriptor, pb -> {
            try {
                Process p = startProcess(pb);
                rawProcessHolder[0] = p;
                return p;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        onServerStarted(descriptor, server, rawProcessHolder[0]);

        if (descriptor.daemonize()) {
            server.detach();
            onDaemonize(descriptor);
            return;
        }

        int exitCode = server.waitFor();
        onServerExit(descriptor, exitCode);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    // ---- Overridable lifecycle hooks ----

    /**
     * Returns the CLI name passed as {@code -Dcli.name} to the preparer process.
     */
    protected String cliName() {
        return "server";
    }

    /**
     * Returns the CLI libs path passed as {@code -Dcli.libs} to the preparer process.
     */
    protected String cliLibs() {
        return "lib/tools/server-cli";
    }

    /**
     * Returns the name for the JVM shutdown hook thread.
     */
    protected String shutdownHookName() {
        return "server-launcher-shutdown";
    }

    /**
     * Returns extra system property names to forward from the launcher to the preparer process.
     */
    protected List<String> extraPreparerSystemProperties() {
        return List.of();
    }

    /**
     * Reads a launch descriptor from the preparer's stdout stream.
     * Returns null if no bytes were written (e.g. --version or --help was used).
     * <p>
     * Subclasses override this to read their specific descriptor type.
     */
    @SuppressWarnings("unchecked")
    protected D readDescriptorFromStream(InputStream in) throws IOException {
        byte[] bytes = in.readAllBytes();
        if (bytes.length == 0) {
            return null;
        }
        return (D) LaunchDescriptor.readFrom(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    /**
     * Starts a process from the given builder. Subclasses can override to capture the raw process.
     */
    protected Process startProcess(ProcessBuilder pb) throws IOException {
        return pb.start();
    }

    /**
     * Called after the server process has started and the ready marker has been received.
     */
    protected void onServerStarted(D descriptor, ServerProcess server, Process rawProcess) throws IOException {}

    /**
     * Called before returning in daemon mode, after the server has been detached.
     */
    protected void onDaemonize(D descriptor) {}

    /**
     * Called after the server process exits (non-daemon mode).
     */
    protected void onServerExit(D descriptor, int exitCode) {}

    // ---- Shared logic (private) ----

    private Process startPreparer(String[] userArgs) throws IOException {
        String java = requireEnv("JAVA");
        String esHome = requireEnv("ES_HOME");
        String esPathConf = requireEnv("ES_PATH_CONF");
        String esDistType = System.getenv("ES_DISTRIBUTION_TYPE");
        String javaType = System.getenv("JAVA_TYPE");
        String cliJavaOpts = System.getenv("CLI_JAVA_OPTS");

        String classpath = esHome
            + File.separator
            + "lib"
            + File.separator
            + "*"
            + File.pathSeparator
            + esHome
            + File.separator
            + "lib"
            + File.separator
            + "cli-launcher"
            + File.separator
            + "*";

        List<String> command = new ArrayList<>();
        command.add(java);

        if (cliJavaOpts != null && cliJavaOpts.isBlank() == false) {
            Collections.addAll(command, cliJavaOpts.trim().split("\\s+"));
        }

        command.add("-Dcli.name=" + cliName());
        command.add("-Dcli.libs=" + cliLibs());
        command.add("-Des.path.home=" + esHome);
        command.add("-Des.path.conf=" + esPathConf);
        if (esDistType != null) {
            command.add("-Des.distribution.type=" + esDistType);
        }
        if (javaType != null) {
            command.add("-Des.java.type=" + javaType);
        }

        for (String prop : extraPreparerSystemProperties()) {
            String value = System.getProperty(prop);
            if (value != null) {
                command.add("-D" + prop + "=" + value);
            }
        }

        command.add("-cp");
        command.add(classpath);
        command.add("org.elasticsearch.launcher.CliToolLauncher");

        command.addAll(Arrays.asList(userArgs));

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
        pb.redirectError(ProcessBuilder.Redirect.PIPE);
        pb.environment().put("ES_REDIRECT_STDOUT_TO_STDERR", "true");

        return pb.start();
    }

    private D readDescriptorFromPreparer(Process preparerProcess) throws Exception {
        D descriptor = null;
        Exception readException = null;
        try {
            descriptor = readDescriptorFromStream(preparerProcess.getInputStream());
        } catch (Exception e) {
            readException = e;
        }
        int preparerExit = preparerProcess.waitFor();
        if (preparerExit != 0) {
            System.exit(preparerExit);
        }
        if (readException != null) {
            if (readException instanceof IOException io) {
                throw new UncheckedIOException(io);
            }
            throw new RuntimeException(readException);
        }
        return descriptor;
    }

    private void installShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown, shutdownHookName()));
    }

    /**
     * Called by the shutdown hook when the JVM is terminating. Stops the server process.
     * Subclasses can override to add post-shutdown behavior (e.g. waiting for diagnostic collection).
     */
    protected void onShutdown() {
        synchronized (shuttingDown) {
            shuttingDown.set(true);
            if (server != null) {
                try {
                    server.stop();
                } catch (IOException e) {
                    terminal.errorPrintln("Error stopping server: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Starts the server process from the given descriptor and process starter function.
     * Package-visible for testing.
     */
    ServerProcess startServer(LaunchDescriptor descriptor, Function<ProcessBuilder, Process> processStarter) throws Exception {
        ensureWorkingDirExists(descriptor.workingDir());

        List<String> command = new ArrayList<>();
        command.add(descriptor.command());
        command.addAll(descriptor.jvmOptions());
        command.addAll(descriptor.jvmArgs());

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.environment().clear();
        pb.environment().putAll(descriptor.environment());
        pb.directory(new File(descriptor.workingDir()));
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        Process jvmProcess = null;
        ErrorPumpThread errorPump;
        boolean success = false;

        try {
            jvmProcess = processStarter.apply(pb);
            errorPump = new ErrorPumpThread(jvmProcess.getErrorStream(), System.err);
            errorPump.start();
            sendServerArgs(descriptor.serverArgsBytes(), jvmProcess.getOutputStream());

            boolean serverOk = errorPump.waitUntilReady();
            if (serverOk == false) {
                int exitCode = jvmProcess.waitFor();
                terminal.errorPrintln("Elasticsearch died while starting up, exit code: " + exitCode);
                System.exit(exitCode != 0 ? exitCode : 1);
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

    private void ensureWorkingDirExists(String workingDir) throws Exception {
        Path path = Path.of(workingDir);
        if (Files.exists(path) && Files.isDirectory(path) == false) {
            terminal.errorPrintln("Error: working directory exists but is not a directory: " + workingDir);
            System.exit(1);
        }
        Files.createDirectories(path);
    }

    private static void sendServerArgs(byte[] serverArgsBytes, OutputStream processStdin) {
        try {
            processStdin.write(serverArgsBytes);
            processStdin.flush();
        } catch (IOException ignore) {
            // A failure to write here means the process has problems, and it will die anyway.
            // The error pump thread will report the actual error.
        }
    }

    private String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            terminal.errorPrintln("Error: required environment variable " + name + " is not set");
            System.exit(1);
        }
        return value;
    }
}
