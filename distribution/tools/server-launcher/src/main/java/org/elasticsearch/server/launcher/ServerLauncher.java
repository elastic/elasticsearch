/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.launcher;

import org.elasticsearch.server.launcher.common.LaunchDescriptor;

import java.io.File;
import java.io.IOException;
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
 * <p> This program has zero Elasticsearch dependencies beyond the shared launcher-common library.
 */
public class ServerLauncher {

    private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private static volatile ServerProcess server;

    public static void main(String[] args) throws Exception {
        if (args.length >= 2 && "--dump".equals(args[0])) {
            LaunchDescriptor descriptor = LaunchDescriptor.readFrom(Path.of(args[1]));
            System.out.println(descriptor.toHumanReadable());
            return;
        }

        String tempDir = setupTempDir();

        int preparerExit = runPreparer(args, tempDir);
        if (preparerExit != 0) {
            System.exit(preparerExit);
        }

        Path descriptorPath = Path.of(tempDir, LaunchDescriptor.DESCRIPTOR_FILENAME);
        if (Files.exists(descriptorPath) == false) {
            return;
        }

        LaunchDescriptor descriptor = LaunchDescriptor.readFrom(descriptorPath);
        Files.deleteIfExists(descriptorPath);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            synchronized (shuttingDown) {
                shuttingDown.set(true);
                if (server != null) {
                    try {
                        server.stop();
                    } catch (IOException e) {
                        System.err.println("Error stopping server: " + e.getMessage());
                    }
                }
            }
        }, "server-launcher-shutdown"));

        server = startServer(descriptor, pb -> {
            try {
                return pb.start();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        if (descriptor.daemonize()) {
            server.detach();
            return;
        }

        int exitCode = server.waitFor();
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static String setupTempDir() throws IOException {
        String existing = System.getenv("ES_TMPDIR");
        if (existing != null) {
            Path p = Path.of(existing);
            if (Files.exists(p) == false) {
                System.err.println("Error: ES_TMPDIR does not exist: " + existing);
                System.exit(1);
            }
            if (Files.isDirectory(p) == false) {
                System.err.println("Error: ES_TMPDIR is not a directory: " + existing);
                System.exit(1);
            }
            return existing;
        }
        boolean isWindows = System.getProperty("os.name", "").startsWith("Windows");
        Path tempDir;
        if (isWindows) {
            tempDir = Path.of(System.getProperty("java.io.tmpdir"), "elasticsearch");
            Files.createDirectories(tempDir);
        } else {
            tempDir = Files.createTempDirectory("elasticsearch-");
        }
        return tempDir.toString();
    }

    private static int runPreparer(String[] userArgs, String tempDir) throws IOException, InterruptedException {
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

        command.add("-Dcli.name=server");
        command.add("-Dcli.libs=lib/tools/server-cli");
        command.add("-Des.path.home=" + esHome);
        command.add("-Des.path.conf=" + esPathConf);
        if (esDistType != null) {
            command.add("-Des.distribution.type=" + esDistType);
        }
        if (javaType != null) {
            command.add("-Des.java.type=" + javaType);
        }
        command.add("-cp");
        command.add(classpath);
        command.add("org.elasticsearch.launcher.CliToolLauncher");

        command.addAll(Arrays.asList(userArgs));

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.inheritIO();
        pb.environment().put("ES_TMPDIR", tempDir);

        Process process = pb.start();
        return process.waitFor();
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            System.err.println("Error: required environment variable " + name + " is not set");
            System.exit(1);
        }
        return value;
    }

    static ServerProcess startServer(LaunchDescriptor descriptor, Function<ProcessBuilder, Process> processStarter) throws Exception {
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
                System.err.println("Elasticsearch died while starting up, exit code: " + exitCode);
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

    private static void ensureWorkingDirExists(String workingDir) throws Exception {
        Path path = Path.of(workingDir);
        if (Files.exists(path) && Files.isDirectory(path) == false) {
            System.err.println("Error: working directory exists but is not a directory: " + workingDir);
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
}
