/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.windows.service;

import joptsimple.OptionSet;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.terminal.Terminal;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.server.cli.JvmOptionsParser;
import org.elasticsearch.server.cli.MachineDependentHeap;
import org.elasticsearch.server.cli.ServerProcessUtils;
import org.elasticsearch.server.launcher.ErrorPumpThread;
import org.elasticsearch.server.launcher.ServerProcess;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Starts an Elasticsearch process, but does not wait for it to exit.
 * <p>
 * This class is expected to be run via Apache Procrun in a long-lived JVM that will call close
 * when the server should shut down.
 */
class WindowsServiceDaemon extends EnvironmentAwareCommand {

    private volatile ServerProcess server;

    WindowsServiceDaemon() {
        super("Starts and stops the Elasticsearch server process for a Windows Service");
    }

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        // the Windows service daemon doesn't support secure settings implementations other than the keystore
        try (var loadedSecrets = KeyStoreWrapper.bootstrap(env.configDir(), () -> new SecureString(new char[0]))) {
            var args = new ServerArgs(false, true, null, loadedSecrets, env.settings(), env.configDir(), env.logsDir());
            var tempDir = ServerProcessUtils.setupTempDir(processInfo);
            var jvmOptions = JvmOptionsParser.determineJvmOptions(args, processInfo, tempDir, new MachineDependentHeap());

            String command = getJavaCommand(processInfo);
            List<String> jvmArgs = getJvmArgs(processInfo);
            Map<String, String> environment = getEnvironment(processInfo, tempDir);
            byte[] serverArgsBytes = serializeServerArgs(args);

            this.server = startServer(command, jvmOptions, jvmArgs, environment, args.logsDir(), serverArgsBytes);
            // start does not return until the server is ready, and we do not wait for the process
        }
    }

    private static ServerProcess startServer(
        String command,
        List<String> jvmOptions,
        List<String> jvmArgs,
        Map<String, String> environment,
        Path workingDir,
        byte[] serverArgsBytes
    ) throws Exception {
        Files.createDirectories(workingDir);

        List<String> cmd = new ArrayList<>();
        cmd.add(command);
        cmd.addAll(jvmOptions);
        cmd.addAll(jvmArgs);

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().clear();
        pb.environment().putAll(environment);
        pb.directory(new File(workingDir.toString()));
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        Process jvmProcess = null;
        ErrorPumpThread errorPump;
        boolean success = false;

        try {
            jvmProcess = pb.start();
            errorPump = new ErrorPumpThread(jvmProcess.getErrorStream(), System.err);
            errorPump.start();
            sendServerArgs(serverArgsBytes, jvmProcess.getOutputStream());

            boolean serverOk = errorPump.waitUntilReady();
            if (serverOk == false) {
                int exitCode = jvmProcess.waitFor();
                throw new RuntimeException("Elasticsearch died while starting up, exit code: " + exitCode);
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

    private static String getJavaCommand(ProcessInfo processInfo) {
        Path javaHome = Path.of(processInfo.sysprops().get("java.home"));
        return javaHome.resolve("bin").resolve("java.exe").toString();
    }

    private static List<String> getJvmArgs(ProcessInfo processInfo) {
        Path esHome = processInfo.workingDir();
        return List.of(
            "--module-path",
            esHome.resolve("lib").toString(),
            "--add-modules=jdk.net",
            "--add-modules=jdk.management.agent",
            "--add-modules=ALL-MODULE-PATH",
            "-m",
            "org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch"
        );
    }

    private static Map<String, String> getEnvironment(ProcessInfo processInfo, Path tempDir) {
        Map<String, String> envVars = new HashMap<>(processInfo.envVars());
        envVars.remove("ES_TMPDIR");
        if (envVars.containsKey("LIBFFI_TMPDIR") == false) {
            envVars.put("LIBFFI_TMPDIR", tempDir.toString());
        }
        envVars.remove("ES_JAVA_OPTS");
        return envVars;
    }

    private static byte[] serializeServerArgs(ServerArgs args) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            args.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        }
    }

    private static void sendServerArgs(byte[] serverArgsBytes, OutputStream processStdin) {
        try {
            processStdin.write(serverArgsBytes);
            processStdin.flush();
        } catch (IOException ignore) {
            // A failure to write here means the process has problems, and it will die anyway.
        }
    }

    @Override
    public void close() throws IOException {
        if (server != null) {
            server.stop();
        }
    }
}
