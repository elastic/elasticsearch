/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.core.PathUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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

    public ServerProcessBuilder withTempDir(Path tempDir) {
        this.tempDir = tempDir;
        return this;
    }

    public ServerProcessBuilder withProcessInfo(ProcessInfo processInfo) {
        this.processInfo = processInfo;
        return this;
    }

    public ServerProcessBuilder withServerArgs(ServerArgs serverArgs) {
        this.serverArgs = serverArgs;
        return this;
    }

    public ServerProcessBuilder withJvmOptions(List<String> jvmOptions) {
        this.jvmOptions = jvmOptions;
        return this;
    }

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

    // package private for testing
    ServerProcess start(ProcessStarter processStarter) throws UserException {
        Process jvmProcess = null;
        ErrorPumpThread errorPump;

        boolean success = false;
        try {
            jvmProcess = createProcess(getCommand(), getJvmArgs(), getEnvironment(), processStarter);
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

    private Process createProcess(String command, List<String> jvmArgs, Map<String, String> environment, ProcessStarter processStarter)
        throws InterruptedException, IOException {

        var builder = new ProcessBuilder(Stream.concat(Stream.of(command), Stream.concat(jvmOptions.stream(), jvmArgs.stream())).toList());
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
