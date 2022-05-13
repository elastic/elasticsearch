/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.bootstrap.BootstrapInfo.SERVER_READY_MARKER;
import static org.elasticsearch.bootstrap.BootstrapInfo.USER_EXCEPTION_MARKER;
import static org.elasticsearch.server.cli.JvmOptionsParser.determineJvmOptions;

class JavaServerProcess implements ServerProcess {
    private static final Logger logger = LogManager.getLogger(ServerProcess.class);

    private final Process jvmProcess;
    private final ErrorPumpThread errorPump;
    private final CountDownLatch readyOrDead = new CountDownLatch(1);
    private volatile boolean ready;
    private volatile String userExceptionMsg;
    private volatile IOException ioFailure;

    JavaServerProcess(Terminal terminal, ProcessInfo processInfo, ServerArgs args, Path pluginsDir) throws UserException {

        try {
            // start Elasticsearch, stashing the process into a volatile so the close via the shutdown handler will kill the process
            this.jvmProcess = createProcess(processInfo, args.configDir(), pluginsDir);
            this.errorPump = new ErrorPumpThread(terminal.getErrorWriter(), jvmProcess.getErrorStream(), false);
            errorPump.start();
            logger.info("ES PID: " + jvmProcess.pid());
            sendArgs(args, jvmProcess.getOutputStream());

            // Read from stderr until we get a signal back that ES is either ready or it had an error.
            readyOrDead.await();
            if (ioFailure != null) {
                throw ioFailure;
            }
            if (ready == false) {
                // something bad happened, wait for the process to exit then rethrow
                int exitCode = jvmProcess.waitFor();
                throw new UserException(exitCode, userExceptionMsg);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void detach() throws IOException {
        // TODO: use a flag to store the fact we are detached, so stop is a noop
        /*try {
            // the server will close its streams when we want to detach, so we wait to finish reading stderr
            errorPump.join();
        } catch (InterruptedException e) {
            // how can this happen?
        }*/
        IOUtils.close(jvmProcess.getOutputStream(), jvmProcess.getInputStream(), jvmProcess.getErrorStream());
    }

    @Override
    public void waitFor() throws UserException, InterruptedException {
        // todo: should we catch interrupted and retry?
        int exitCode = jvmProcess.waitFor();
        if (exitCode != ExitCodes.OK) {
            throw new UserException(exitCode, userExceptionMsg);
        }
    }

    @Override
    public void stop() {
        logger.info("Terminating subprocess");
        try {
            OutputStream os = jvmProcess.getOutputStream();
            os.write(BootstrapInfo.SERVER_SHUTDOWN_MARKER);
            os.flush();
        } catch (IOException e) {
            // process is already effectively dead, fall through to wait for it, or should we SIGKILL?
        }

        logger.info("Waiting for stderr to drain");
        try {
            errorPump.join();
        } catch (InterruptedException e) {
            // we don't interrupt this thread, so should not be possible
            throw new AssertionError(e);
        }

        // We wait forever here. Any timeout while waiting, eg in systemd would forcibly kill the
        // parent process. The server process would then break out of its block on stdin and exit.
        while (true) {
            try {
                logger.info("Waiting for subprocess");
                jvmProcess.waitFor();
                break;
            } catch (InterruptedException ignore) {
                // retry
            }
        }
    }

    private void sendArgs(ServerArgs args, OutputStream processStdin) {
        // DO NOT close the underlying process stdin, since we need to be able to write to it to signal exit
        var out = new OutputStreamStreamOutput(processStdin);
        try {
            args.writeTo(out);
            out.flush();
        } catch (IOException ignore) {
            // A failure to write here means the process has problems, and it will die anyways. We let this fall through
            // so the pump thread can complete, writing out the actual error. All we get here is the failure to write to
            // the process pipe, which isn't helpful to print.
        }
        args.keystorePassword().close();
    }

    private Process createProcess(ProcessInfo processInfo, Path configDir, Path pluginsDir) throws InterruptedException, IOException,
        UserException {
        Map<String, String> envVars = new HashMap<>(processInfo.envVars());
        Path tempDir = TempDirectory.setup(envVars);
        List<String> jvmOptions = getJvmOptions(configDir, pluginsDir, tempDir, envVars.get("ES_JAVA_OPTS"));
        // jvmOptions.add("-Des.path.conf=" + env.configFile());
        jvmOptions.add("-Des.distribution.type=" + processInfo.sysprops().get("es.distribution.type"));

        Path esHome = processInfo.workingDir();
        Path javaHome = PathUtils.get(processInfo.sysprops().get("java.home"));
        List<String> command = new ArrayList<>();
        boolean isWindows = processInfo.sysprops().get("os.name").startsWith("Windows");
        command.add(javaHome.resolve("bin").resolve("java" + (isWindows ? ".exe" : "")).toString());
        command.addAll(jvmOptions);
        command.add("-cp");
        // The '*' isn't allows by the windows filesystem, so we need to force it into the classpath after converting to a string.
        // Thankfully this will all go away when switching to modules, which take the directory instead of a glob.
        command.add(esHome.resolve("lib") + (isWindows ? "\\" : "/") + "*");
        command.add("org.elasticsearch.bootstrap.Elasticsearch");

        var builder = new ProcessBuilder(command);
        builder.environment().putAll(envVars);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        return startProcess(builder);
    }

    private class ErrorPumpThread extends Thread {
        private final BufferedReader reader;
        private final PrintWriter writer;
        private final boolean exitWhenReady;

        private ErrorPumpThread(PrintWriter errOutput, InputStream errInput, boolean exitWhenReady) {
            super("server-cli error pump");
            this.reader = new BufferedReader(new InputStreamReader(errInput, StandardCharsets.UTF_8));
            this.writer = errOutput;
            this.exitWhenReady = exitWhenReady;
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty() == false && line.charAt(0) == USER_EXCEPTION_MARKER) {
                        userExceptionMsg = line.substring(1);
                        logger.error("Got user exception: " + userExceptionMsg);
                        readyOrDead.countDown();
                    } else if (line.isEmpty() == false && line.charAt(0) == SERVER_READY_MARKER) {
                        logger.info("Got ready signal");
                        ready = true;
                        readyOrDead.countDown();
                        if (this.exitWhenReady) {
                            // The server closes stderr right after this message, but for some unknown reason
                            // the pipe closing does not close this end of the pipe, so we must explicitly
                            // break out of this loop, or we will block forever on the next read.
                            break;
                        }
                    } else {
                        writer.println(line);
                        logger.info("got stderr: " + line);
                    }
                }
            } catch (IOException e) {
                logger.error("Got io exception in pump", e);
                ioFailure = e;
            }
            writer.flush();
            readyOrDead.countDown();
        }
    }

    // protected to allow tests to override
    protected List<String> getJvmOptions(Path configDir, Path pluginsDir, Path tmpDir, String envOptions) throws InterruptedException,
        IOException, UserException {
        return new ArrayList<>(determineJvmOptions(configDir, pluginsDir, tmpDir, envOptions));
    }

    // protected to allow tests to override
    protected Process startProcess(ProcessBuilder builder) throws IOException {
        return builder.start();
    }
}
