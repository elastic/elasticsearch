/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import joptsimple.OptionSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.CliToolProvider;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
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

class ServerProcess {

    private static final Logger logger = LogManager.getLogger(ServerProcess.class);

    record Options(boolean daemonize, boolean quiet, Path pidFile, String enrollmentToken) {}

    interface EnvReloader {
        Environment reload() throws UserException;
    }

    private final Process jvmProcess;
    private final ErrorPumpThread errorPump;
    private final CountDownLatch readyOrDead = new CountDownLatch(1);
    private volatile boolean ready;
    private volatile String userExceptionMsg;
    private volatile IOException ioFailure;

    ServerProcess(Terminal terminal, ProcessInfo processInfo, Options options,
                        Environment env, EnvReloader envReloader) throws Exception {
        // setup security
        final SecureString keystorePassword = getKeystorePassword(env.configFile(), terminal);
        env = autoConfigureSecurity(terminal, processInfo, env, keystorePassword, options.enrollmentToken, envReloader);

        // start Elasticsearch, stashing the process into a volatile so the close via the shutdown handler will kill the process
        this.jvmProcess = createProcess(processInfo, env.configFile(), env.pluginsFile());
        this.errorPump = new ErrorPumpThread(terminal.getErrorWriter(), jvmProcess.getErrorStream());
        errorPump.start();
        logger.info("ES PID: " + jvmProcess.pid());
        sendArgs(options, keystorePassword, env, jvmProcess.getOutputStream());

        // Read from stderr until we get a signal back that ES is either ready or it had an error.
        readyOrDead.await();
        if (ioFailure != null) {
            throw ioFailure;
        }
    }

    boolean isReady() {
        return ready;
    }

    void detach() throws IOException {
        try {
            // the server will close its streams when we want to detach, so we wait to finish reading stderr
            errorPump.join();
        } catch (InterruptedException e) {
            // how can this happen?
        }
        IOUtils.close(jvmProcess.getOutputStream(), jvmProcess.getInputStream(), jvmProcess.getErrorStream());
    }

    void waitFor() throws UserException, InterruptedException {
        // todo: should we catch interrupted and retry?
        int exitCode = jvmProcess.waitFor();
        if (exitCode != ExitCodes.OK) {
            throw new UserException(exitCode, userExceptionMsg);
        }
    }

    void stop() {
        logger.info("Terminating subprocess");
        try {
            OutputStream os = jvmProcess.getOutputStream();
            os.write(BootstrapInfo.SERVER_SHUTDOWN_MARKER);
            os.flush();
        } catch (IOException e) {
            // process is already effectively dead, fall through to wait for it, or should we SIGKILL?
        }

        try {
            errorPump.join();
        } catch (InterruptedException e) {
            // what cases can this happen during shutdown?
        }
        //process.destroy();

        // TODO: we should wait adn then forcibly kill, but after how long? this is configurable in eg systemd,
        // but that is giving us. For systemd we should sd_notify that the main pid is different. For SIGINT
        // we should have some default max time before sending SIGKILL? we should also maybe block on the main thread
        // exiting after it the process is terminated
        while (true) {
            try {
                logger.info("Waiting for subprocess");
                int exitCode = jvmProcess.waitFor();
                // TODO: what error conditions exist? if we got a SIGINT, SIGTERM, these are sent tot he child, so it should
                // exit the same
                break;
            } catch (InterruptedException ignore) {
                // retry
            }
        }
    }

    private static SecureString getKeystorePassword(Path configDir, Terminal terminal) throws IOException {
        try (KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir)) {
            if (keystore != null && keystore.hasPassword()) {
                return new SecureString(terminal.readSecret(KeyStoreWrapper.PROMPT));
            } else {
                return new SecureString(new char[0]);
            }
        }
    }

    private Environment autoConfigureSecurity(
        Terminal terminal,
        ProcessInfo processInfo,
        Environment env,
        SecureString keystorePassword,
        String enrollmentToken,
        EnvReloader envReloader
    ) throws Exception {

        String autoConfigLibs = "modules/x-pack-core,modules/x-pack-security,lib/tools/security-cli";
        Command cmd = loadTool("auto-configure-node", autoConfigLibs);
        assert cmd instanceof EnvironmentAwareCommand;
        @SuppressWarnings("raw")
        var autoConfigNode = (EnvironmentAwareCommand) cmd;
        final String[] autoConfigArgs;
        if (enrollmentToken != null) {
            autoConfigArgs = new String[] { "--enrollment-token", enrollmentToken };
        } else {
            autoConfigArgs = new String[0];
        }
        OptionSet autoConfigOptions = autoConfigNode.parseOptions(autoConfigArgs);

        boolean changed = true;
        try (var autoConfigTerminal = new KeystorePasswordTerminal(terminal, keystorePassword.clone())) {
            autoConfigNode.execute(autoConfigTerminal, autoConfigOptions, env, processInfo);
        } catch (UserException e) {
            boolean okCode = switch (e.exitCode) {
                // these exit codes cover the cases where auto-conf cannot run but the node should NOT be prevented from starting as usual
                // eg the node is restarted, is already configured in an incompatible way, or the file system permissions do not allow it
                case ExitCodes.CANT_CREATE, ExitCodes.CONFIG, ExitCodes.NOOP -> true;
                default -> false;
            };
            if (enrollmentToken == null && okCode) {
                // we still want to print the error, just don't fail startup
                terminal.errorPrintln(e.getMessage());
                changed = false;
            } else {
                throw e;
            }
        }
        if (changed) {
            // reload settings since auto security changed them
            env = envReloader.reload();
        }
        return env;

    }

    private void sendArgs(Options options, SecureString keystorePassword, Environment env, OutputStream processStdin) {
        final boolean daemonize = options.daemonize();
        final boolean quiet = options.quiet();
        final Path pidFile = options.pidFile();
        final var args = new ServerArgs(daemonize, quiet, pidFile, keystorePassword, env.settings(), env.configFile());

        // DO NOT close the underlying process stdin, since we need to be able to write to it to signal exit
        var out = new OutputStreamStreamOutput(processStdin);
        try {
            args.writeTo(out);
        } catch (IOException ignore) {
            // A failure to write here means the process has problems, and it will die anyways. We let this fall through
            // so the pump thread can complete, writing out the actual error. All we get here is the failure to write to
            // the process pipe, which isn't helpful to print.
        }
        keystorePassword.close();
    }

    private Process createProcess(ProcessInfo processInfo, Path configDir, Path pluginsDir) throws Exception {
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

        private ErrorPumpThread(PrintWriter errOutput, InputStream errInput) {
            super("server-cli error pump");
            this.reader = new BufferedReader(new InputStreamReader(errInput, StandardCharsets.UTF_8));
            this.writer = errOutput;
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
                        // The server closes stderr right after this message, but for some unknown reason
                        // the pipe closing does not close this end of the pipe, so we must explicitly
                        // break out of this loop, or we will block forever on the next read.
                        logger.info("Got ready signal");
                        ready = true;
                        readyOrDead.countDown();
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
    protected List<String> getJvmOptions(Path configDir, Path pluginsDir, Path tmpDir, String envOptions) throws Exception {
        return new ArrayList<>(determineJvmOptions(configDir, pluginsDir, tmpDir, envOptions));
    }

    // protected to allow tests to override
    protected Process startProcess(ProcessBuilder builder) throws IOException {
        return builder.start();
    }

    // protected to allow tests to override
    protected Command loadTool(String toolname, String libs) {
        return CliToolProvider.load(toolname, libs).create();
    }
}
