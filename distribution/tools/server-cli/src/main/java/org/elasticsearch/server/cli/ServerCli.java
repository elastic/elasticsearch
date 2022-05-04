/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import joptsimple.util.PathConverter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
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
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.bootstrap.BootstrapInfo.SERVER_READY_MARKER;
import static org.elasticsearch.bootstrap.BootstrapInfo.USER_EXCEPTION_MARKER;
import static org.elasticsearch.server.cli.JvmOptionsParser.determineJvmOptions;

class ServerCli extends EnvironmentAwareCommand {

    private static final Logger logger = LogManager.getLogger(ServerCli.class);

    private final OptionSpecBuilder versionOption;
    private final OptionSpecBuilder daemonizeOption;
    private final OptionSpec<Path> pidfileOption;
    private final OptionSpecBuilder quietOption;
    private final OptionSpec<String> enrollmentTokenOption;

    private volatile Process process;

    // visible for testing
    ServerCli() {
        super("Starts Elasticsearch"); // we configure logging later so we override the base class from configuring logging
        versionOption = parser.acceptsAll(Arrays.asList("V", "version"), "Prints Elasticsearch version information and exits");
        daemonizeOption = parser.acceptsAll(Arrays.asList("d", "daemonize"), "Starts Elasticsearch in the background")
            .availableUnless(versionOption);
        pidfileOption = parser.acceptsAll(Arrays.asList("p", "pidfile"), "Creates a pid file in the specified path on start")
            .availableUnless(versionOption)
            .withRequiredArg()
            .withValuesConvertedBy(new PathConverter());
        quietOption = parser.acceptsAll(Arrays.asList("q", "quiet"), "Turns off standard output/error streams logging in console")
            .availableUnless(versionOption)
            .availableUnless(daemonizeOption);
        enrollmentTokenOption = parser.accepts("enrollment-token", "An existing enrollment token for securely joining a cluster")
            .availableUnless(versionOption)
            .withRequiredArg();
    }

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        if (options.nonOptionArguments().isEmpty() == false) {
            throw new UserException(ExitCodes.USAGE, "Positional arguments not allowed, found " + options.nonOptionArguments());
        }
        if (options.has(versionOption)) {
            printVersion(terminal);
            return;
        }

        // setup security
        final SecureString keystorePassword = getKeystorePassword(env.configFile(), terminal);
        env = autoConfigureSecurity(terminal, options, processInfo, env, keystorePassword);

        // start Elasticsearch, stashing the process into a volatile so the close via the shutdown handler will kill the process
        this.process = createProcess(processInfo, env.configFile(), env.pluginsFile());
        final Process process = this.process; // avoid volatile read locally, we only set it once above
        logger.info("ES PID: " + process.pid());
        final ErrorPumpThread errorPump = new ErrorPumpThread(terminal, process.getErrorStream());
        errorPump.start();
        sendArgs(options, keystorePassword, env, process.getOutputStream());

        // Read from stderr until we get a signal back that ES is either ready or it had an error.
        // If we are running in the foreground, this pump will never exit.
        errorPump.join();
        if (errorPump.ioFailure != null) {
            throw errorPump.ioFailure;
        }

        // if we are daemonized and we got the all-clear signal, we can exit cleanly
        if (errorPump.ready && options.has(daemonizeOption)) {
            this.process = null; // clear the process handle, we don't want to shut it down now that we are started
            closeStreams(process);
            return;
        }

        // We pass any ES error code through UserException. If the message was set,
        // then it is a real UserException, otherwise it is just the error code and a null message.
        int code = process.waitFor();
        if (code != ExitCodes.OK) {
            throw new UserException(code, errorPump.userExceptionMsg);
        }
    }

    private void closeStreams(Process process) throws IOException {
        IOUtils.close(process.getOutputStream(), process.getInputStream(), process.getErrorStream());
    }

    private void printVersion(Terminal terminal) {
        final String versionOutput = String.format(
            Locale.ROOT,
            "Version: %s, Build: %s/%s/%s, JVM: %s",
            Build.CURRENT.qualifiedVersion(),
            Build.CURRENT.type().displayName(),
            Build.CURRENT.hash(),
            Build.CURRENT.date(),
            JvmInfo.jvmInfo().version()
        );
        terminal.println(versionOutput);
    }

    private SecureString getKeystorePassword(Path configDir, Terminal terminal) throws IOException {
        try (KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir)) {
            if (keystore != null && keystore.hasPassword()) {
                logger.info("keystore has password");
                return new SecureString(terminal.readSecret(KeyStoreWrapper.PROMPT));
            } else {
                logger.info("keystore does not have password");
                return new SecureString(new char[0]);
            }
        }
    }

    private Environment autoConfigureSecurity(
        Terminal terminal,
        OptionSet options,
        ProcessInfo processInfo,
        Environment env,
        SecureString keystorePassword
    ) throws Exception {
        if (options.valuesOf(enrollmentTokenOption).size() > 1) {
            throw new UserException(ExitCodes.USAGE, "Multiple --enrollment-token parameters are not allowed");
        }

        logger.info("Running auto config");
        String autoConfigLibs = "modules/x-pack-core,modules/x-pack-security,lib/tools/security-cli";
        Command cmd = loadTool("auto-configure-node", autoConfigLibs);
        assert cmd instanceof EnvironmentAwareCommand;
        @SuppressWarnings("raw")
        var autoConfigNode = (EnvironmentAwareCommand) cmd;
        final String[] autoConfigArgs;
        if (options.has(enrollmentTokenOption)) {
            autoConfigArgs = new String[] { "--enrollment-token", options.valueOf(enrollmentTokenOption) };
        } else {
            autoConfigArgs = new String[0];
        }
        OptionSet autoConfigOptions = autoConfigNode.parseOptions(autoConfigArgs);

        boolean changed = true;
        try (var autoConfigTerminal = new KeystorePasswordTerminal(terminal, keystorePassword.clone())) {
            autoConfigNode.execute(autoConfigTerminal, autoConfigOptions, env, processInfo);
        } catch (UserException e) {
            logger.error("GOT USER EXCEPTION from auto config", e);
            boolean okCode = switch (e.exitCode) {
                // these exit codes cover the cases where auto-conf cannot run but the node should NOT be prevented from starting as usual
                // eg the node is restarted, is already configured in an incompatible way, or the file system permissions do not allow it
                case ExitCodes.CANT_CREATE, ExitCodes.CONFIG, ExitCodes.NOOP -> true;
                default -> false;
            };
            if (options.has(enrollmentTokenOption) == false && okCode) {
                // we still want to print the error, just don't fail startup
                terminal.errorPrintln(e.getMessage());
                changed = false;
            } else {
                throw e;
            }
        }
        if (changed) {
            // reload settings since auto security changed them
            env = createEnv(options, processInfo);
        }
        return env;

    }

    private void sendArgs(OptionSet options, SecureString keystorePassword, Environment env, OutputStream processStdin) {
        final boolean daemonize = options.has(daemonizeOption);
        final boolean quiet = options.has(quietOption);
        final Path pidFile = pidfileOption.value(options);
        final var args = new ServerArgs(daemonize, quiet, pidFile, keystorePassword, env.settings(), env.configFile());

        try (var out = new OutputStreamStreamOutput(processStdin)) {
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

    static class ErrorPumpThread extends Thread {
        private final BufferedReader reader;
        private final PrintWriter writer;
        private volatile boolean ready = false;
        private volatile String userExceptionMsg;
        private volatile IOException ioFailure;

        private ErrorPumpThread(Terminal terminal, InputStream err) {
            super("server-cli error pump");
            this.reader = new BufferedReader(new InputStreamReader(err, StandardCharsets.UTF_8));
            this.writer = terminal.getErrorWriter();
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty() == false && line.charAt(0) == USER_EXCEPTION_MARKER) {
                        userExceptionMsg = line.substring(1);
                        logger.error("Got user exception: " + userExceptionMsg);
                    } else if (line.isEmpty() == false && line.charAt(0) == SERVER_READY_MARKER) {
                        // The server closes stderr right after this message, but for some unknown reason
                        // the pipe closing does not close this end of the pipe, so we must explicitly
                        // break out of this loop, or we will block forever on the next read.
                        logger.info("Got ready signal");
                        ready = true;
                        return;
                    } else {
                        writer.println(line);
                    }
                }
            } catch (IOException e) {
                logger.error("Got io exception in pump", e);
                ioFailure = e;
            }
            writer.flush();
        }
    }

    @Override
    public void close() {
        if (process != null) {
            logger.info("Terminating subprocess");
            process.destroy();

            // TODO: we should wait adn then forcibly kill, but after how long? this is configurable in eg systemd,
            // but that is giving us. For systemd we should sd_notify that the main pid is different. For SIGINT
            // we should have some default max time before sending SIGKILL? we should also maybe block on the main thread
            // exiting after it the process is terminated
            while (true) {
                try {
                    logger.info("Waiting for subprocess");
                    // TODO: check the exit code!!
                    process.waitFor();
                    break;
                } catch (InterruptedException ignore) {
                    // retry
                }
            }
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
