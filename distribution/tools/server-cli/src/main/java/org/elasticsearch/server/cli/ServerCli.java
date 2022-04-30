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
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.bootstrap.BootstrapInfo.SERVER_READY_MARKER;
import static org.elasticsearch.bootstrap.BootstrapInfo.USER_EXCEPTION_MARKER;
import static org.elasticsearch.server.cli.JvmOptionsParser.determineJvmOptions;

class ServerCli extends EnvironmentAwareCommand {
    private final OptionSpecBuilder versionOption;
    private final OptionSpecBuilder daemonizeOption;
    private final OptionSpec<Path> pidfileOption;
    private final OptionSpecBuilder quietOption;
    private final OptionSpec<String> enrollmentTokenOption;

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
        var autoConfigTerminal = new KeystorePasswordTerminal(terminal, keystorePassword);
        runAutoConfigTool(autoConfigTerminal, options, processInfo, env);
        // reload settings since auto security might have changed them
        // TODO: don't recreate if security settings were not changed
        env = createEnv(options, processInfo);

        // start Elasticsearch
        final Process process = createProcess(processInfo, env);

        // send arguments
        var args = createArgs(options, keystorePassword, env);
        try (var out = new OutputStreamStreamOutput(process.getOutputStream())) {
            args.writeTo(out);
        }
        keystorePassword.close();

        // Read from stderr until we get a signal back that ES is either ready or it had an error.
        // If we are running in the foreground, this pump will never exit.
        AtomicReference<String> userExceptionMsg = new AtomicReference<>();
        boolean ready = pumpStderr(terminal, process.getErrorStream(), userExceptionMsg);

        // if we are daemonized and we got the all-clear signal, we can exit cleanly
        if (ready && args.daemonize()) {
            closeStreams(process);
            return;
        }

        // We pass any ES error code through UserException. If the message was set,
        // then it is a real UserException, otherwise it is just the error code and a null message.
        int code = process.waitFor();
        if (code != ExitCodes.OK) {
            throw new UserException(code, userExceptionMsg.get());
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

    private SecureString getKeystorePassword(Path configDir, Terminal terminal) {
        try {
            KeyStoreWrapper keystore = KeyStoreWrapper.load(KeyStoreWrapper.keystorePath(configDir));
            if (keystore != null && keystore.hasPassword()) {
                return new SecureString(terminal.readSecret(KeyStoreWrapper.PROMPT));
            } else {
                return new SecureString(new char[0]);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void runAutoConfigTool(Terminal terminal, OptionSet options, ProcessInfo processInfo, Environment env) throws Exception {
        String autoConfigLibs = "modules/x-pack-core,modules/x-pack-security,lib/tools/security-cli";
        Command cmd = loadTool("auto-configure-node", autoConfigLibs);
        assert cmd instanceof EnvironmentAwareCommand;
        @SuppressWarnings("raw")
        var autoConfigNode = (EnvironmentAwareCommand) cmd;

        try {
            autoConfigNode.execute(terminal, options, env, processInfo);
        } catch (UserException e) {
            if (options.has(enrollmentTokenOption) == false) {
                // if we don't have an enrollment token then these exit codes are "ok"
                switch (e.exitCode) {
                    case ExitCodes.CANT_CREATE, ExitCodes.CONFIG, ExitCodes.NOOP:
                        return;
                }
            }
            throw e;
        }
    }

    private ServerArgs createArgs(OptionSet options, SecureString keystorePassword, Environment env) {
        final boolean daemonize = options.has(daemonizeOption);
        final boolean quiet = options.has(quietOption);
        final Path pidFile = pidfileOption.value(options);

        return new ServerArgs(daemonize, quiet, pidFile, keystorePassword, env.settings(), env.configFile());
    }

    private Process createProcess(ProcessInfo processInfo, Environment env) throws Exception {
        Map<String, String> envVars = new HashMap<>(processInfo.envVars());
        Path tempDir = TempDirectory.setup(envVars);
        List<String> jvmOptions = getJvmOptions(env.configFile(), env.pluginsFile(), tempDir, envVars.get("ES_JAVA_OPTS"));
        // jvmOptions.add("-Des.path.conf=" + env.configFile());
        jvmOptions.add("-Des.distribution.type=" + processInfo.sysprops().get("es.distribution.type"));

        Path esHome = processInfo.workingDir();
        Path javaHome = PathUtils.get(processInfo.sysprops().get("java.home"));
        List<String> command = new ArrayList<>();
        boolean isWindows = processInfo.sysprops().get("os.name").startsWith("Windows");
        command.add(javaHome.resolve("bin").resolve("java" + (isWindows ? ".exe" : "")).toString());
        command.addAll(jvmOptions);
        command.add("-cp");
        command.add(esHome.resolve("lib").resolve("*").toString());
        command.add("org.elasticsearch.bootstrap.Elasticsearch");

        var builder = new ProcessBuilder(command);
        builder.environment().putAll(envVars);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        return startProcess(builder);
    }

    private boolean pumpStderr(Terminal terminal, InputStream err, AtomicReference<String> userExceptionMsg) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(err, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty() == false && line.charAt(0) == USER_EXCEPTION_MARKER) {
                userExceptionMsg.set(line.substring(1));
                return false;
            } else if (line.isEmpty() == false && line.charAt(0) == SERVER_READY_MARKER) {
                // The server closes stderr right after this message, but for some unknown reason
                // the pipe closing does not close this end of the pipe, so we must explicitly
                // break out of this loop, or we will block forever on the next read.
                return true;
            } else {
                terminal.getErrorWriter().println(line);
            }
        }
        return false;
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
