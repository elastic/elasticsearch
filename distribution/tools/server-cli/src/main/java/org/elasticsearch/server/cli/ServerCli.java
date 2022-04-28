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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.bootstrap.BootstrapInfo.SERVER_READY_MARKER;
import static org.elasticsearch.bootstrap.BootstrapInfo.USER_EXCEPTION_MARKER;

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
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (options.nonOptionArguments().isEmpty() == false) {
            throw new UserException(ExitCodes.USAGE, "Positional arguments not allowed, found " + options.nonOptionArguments());
        }
        if (options.has(versionOption)) {
            printVersion(terminal);
            return;
        }

        // setup security
        final SecureString keystorePassword = getKeystorePassword(env.configFile(), terminal);
        autoConfigureSecurity(terminal, options, keystorePassword);
        // reload settings since auto security might have changed them
        // TODO: don't recreate if security settings were not changed
        env = createEnv(options);

        // determine process environment and arguments
        Map<String, String> envVars = new HashMap<>(this.envVars);
        Path tempDir = TempDirectory.setup(envVars);
        List<String> jvmOptions = JvmOptionsParser.determineOptions(
            env.configFile(),
            env.pluginsFile(),
            tempDir,
            envVars.get("ES_JAVA_OPTS")
        );
        // jvmOptions.add("-Des.path.conf=" + env.configFile());
        jvmOptions.add("-Des.distribution.type=" + sysprops.get("es.distribution.type"));
        var args = createArgs(options, keystorePassword, env);

        final Process process = createProcess(jvmOptions, envVars);
        try (var out = new OutputStreamStreamOutput(process.getOutputStream())) {
            args.writeTo(out);
        }
        String userExceptionMsg = null;
        boolean ready = false;
        InputStream err = process.getErrorStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(err));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty() == false && line.charAt(0) == USER_EXCEPTION_MARKER) {
                userExceptionMsg = line.substring(1);
                break;
            } else if (line.isEmpty() == false && line.charAt(0) == SERVER_READY_MARKER) {
                ready = true;
                // The server closes stderr right after this message, but for some unknown reason
                // the pipe closing does not close this end of the pipe, so we must explicitly
                // break out of this loop, or we will block forever on the next read.
                break;
            } else {
                terminal.getErrorWriter().println(line);
            }
        }

        if (ready && args.daemonize()) {
            closeStreams(process);
            return;
        }

        int code = process.waitFor();
        terminal.flush();
        System.out.println("exited: " + code);
        if (code != ExitCodes.OK) {
            throw new UserException(code, userExceptionMsg);
        }
    }

    private void closeStreams(Process process) throws IOException {
        IOUtils.close(process.getOutputStream(), process.getInputStream(), process.getErrorStream());
    }

    private void printVersion(Terminal terminal) {
        final String versionOutput = String.format(
            Locale.ROOT,
            "Version: %s, Build: %s/%s/%s/%s, JVM: %s",
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

    private void autoConfigureSecurity(Terminal terminal, OptionSet options, SecureString keystorePassword) throws Exception {
        KeystorePasswordTerminal autoConfigTerminal = new KeystorePasswordTerminal(terminal, keystorePassword);

        // reconstitute command lines
        List<?> settingValues = options.asMap().get(settingOption);
        List<String> args = new ArrayList<>();
        settingValues.forEach(v -> {
            args.add("-E");
            args.add(v.toString());
        });

        String autoConfigLibs = "modules/x-pack-core,modules/x-pack-security,lib/tools/security-cli";
        Command autoConfigNode = CliToolProvider.load("auto-configure-node", autoConfigLibs).create();
        if (options.has(enrollmentTokenOption)) {
            final String enrollmentToken = enrollmentTokenOption.value(options);
            args.add("--enrollment-token");
            args.add(enrollmentToken);
            int ret = autoConfigNode.main(args.toArray(new String[0]), autoConfigTerminal);
            if (ret != 0) {
                throw new UserException(ret, "Auto security enrollment failed");
            }
        } else {
            int ret = autoConfigNode.main(args.toArray(new String[0]), autoConfigTerminal);
            switch (ret) {
                case ExitCodes.OK, ExitCodes.CANT_CREATE, ExitCodes.CONFIG, ExitCodes.NOOP:
                    break;
                default:
                    throw new UserException(ret, "Auto security enrollment failed");
            }
        }
    }

    private ServerArgs createArgs(OptionSet options, SecureString keystorePassword, Environment env) {
        final boolean daemonize = options.has(daemonizeOption);
        final boolean quiet = options.has(quietOption);
        final Path pidFile = pidfileOption.value(options);

        return new ServerArgs(daemonize, quiet, pidFile, keystorePassword, env.settings(), env.configFile());
    }

    private Process createProcess(List<String> jvmOptions, Map<String, String> envVars) throws IOException {
        Path esHome = PathUtils.get("");
        Path javaHome = PathUtils.get(sysprops.get("java.home"));
        List<String> command = new ArrayList<>();
        boolean isWindows = sysprops.get("os.name").startsWith("Windows");
        command.add(javaHome.resolve("bin").resolve("java" + (isWindows ? ".exe" : "")).toString());
        command.addAll(jvmOptions);
        command.add("-cp");
        command.add(esHome.resolve("lib").resolve("*").toString());
        command.add("org.elasticsearch.bootstrap.Elasticsearch");

        var builder = new ProcessBuilder(command);
        builder.environment().putAll(envVars);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        return builder.start();
    }
}
