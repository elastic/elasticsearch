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
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;

/**
 * The main CLI for running Elasticsearch.
 */
class ServerCli extends EnvironmentAwareCommand {

    private final OptionSpecBuilder versionOption;
    private final OptionSpecBuilder daemonizeOption;
    private final OptionSpec<Path> pidfileOption;
    private final OptionSpecBuilder quietOption;
    private final OptionSpec<String> enrollmentTokenOption;

    private volatile ServerProcess server;

    // visible for testing
    ServerCli() {
        super("Starts Elasticsearch"); // we configure logging later, so we override the base class from configuring logging
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

        validateConfig(options, env);

        try (KeyStoreWrapper keystore = KeyStoreWrapper.load(env.configFile())) {
            // setup security
            final SecureString keystorePassword = getKeystorePassword(keystore, terminal);
            env = autoConfigureSecurity(terminal, options, processInfo, env, keystorePassword);

            if (keystore != null) {
                keystore.decrypt(keystorePassword.getChars());
            }

            // install/remove plugins from elasticsearch-plugins.yml
            syncPlugins(terminal, env, processInfo);

            ServerArgs args = createArgs(options, env, keystorePassword, processInfo);
            this.server = startServer(terminal, processInfo, args, keystore);
        }

        if (options.has(daemonizeOption)) {
            server.detach();
            return;
        }

        // we are running in the foreground, so wait for the server to exit
        int exitCode = server.waitFor();
        if (exitCode != ExitCodes.OK) {
            throw new UserException(exitCode, "Elasticsearch exited unexpectedly");
        }
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

    private void validateConfig(OptionSet options, Environment env) throws UserException {
        if (options.valuesOf(enrollmentTokenOption).size() > 1) {
            throw new UserException(ExitCodes.USAGE, "Multiple --enrollment-token parameters are not allowed");
        }

        Path log4jConfig = env.configFile().resolve("log4j2.properties");
        if (Files.exists(log4jConfig) == false) {
            throw new UserException(ExitCodes.CONFIG, "Missing logging config file at " + log4jConfig);
        }
    }

    private static SecureString getKeystorePassword(KeyStoreWrapper keystore, Terminal terminal) {
        if (keystore != null && keystore.hasPassword()) {
            return new SecureString(terminal.readSecret(KeyStoreWrapper.PROMPT));
        } else {
            return new SecureString(new char[0]);
        }
    }

    private Environment autoConfigureSecurity(
        Terminal terminal,
        OptionSet options,
        ProcessInfo processInfo,
        Environment env,
        SecureString keystorePassword
    ) throws Exception {
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
            boolean okCode = switch (e.exitCode) {
                // these exit codes cover the cases where auto-conf cannot run but the node should NOT be prevented from starting as usual
                // e.g. the node is restarted, is already configured in an incompatible way, or the file system permissions do not allow it
                case ExitCodes.CANT_CREATE, ExitCodes.CONFIG, ExitCodes.NOOP -> true;
                default -> false;
            };
            if (options.has(enrollmentTokenOption) == false && okCode) {
                // we still want to print the error, just don't fail startup
                if (e.getMessage() != null) {
                    terminal.errorPrintln(e.getMessage());
                }
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

    private void syncPlugins(Terminal terminal, Environment env, ProcessInfo processInfo) throws Exception {
        String pluginCliLibs = "lib/tools/plugin-cli";
        Command cmd = loadTool("sync-plugins", pluginCliLibs);
        assert cmd instanceof EnvironmentAwareCommand;
        @SuppressWarnings("raw")
        var syncPlugins = (EnvironmentAwareCommand) cmd;
        syncPlugins.execute(terminal, syncPlugins.parseOptions(new String[0]), env, processInfo);
    }

    private void validatePidFile(Path pidFile) throws UserException {
        Path parent = pidFile.getParent();
        if (parent != null && Files.exists(parent) && Files.isDirectory(parent) == false) {
            throw new UserException(ExitCodes.USAGE, "pid file parent [" + parent + "] exists but is not a directory");
        }
        if (Files.exists(pidFile) && Files.isRegularFile(pidFile) == false) {
            throw new UserException(ExitCodes.USAGE, pidFile + " exists but is not a regular file");
        }
    }

    private ServerArgs createArgs(OptionSet options, Environment env, SecureString keystorePassword, ProcessInfo processInfo)
        throws UserException {
        boolean daemonize = options.has(daemonizeOption);
        boolean quiet = options.has(quietOption);
        Path pidFile = null;
        if (options.has(pidfileOption)) {
            pidFile = options.valueOf(pidfileOption);
            if (pidFile.isAbsolute() == false) {
                pidFile = processInfo.workingDir().resolve(pidFile.toString()).toAbsolutePath();
            }
            validatePidFile(pidFile);
        }
        return new ServerArgs(daemonize, quiet, pidFile, keystorePassword, env.settings(), env.configFile());
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop();
        }
    }

    // protected to allow tests to override
    protected Command loadTool(String toolname, String libs) {
        return CliToolProvider.load(toolname, libs).create();
    }

    // protected to allow tests to override
    protected ServerProcess startServer(Terminal terminal, ProcessInfo processInfo, ServerArgs args, KeyStoreWrapper keystore)
        throws UserException {
        return ServerProcess.start(terminal, processInfo, args, keystore);
    }
}
