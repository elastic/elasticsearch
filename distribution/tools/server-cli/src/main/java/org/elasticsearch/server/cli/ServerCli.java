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
import org.elasticsearch.cli.CliToolProvider;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.server.cli.ServerProcess.Options;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.server.cli.JvmOptionsParser.determineJvmOptions;

class ServerCli extends EnvironmentAwareCommand {

    private static final Logger logger = LogManager.getLogger(ServerCli.class);

    private final OptionSpecBuilder versionOption;
    private final OptionSpecBuilder daemonizeOption;
    private final OptionSpec<Path> pidfileOption;
    private final OptionSpecBuilder quietOption;
    private final OptionSpec<String> enrollmentTokenOption;

    private volatile ServerProcess server;

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

        if (options.valuesOf(enrollmentTokenOption).size() > 1) {
            throw new UserException(ExitCodes.USAGE, "Multiple --enrollment-token parameters are not allowed");
        }
        Options serverOptions = new Options(
            options.has(daemonizeOption),
            options.has(quietOption),
            options.valueOf(pidfileOption),
            options.valueOf(enrollmentTokenOption));

        this.server = new ServerProcess(terminal, processInfo, serverOptions, env, () -> createEnv(options, processInfo));

        // if we are daemonized and we got the all-clear signal, we can exit cleanly
        if (server.isReady() && options.has(daemonizeOption)) {
            logger.info("Subprocess is ready and we are daemonized, exiting...");
            server.detach();
            this.server = null; // clear the handle, we don't want to shut it down now that we are started
            return;
        }

        // we are running in the foreground, so wait for the server to exit, or rethrow a startup error if we failed
        server.waitFor();
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

    @Override
    public void close() {
        var server = this.server; // read volatile into local

        if (server != null) {
            server.stop();
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
