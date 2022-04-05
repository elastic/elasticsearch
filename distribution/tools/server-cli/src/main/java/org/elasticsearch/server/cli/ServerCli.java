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
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.NodeValidationException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

class ServerCli extends EnvironmentAwareCommand {
    private final OptionSpecBuilder versionOption;
    private final OptionSpecBuilder daemonizeOption;
    private final OptionSpec<Path> pidfileOption;
    private final OptionSpecBuilder quietOption;
    private final OptionSpec<String> enrollmentTokenOption;

    // visible for testing
    ServerCli() {
        super("Starts Elasticsearch", () -> {}); // we configure logging later so we override the base class from configuring logging
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

    /**
     * Prints a message directing the user to look at the logs. A message is only printed if
     * logging has been configured.
     */
    static void printLogsSuggestion() {
        final String basePath = System.getProperty("es.logs.base_path");
        // It's possible to fail before logging has been configured, in which case there's no point
        // suggesting that the user look in the log file.
        if (basePath != null) {
            Terminal.DEFAULT.errorPrintln(
                "ERROR: Elasticsearch did not exit normally - check the logs at "
                    + basePath
                    + System.getProperty("file.separator")
                    + System.getProperty("es.logs.cluster_name")
                    + ".log"
            );
        }
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws UserException {
        if (options.nonOptionArguments().isEmpty() == false) {
            throw new UserException(ExitCodes.USAGE, "Positional arguments not allowed, found " + options.nonOptionArguments());
        }
        if (options.has(versionOption)) {
            final String versionOutput = String.format(
                Locale.ROOT,
                "Version: %s, Build: %s/%s/%s/%s, JVM: %s",
                Build.CURRENT.qualifiedVersion(),
                Build.CURRENT.flavor().displayName(),
                Build.CURRENT.type().displayName(),
                Build.CURRENT.hash(),
                Build.CURRENT.date(),
                JvmInfo.jvmInfo().version()
            );
            terminal.println(versionOutput);
            return;
        }

        final boolean daemonize = options.has(daemonizeOption);
        final Path pidFile = pidfileOption.value(options);
        final boolean quiet = options.has(quietOption);

        Map<String, String> envVars = new HashMap<>(System.getenv());
        Path tempDir = TempDirectory.initialize(envVars);
        SecureString keystorePassword = null;
        try {
            KeyStoreWrapper keystore = KeyStoreWrapper.load(KeyStoreWrapper.keystorePath(env.configFile()));
            if (keystore != null && keystore.hasPassword()) {
                keystorePassword = new SecureString(terminal.readSecret(KeyStoreWrapper.PROMPT, KeyStoreWrapper.MAX_PASSPHRASE_LENGTH));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }


        if (options.has(enrollmentTokenOption)) {
            final String enrollmentToken = enrollmentTokenOption.value(options);

        }
        /*
        if enrollment-token: auto configure node
        else:  attempt auto configure, exit on codes not in [80, 73, 78]
        */

        // TODO: this settings is wrong, needs to account for docker stuff through env and also auto enrollment, needs to be late binding
        ServerArgs serverArgs = new ServerArgs(daemonize, pidFile, env.settings());

        List<String> jvmOptions = JvmOptionsParser.determine(env.configFile(), env.pluginsFile(), tempDir, envVars.get("ES_JAVA_OPTS"));
        jvmOptions.add("-Des.path.conf=" + env.configFile());
        jvmOptions.add("-Des.distribution.flavor=" + System.getProperty("es.distribution.flavor"));
        jvmOptions.add("-Des.distribution.type=" + System.getProperty("es.distribution.type"));
        jvmOptions.add("-Des.bundled_jdk=" + System.getProperty("es.bundled_jdk"));

        /*
        create process
        - set command
          1. figure out java path
          2. add jvm options
          3. add classpath
          4. add main class
          5. add args
        - set env
        - set redirects

        launch java process
        if (daemon) exit (TODO: wait until server is started)
        wait on process

         */
        String esHome = System.getProperty("es.path.home");
        Path javaHome = PathUtils.get(System.getProperty("java.home"));
        List<String> command = new ArrayList<>();
        // TODO: fix this so it works on windows
        command.add(javaHome.resolve("bin").resolve("java").toString());
        command.addAll(jvmOptions);
        command.add("-cp");
        // TODO: fix this to work on windows
        command.add(esHome + "/lib/*");
        command.add("org.elasticsearch.bootstrap.Elasticsearch");
        System.out.println("command: " + command);

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().putAll(envVars);
        builder.inheritIO();
        builder.redirectInput(ProcessBuilder.Redirect.PIPE);
        try {
            Process process = builder.start();
            try (var out = new OutputStreamStreamOutput(process.getOutputStream())) {
                serverArgs.writeTo(out);
            }
            // TODO: handle daemonize flag
            int code = process.waitFor();
            System.out.println("exited: " + code);
            System.exit(code);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new UserException(ExitCodes.IO_ERROR, "Interrupted while waiting for Elasticsearch process");
        }

        // TODO: check the java.io.tmpdir
        // a misconfigured java.io.tmpdir can cause hard-to-diagnose problems later, so reject it immediately
        /*try {
            env.validateTmpFile();
        } catch (IOException e) {
            throw new UserException(ExitCodes.CONFIG, e.getMessage());
        }

        try {
            init(daemonize, pidFile, quiet, env);
        } catch (NodeValidationException e) {
            throw new UserException(ExitCodes.CONFIG, e.getMessage());
        }*/
    }

    /**
     * Required method that's called by Apache Commons procrun when
     * running as a service on Windows, when the service is stopped.
     *
     * http://commons.apache.org/proper/commons-daemon/procrun.html
     *
     * NOTE: If this method is renamed and/or moved, make sure to
     * update elasticsearch-service.bat!
     */
    static void close(String[] args) throws IOException {
        //Bootstrap.stop();
    }
}
