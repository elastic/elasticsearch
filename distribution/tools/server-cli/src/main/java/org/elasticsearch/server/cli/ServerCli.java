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
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.ToolProvider;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
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
        final SecureString keystorePassword;
        try {
            KeyStoreWrapper keystore = KeyStoreWrapper.load(KeyStoreWrapper.keystorePath(env.configFile()));
            if (keystore != null && keystore.hasPassword()) {
                keystorePassword = new SecureString(terminal.readSecret(KeyStoreWrapper.PROMPT, KeyStoreWrapper.MAX_PASSPHRASE_LENGTH));
            } else {
                keystorePassword = new SecureString(new char[0]);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        KeystorePasswordTerminal autoConfigTerminal = new KeystorePasswordTerminal(terminal, keystorePassword);
        Command autoConfigNode;
        try {
            String autoConfigLibs = "modules/x-pack-core,modules/x-pack-security,lib/tools/security-cli";
            autoConfigNode = ToolProvider.loadTool("auto-configure-node", autoConfigLibs).create();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (options.has(enrollmentTokenOption)) {
            final String enrollmentToken = enrollmentTokenOption.value(options);
            // TODO: add -E params
            int ret = autoConfigNode.main(new String[] { enrollmentTokenOption.options().get(0), enrollmentToken}, autoConfigTerminal);
            if (ret != 0) {
                throw new UserException(ret, "Auto security enrollment failed");
            }
        } else {
            // TODO: add -E params
            int ret = autoConfigNode.main(new String[0], autoConfigTerminal);
            switch (ret) {
                case ExitCodes.OK, ExitCodes.CANT_CREATE, ExitCodes.CONFIG, ExitCodes.NOOP: break;
                default: throw new UserException(ret, "Auto security enrollment failed");
            }
        }


        // reload settings now that auto configuration has occurred
        env = createEnv(options);
        ServerArgs serverArgs = new ServerArgs(daemonize, pidFile, env.settings(), env.configFile());

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
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        try {
            final Process process = builder.start();
            try (var out = new OutputStreamStreamOutput(process.getOutputStream())) {
                serverArgs.writeTo(out);
            }
            String userExceptionMsg = null;
            boolean ready = false;
            InputStream err = process.getErrorStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(err));
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty() == false && line.charAt(0) == '\24') {
                        userExceptionMsg = line.substring(1);
                    } else if (line.isEmpty() == false && line.charAt(0) == '\21') {
                        ready = true;
                        break;
                    } else {
                        terminal.getErrorWriter().println(line);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            if (ready && daemonize) {
                process.getOutputStream().close();
                process.getErrorStream().close();
                process.getInputStream().close();
                return;
            }

            int code = process.waitFor();
            terminal.flush();
            System.out.println("exited: " + code);
            if (code != ExitCodes.OK) {
                throw new UserException(code, userExceptionMsg);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new UserException(ExitCodes.IO_ERROR, "Interrupted while waiting for Elasticsearch process");
        }


        // TODO: add ctrl-c handler so we can wait for subprocess
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
}
