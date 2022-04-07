/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows_service;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class WindowsServiceCli extends MultiCommand {

    private static final Command installCommand = new ProcrunCommand("Install Elasticsearch as a Windows Service", "install") {
        private final Path javaHome;
        private final Path javaDll;
        {
            javaHome = Paths.get(System.getProperty("java.home"));
            Path dll = javaHome.resolve("jre/bin/server/jvm.dll");
            if (Files.exists(dll) == false) {
                dll = javaHome.resolve("bin/server/jvm.dll");
            }
            javaDll = dll;
        }

        @Override
        protected String getAdditionalArgs(String serviceId, Path esHome, Map<String, String> env) {
            List<String> args = new ArrayList<>();
            addArg(args, "--Startup", env.getOrDefault("ES_START_TYPE", "manual"));
            addArg(args, "--StopTimeout", env.getOrDefault("ES_STOP_TIMEOUT", "0"));
            addArg(args, "--StartClass", "org.elasticsearch.cli.Launcher");
            addArg(args, "--StartMethod", "main");
            addArg(args, "++StartParams", "--quiet");
            addArg(args, "--StopClass", "org.elasticsearch.cli.Launcher");
            addArg(args, "--StopMethod", "close");
            addArg(args, "--Classpath", "\"%s\"".formatted(System.getProperty("java.class.path")));
            addArg(args, "--JvmSx", "4m");
            addArg(args, "--JvmMx", "64m");
            addArg(args, "--JvmOptions", "-XX:+UseSerialGC");
            addArg(args, "--PidFile", "\"%s.pid\"".formatted(serviceId));
            // TODO: get ES version
            addArg(args, "--DisplayName",
                env.getOrDefault("SERVICE_DISPLAY_NAME", "Elasticsearch ES_VERSION (%s)".formatted(serviceId)));
            addArg(args, "--Description",
                env.getOrDefault("SERVICE_DESCRIPTION", "Elasticsearch ES_VERSION Windows Service - https://elastic.co"));
            addArg(args, "--Jvm", javaDll.toString());
            addArg(args, "--StartMode", "jvm");
            addArg(args, "--StartPath", esHome.toString());
            addArg(args, "++Environment", "LAUNCHER_TOOLNAME=server-cli");
            addArg(args, "++Environment", "LAUNCHER_LIBS=lib/tools/server-cli");
            String serviceParams = env.get("SERVICE_PARAMS");
            if (serviceParams != null) {
                args.add(serviceParams);
            }
            return String.join(" ", args);
        }

        private static void addArg(List<String> args, String arg, String value) {
            args.add(arg);
            args.add(value);
        }


        @Override
        protected void preExecute(Terminal terminal, String serviceId) throws UserException {
            terminal.println("Installing service : %s".formatted(serviceId));
            terminal.println("Using ES_JAVA_HOME : %s".formatted(javaHome.toString()));

            if (Files.exists(javaDll) == false) {
                throw new UserException(ExitCodes.CONFIG,
                    "Invalid java installation (no jvm.dll found in %s\\jre\\bin\\server\\ or %s\\bin\\server\"). Exiting..."
                        .formatted(javaHome.toString(), javaHome.toString()));
            }
        }

        @Override
        protected String getSuccessMessage(String serviceId) {
            return "The service '%s' has been installed".formatted(serviceId);
        }

        @Override
        protected String getFailureMessage(String serviceId) {
            return "Failed installing '%s' service".formatted(serviceId);
        }
    };

    private static final Command removeCommand = new ProcrunCommand("Remove the Elasticsearch Windows Service", "delete") {
        @Override
        protected String getSuccessMessage(String serviceId) {
            return "The service '%s' has been removed".formatted(serviceId);
        }

        @Override
        protected String getFailureMessage(String serviceId) {
            return "Failed removing '%s' service".formatted(serviceId);
        }
    };

    private static final Command startCommand = new ProcrunCommand("Starts the Elasticsearch Windows Service", "start") {
        @Override
        protected String getSuccessMessage(String serviceId) {
            return "The service '%s' has been started".formatted(serviceId);
        }

        @Override
        protected String getFailureMessage(String serviceId) {
            return "Failed starting '%s' service".formatted(serviceId);
        }
    };

    private static final Command stopCommand = new ProcrunCommand("Stops the Elasticsearch Windows Service", "stop") {
        @Override
        protected String getSuccessMessage(String serviceId) {
            return "The service '%s' has been stopped".formatted(serviceId);
        }

        @Override
        protected String getFailureMessage(String serviceId) {
            return "Failed stopping '%s' service".formatted(serviceId);
        }
    };

    private static final Command managerCommand = new ProcrunCommand("Starts the Elasticsearch Windows Service manager", "manage") {
        @Override
        protected String getExecutable() {
            return "elasticsearch-service-mgr";
        }
        @Override
        protected String getSuccessMessage(String serviceId) {
            return "Successfully started service manager for '%s'".formatted(serviceId);
        }

        @Override
        protected String getFailureMessage(String serviceId) {
            return "Failed starting service manager for '%s'".formatted(serviceId);
        }
    };

    WindowsServiceCli() {
        super("A tool for managing Elasticsearch as a Windows service");
        subcommands.put("install", installCommand);
        subcommands.put("remove", removeCommand);
        subcommands.put("start", startCommand);
        subcommands.put("stop", stopCommand);
        subcommands.put("manager", managerCommand);
    }
}
