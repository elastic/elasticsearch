/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows_service;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Base command for interacting with Apache procrun executable.
 */
abstract class ProcrunCommand extends Command {
    private final Path procrun;
    private final String cmd;

    protected ProcrunCommand(String desc, String cmd) {
        super(desc);
        this.procrun = Paths.get("").resolve("bin").resolve(getExecutable()).toAbsolutePath();
        if (Files.exists(procrun) == false) {
            throw new IllegalStateException("Missing procrun exe: " + procrun);
        }
        this.cmd = cmd;
    }

    protected String getExecutable() {
        return "elasticsearch-service-x64.exe";
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        Map<String, String> env = System.getenv();
        Path esHome = Paths.get("").toAbsolutePath(); // TODO: this should be passed through execute
        String serviceId = getServiceId(options, env);
        preExecute(terminal, serviceId);

        List<String> procrunCmd = new ArrayList<>();
        procrunCmd.add(procrun.toString());
        procrunCmd.add(cmd);
        procrunCmd.add(serviceId);
        procrunCmd.add(getLogArgs(serviceId, esHome, env));
        procrunCmd.add(getAdditionalArgs(serviceId, esHome, env));

        ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/C", String.join(" ", procrunCmd).trim());
        processBuilder.inheritIO();
        Process process = processBuilder.start();
        int ret = process.waitFor();
        if (ret != ExitCodes.OK) {
            throw new UserException(ret, getFailureMessage(serviceId));
        } else {
            terminal.println(getSuccessMessage(serviceId));
        }
    }

    private String getServiceId(OptionSet options, Map<String, String> env) throws UserException {
        List<?> args = options.nonOptionArguments();
        if (args.size() > 1) {
            throw new UserException(ExitCodes.USAGE, null);
        }
        final String serviceId;
        if (args.size() > 0) {
            serviceId = args.get(0).toString();
        } else {
            serviceId = env.getOrDefault("SERVICE_ID", "elasticsearch-service-x64");
        }
        return serviceId;
    }

    private String getLogArgs(String serviceId, Path esHome, Map<String, String> env) {
        String logArgs = env.get("LOG_OPTS");
        if (logArgs != null && logArgs.isBlank() == false) {
            return logArgs;
        }
        String logsDir = env.get("SERVICE_LOG_DIR");
        if (logsDir == null || logsDir.isBlank()) {
            logsDir = esHome.resolve("logs").toString();
        }
        String logArgsFormat = "--LogPath \"%s\" --LogPrefix \"%s\" --StdError auto --StdOutput auto";
        return String.format(Locale.ROOT, logArgsFormat, logsDir, serviceId);
    }

    protected String getAdditionalArgs(String serviceId, Path esHome, Map<String, String> env) {
        return "";
    }

    protected void preExecute(Terminal terminal, String serviceId) throws UserException {}

    protected abstract String getSuccessMessage(String serviceId);

    protected abstract String getFailureMessage(String serviceId);
}
