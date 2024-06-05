/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

import joptsimple.OptionSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Base command for interacting with Apache procrun executable.
 *
 * @see <a href="https://commons.apache.org/proper/commons-daemon/procrun.html">Apache Procrun Docs</a>
 */
abstract class ProcrunCommand extends Command {
    private static final Logger logger = LogManager.getLogger(ProcrunCommand.class);

    private final String cmd;

    /**
     * Constructs CLI subcommand that will internally call procrun.
     * @param desc A help description for this subcommand
     * @param cmd The procrun command to run
     */
    protected ProcrunCommand(String desc, String cmd) {
        super(desc);
        this.cmd = cmd;
    }

    /**
     * Returns the name of the exe within the Elasticsearch bin dir to run.
     *
     * <p> Procrun comes with two executables, {@code prunsrv.exe} and {@code prunmgr.exe}. These are renamed by
     * Elasticsearch to {@code elasticsearch-service-x64.exe} and {@code elasticsearch-service-mgr.exe}, respectively.
     */
    protected String getExecutable() {
        return "elasticsearch-service-x64.exe";
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        Path procrun = processInfo.workingDir().resolve("bin").resolve(getExecutable()).toAbsolutePath();
        if (Files.exists(procrun) == false) {
            throw new IllegalStateException("Missing procrun exe: " + procrun);
        }
        String serviceId = getServiceId(options, processInfo.envVars());
        preExecute(terminal, processInfo, serviceId);

        List<String> procrunCmd = new ArrayList<>();
        procrunCmd.add(quote(procrun.toString()));
        procrunCmd.add("//%s/%s".formatted(cmd, serviceId));
        if (includeLogArgs()) {
            procrunCmd.add(getLogArgs(serviceId, processInfo.workingDir(), processInfo.envVars()));
        }
        procrunCmd.add(getAdditionalArgs(serviceId, processInfo));

        ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/C", String.join(" ", procrunCmd).trim());
        logger.debug((Supplier<?>) () -> "Running procrun: " + String.join(" ", processBuilder.command()));
        processBuilder.inheritIO();
        Process process = startProcess(processBuilder);
        int ret = process.waitFor();
        if (ret != ExitCodes.OK) {
            throw new UserException(ret, getFailureMessage(serviceId));
        } else {
            terminal.println(getSuccessMessage(serviceId));
        }
    }

    /** Quotes the given String. */
    static String quote(String s) {
        return '"' + s + '"';
    }

    /** Determines the service id for the Elasticsearch service that should be used */
    private static String getServiceId(OptionSet options, Map<String, String> env) throws UserException {
        List<?> args = options.nonOptionArguments();
        if (args.size() > 1) {
            throw new UserException(ExitCodes.USAGE, "too many arguments, expected one service id");
        }
        final String serviceId;
        if (args.size() > 0) {
            serviceId = args.get(0).toString();
        } else {
            serviceId = env.getOrDefault("SERVICE_ID", "elasticsearch-service-x64");
        }
        return serviceId;
    }

    /** Determines the logging arguments that should be passed to the procrun command */
    private static String getLogArgs(String serviceId, Path esHome, Map<String, String> env) {
        String logArgs = env.get("LOG_OPTS");
        if (logArgs != null && logArgs.isBlank() == false) {
            return logArgs;
        }
        String logsDir = env.get("SERVICE_LOG_DIR");
        if (logsDir == null || logsDir.isBlank()) {
            logsDir = esHome.resolve("logs").toString();
        }
        String logArgsFormat = "--LogPath \"%s\" --LogPrefix \"%s\" --StdError auto --StdOutput auto --LogLevel Debug";
        return String.format(Locale.ROOT, logArgsFormat, logsDir, serviceId);
    }

    /**
     * Gets arguments that should be passed to the procrun command.
     *
     * @param serviceId The service id of the Elasticsearch service
     * @param processInfo The current process info
     * @return The additional arguments, space delimited
     */
    protected String getAdditionalArgs(String serviceId, ProcessInfo processInfo) {
        return "";
    }

    /** Return whether logging args should be added to the procrun command */
    protected boolean includeLogArgs() {
        return true;
    }

    /**
     * A hook to add logging and validation before executing the procrun command.
     * @throws UserException if there is a problem with the command invocation
     */
    protected void preExecute(Terminal terminal, ProcessInfo pinfo, String serviceId) throws UserException {}

    /** Returns a message that should be output on success of the procrun command */
    protected abstract String getSuccessMessage(String serviceId);

    /** Returns a message that should be output on failure of the procrun command */
    protected abstract String getFailureMessage(String serviceId);

    // package private to allow tests to override
    Process startProcess(ProcessBuilder processBuilder) throws IOException {
        return processBuilder.start();
    }
}
