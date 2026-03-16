/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.windows.service;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Base command for controlling Windows services via {@code sc.exe}.
 *
 * <p>Unlike {@link ProcrunCommand}, which delegates to the Apache procrun executable for all service
 * operations, this class uses the standard Windows {@code sc.exe} tool for service control operations
 * (start, stop, delete). This avoids procrun's unreliable behavior around service state transitions,
 * particularly during stop where procrun may return before the service has fully stopped and may
 * incorrectly report errors.
 *
 * @see <a href="https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/sc-config">sc.exe docs</a>
 */
abstract class ScCommand extends Command {

    private final String scCommand;

    /**
     * Constructs a CLI subcommand that will internally call {@code sc.exe}.
     * @param desc A help description for this subcommand
     * @param scCommand The sc.exe verb to run (e.g. "start", "stop", "delete")
     */
    protected ScCommand(String desc, String scCommand) {
        super(desc);
        this.scCommand = scCommand;
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        String serviceId = getServiceId(options, processInfo.envVars());
        preExecute(terminal, processInfo, serviceId);

        List<String> command = List.of("sc.exe", scCommand, serviceId);
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = startProcess(processBuilder);
        String output = readStdout(process);
        int ret = process.waitFor();

        if (ret != ExitCodes.OK) {
            throw new UserException(ret, getFailureMessage(serviceId) + (output.isEmpty() ? "" : ": " + output));
        }

        postExecute(terminal, processInfo, serviceId);
        terminal.println(getSuccessMessage(serviceId));
    }

    /** Reads all stdout from the process into a trimmed string. */
    static String readStdout(Process process) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (sb.isEmpty() == false) {
                    sb.append(System.lineSeparator());
                }
                sb.append(line);
            }
        }
        return sb.toString().trim();
    }

    /** Determines the service id for the Elasticsearch service that should be used. */
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

    /**
     * A hook to add logging and validation before executing the sc.exe command.
     * @throws UserException if there is a problem with the command invocation
     */
    protected void preExecute(Terminal terminal, ProcessInfo pinfo, String serviceId) throws UserException {}

    /**
     * A hook for additional work after the sc.exe command succeeds (e.g. polling for stop completion).
     * @throws UserException if there is a problem after the command
     */
    protected void postExecute(Terminal terminal, ProcessInfo pinfo, String serviceId) throws Exception {}

    /** Returns a message that should be output on success of the sc.exe command. */
    protected abstract String getSuccessMessage(String serviceId);

    /** Returns a message that should be output on failure of the sc.exe command. */
    protected abstract String getFailureMessage(String serviceId);

    // package private to allow tests to override
    Process startProcess(ProcessBuilder processBuilder) throws IOException {
        return processBuilder.start();
    }
}
