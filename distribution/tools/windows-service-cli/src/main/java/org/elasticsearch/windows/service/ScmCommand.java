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
import org.elasticsearch.service.windows.WindowsServiceControl;
import org.elasticsearch.service.windows.WindowsServiceException;

import java.util.List;
import java.util.Map;

/**
 * Base command for controlling Windows services via the Service Control Manager.
 *
 * <p>Uses {@link WindowsServiceControl} to interact with the SCM, which can be backed by
 * native Panama FFI calls (production) or a mock implementation (tests).
 */
abstract class ScmCommand extends Command {

    private final WindowsServiceControl serviceControl;

    /**
     * Constructs a CLI subcommand that uses the SCM for service control.
     * @param desc A help description for this subcommand
     * @param serviceControl the service control implementation to use
     */
    protected ScmCommand(String desc, WindowsServiceControl serviceControl) {
        super(desc);
        this.serviceControl = serviceControl;
    }

    protected WindowsServiceControl getServiceControl() {
        return serviceControl;
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        String serviceId = getServiceId(options, processInfo.envVars());
        preExecute(terminal, processInfo, serviceId);

        try {
            executeServiceCommand(serviceControl, serviceId);
        } catch (WindowsServiceException e) {
            throw new UserException(ExitCodes.CODE_ERROR, getFailureMessage(serviceId) + ": " + e.getMessage());
        }

        postExecute(terminal, processInfo, serviceId);
        terminal.println(getSuccessMessage(serviceId));
    }

    /**
     * Performs the actual service control operation.
     * @param serviceControl the service control API
     * @param serviceId the service name
     * @throws WindowsServiceException if the operation fails
     */
    protected abstract void executeServiceCommand(WindowsServiceControl serviceControl, String serviceId) throws WindowsServiceException;

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
     * A hook to add logging and validation before executing the service command.
     * @throws UserException if there is a problem with the command invocation
     */
    protected void preExecute(Terminal terminal, ProcessInfo pinfo, String serviceId) throws UserException {}

    /**
     * A hook for additional work after the service command succeeds (e.g. polling for stop completion).
     * @throws Exception if there is a problem after the command
     */
    protected void postExecute(Terminal terminal, ProcessInfo pinfo, String serviceId) throws Exception {}

    /** Returns a message that should be output on success. */
    protected abstract String getSuccessMessage(String serviceId);

    /** Returns a message that should be output on failure. */
    protected abstract String getFailureMessage(String serviceId);
}
