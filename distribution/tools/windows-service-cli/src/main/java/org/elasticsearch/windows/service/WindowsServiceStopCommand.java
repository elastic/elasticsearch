/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.windows.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;

import java.time.Duration;
import java.util.Locale;

/**
 * Stops the Elasticsearch Windows service.
 *
 * <p>After issuing the stop command, this polls the service status via {@code sc.exe query} until the
 * service reaches the {@code STOPPED} state or a timeout is reached. This is necessary because the
 * Windows Service Control Manager returns immediately with {@code STOP_PENDING}, and Elasticsearch
 * can take minutes to shut down gracefully.
 */
class WindowsServiceStopCommand extends ScCommand {
    private static final Logger logger = LogManager.getLogger(WindowsServiceStopCommand.class);

    static final Duration DEFAULT_STOP_TIMEOUT = Duration.ofMinutes(5);
    static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    WindowsServiceStopCommand() {
        super("Stops the Elasticsearch Windows Service", "stop");
    }

    @Override
    protected void postExecute(Terminal terminal, ProcessInfo pinfo, String serviceId) throws Exception {
        Duration timeout = getStopTimeout(pinfo);
        waitForStopped(terminal, serviceId, timeout);
    }

    /**
     * Polls {@code sc.exe query} until the service reaches STOPPED state or the timeout expires.
     */
    void waitForStopped(Terminal terminal, String serviceId, Duration timeout) throws UserException {
        terminal.println(String.format(Locale.ROOT, "Waiting for service '%s' to stop...", serviceId));
        long start = System.currentTimeMillis();
        while (true) {
            String state = queryServiceState(serviceId);
            if ("STOPPED".equalsIgnoreCase(state)) {
                return;
            }

            Duration elapsed = Duration.ofMillis(System.currentTimeMillis() - start);
            if (elapsed.compareTo(timeout) > 0) {
                String msg = String.format(
                    Locale.ROOT,
                    "Timed out after %s waiting for service '%s' to stop (last state: %s)",
                    elapsed,
                    serviceId,
                    state
                );
                logger.warn(msg);
                terminal.errorPrintln(msg);
                throw new UserException(1, msg);
            }

            logger.debug("Service [{}] still in state [{}], waiting...", serviceId, state);
            terminal.println(Terminal.Verbosity.VERBOSE, String.format(Locale.ROOT, "Service '%s' still %s...", serviceId, state));
            try {
                Thread.sleep(POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UserException(1, "Interrupted while waiting for service '" + serviceId + "' to stop");
            }
        }
    }

    /**
     * Queries the current state of the service via {@code sc.exe query}.
     * Returns the state string (e.g. "RUNNING", "STOPPED", "STOP_PENDING") or "UNKNOWN" on failure.
     */
    String queryServiceState(String serviceId) {
        try {
            ProcessBuilder pb = new ProcessBuilder("sc.exe", "query", serviceId);
            Process process = startProcess(pb);
            String output = ScCommand.readStdout(process);
            process.waitFor();
            return parseState(output);
        } catch (Exception e) {
            logger.warn("Failed to query service state for [{}]", serviceId, e);
            return "UNKNOWN";
        }
    }

    /**
     * Parses the STATE field from {@code sc.exe query} output.
     * The output contains a line like: {@code STATE : 4  RUNNING} or {@code STATE : 1  STOPPED}.
     */
    static String parseState(String output) {
        for (String line : output.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.startsWith("STATE")) {
                int lastSpace = trimmed.lastIndexOf(' ');
                if (lastSpace > 0) {
                    return trimmed.substring(lastSpace + 1).trim();
                }
            }
        }
        return "UNKNOWN";
    }

    private static Duration getStopTimeout(ProcessInfo pinfo) {
        String timeoutStr = pinfo.envVars().get("ES_STOP_TIMEOUT");
        if (timeoutStr != null && timeoutStr.isBlank() == false) {
            try {
                return Duration.ofSeconds(Long.parseLong(timeoutStr));
            } catch (NumberFormatException e) {
                logger.warn("Invalid ES_STOP_TIMEOUT value [{}], using default", timeoutStr);
            }
        }
        return DEFAULT_STOP_TIMEOUT;
    }

    @Override
    protected String getSuccessMessage(String serviceId) {
        return String.format(Locale.ROOT, "The service '%s' has been stopped", serviceId);
    }

    @Override
    protected String getFailureMessage(String serviceId) {
        return String.format(Locale.ROOT, "Failed stopping '%s' service", serviceId);
    }
}
