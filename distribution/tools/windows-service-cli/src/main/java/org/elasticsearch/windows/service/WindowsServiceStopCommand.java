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
import org.elasticsearch.service.windows.ServiceStatus;
import org.elasticsearch.service.windows.WindowsServiceControl;
import org.elasticsearch.service.windows.WindowsServiceException;

import java.time.Duration;
import java.util.Locale;
import java.util.function.Predicate;

/**
 * Stops the Elasticsearch Windows service.
 *
 * <p>After issuing the stop command via the SCM, this polls the service status until it reaches
 * the {@code STOPPED} state or a timeout is reached. This is necessary because the SCM returns
 * immediately with {@code STOP_PENDING}, and Elasticsearch can take minutes to shut down gracefully.
 *
 * <p>The polling loop respects the service's {@code dwWaitHint} and {@code dwCheckPoint} fields
 * from {@code SERVICE_STATUS_PROCESS} for SCM-compliant waiting behavior.
 */
class WindowsServiceStopCommand extends ScmCommand {
    private static final Logger logger = LogManager.getLogger(WindowsServiceStopCommand.class);

    static final Duration DEFAULT_STOP_TIMEOUT = Duration.ofMinutes(5);
    static final Duration MIN_POLL_INTERVAL = Duration.ofSeconds(1);
    static final Duration MAX_POLL_INTERVAL = Duration.ofSeconds(10);
    static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(2);

    WindowsServiceStopCommand() {
        this(WindowsServiceControl.create());
    }

    WindowsServiceStopCommand(WindowsServiceControl serviceControl) {
        super("Stops the Elasticsearch Windows Service", serviceControl);
    }

    @Override
    protected void executeServiceCommand(WindowsServiceControl serviceControl, String serviceId) throws WindowsServiceException {
        serviceControl.stopService(serviceId);
    }

    @Override
    protected void postExecute(Terminal terminal, ProcessInfo pinfo, String serviceId) throws Exception {
        waitForStopped(terminal, serviceId, getStopTimeout(pinfo));
    }

    /**
     * Polls the service status until it reaches STOPPED or the timeout expires.
     * Uses {@code dwWaitHint} from the service to determine poll intervals.
     */
    void waitForStopped(Terminal terminal, String serviceId, Predicate<Duration> isTimedOut) throws UserException {
        terminal.println(String.format(Locale.ROOT, "Waiting for service '%s' to stop...", serviceId));
        long start = System.currentTimeMillis();

        while (true) {
            ServiceStatus status = queryStatus(serviceId);
            if (status.state() == ServiceStatus.STOPPED) {
                return;
            }

            Duration elapsed = Duration.ofMillis(System.currentTimeMillis() - start);
            if (isTimedOut.test(elapsed)) {
                String msg = String.format(
                    Locale.ROOT,
                    "Timed out after %s waiting for service '%s' to stop (last state: %s)",
                    elapsed,
                    serviceId,
                    status.stateName()
                );
                logger.warn(msg);
                terminal.errorPrintln(msg);
                throw new UserException(1, msg);
            }

            if (status.state() == ServiceStatus.UNKNOWN) {
                terminal.errorPrintln(
                    String.format(Locale.ROOT, "WARNING: Unable to query status of service '%s'; will keep waiting...", serviceId)
                );
            }

            Duration pollInterval = computePollInterval(status);
            logger.debug("Service [{}] in state [{}], waiting [{}]", serviceId, status.stateName(), pollInterval);
            terminal.println(
                Terminal.Verbosity.VERBOSE,
                String.format(Locale.ROOT, "Service '%s' still %s...", serviceId, status.stateName())
            );

            try {
                // It is OK call sleep here, we need to wait for the service to stop
                // noinspection BusyWait
                Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UserException(1, "Interrupted while waiting for service '" + serviceId + "' to stop");
            }
        }
    }

    /**
     * Computes the poll interval based on the service's {@code dwWaitHint}.
     * Microsoft recommends using one-tenth of the wait hint, but no less than 1 second
     * and no more than 10 seconds.
     *
     * @see <a href="https://learn.microsoft.com/en-us/windows/win32/services/stopping-a-service">
     *      Stopping a Service (Microsoft Learn)</a>
     */
    static Duration computePollInterval(ServiceStatus status) {
        int waitHint = status.waitHint();
        if (waitHint > 0) {
            long interval = waitHint / 10;
            if (interval < MIN_POLL_INTERVAL.toMillis()) {
                return MIN_POLL_INTERVAL;
            } else if (interval > MAX_POLL_INTERVAL.toMillis()) {
                return MAX_POLL_INTERVAL;
            }
            return Duration.ofMillis(interval);
        }
        return DEFAULT_POLL_INTERVAL;
    }

    ServiceStatus queryStatus(String serviceId) {
        try {
            return getServiceControl().queryStatus(serviceId);
        } catch (WindowsServiceException e) {
            logger.warn("Failed to query service status for [{}]", serviceId, e);
            return ServiceStatus.unknown();
        }
    }

    private static Predicate<Duration> getStopTimeout(ProcessInfo pinfo) {
        String timeoutStr = pinfo.envVars().get("ES_STOP_TIMEOUT");
        if (timeoutStr != null && timeoutStr.isBlank() == false) {
            try {
                long timeoutInSeconds = Long.parseLong(timeoutStr);
                if (timeoutInSeconds > 0) {
                    var timeout = Duration.ofSeconds(timeoutInSeconds);
                    return elapsed -> elapsed.compareTo(timeout) > 0;
                } else {
                    // 0 means no timeout
                    return elapsed -> false;
                }
            } catch (NumberFormatException e) {
                logger.warn("Invalid ES_STOP_TIMEOUT value [{}], using default", timeoutStr);
            }
        }
        return elapsed -> elapsed.compareTo(DEFAULT_STOP_TIMEOUT) > 0;
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
