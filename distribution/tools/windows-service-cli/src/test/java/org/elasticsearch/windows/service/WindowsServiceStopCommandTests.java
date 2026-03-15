/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.windows.service;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.service.windows.ServiceStatus;
import org.elasticsearch.service.windows.WindowsServiceException;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class WindowsServiceStopCommandTests extends ScmCommandTestCase {

    public WindowsServiceStopCommandTests(boolean spaceInPath) {
        super(spaceInPath);
    }

    @Override
    protected Command newCommand() {
        return newStopCommand(null);
    }

    private WindowsServiceStopCommand newStopCommand(ServiceStatus[] queryResults) {
        return new WindowsServiceStopCommand(mockServiceControl) {
            private final AtomicInteger queryCount = new AtomicInteger(0);

            @Override
            ServiceStatus queryStatus(String serviceId) {
                if (queryResults == null) {
                    return new ServiceStatus(ServiceStatus.STOPPED, 0, 0, 0);
                }
                int idx = Math.min(queryCount.getAndIncrement(), queryResults.length - 1);
                return queryResults[idx];
            }
        };
    }

    @Override
    protected String getExpectedOperation() {
        return "stop";
    }

    @Override
    protected String getDefaultSuccessMessage() {
        return "The service 'elasticsearch-service-x64' has been stopped";
    }

    @Override
    protected String getDefaultFailureMessage() {
        return "Failed stopping 'elasticsearch-service-x64' service";
    }

    public void testFailure() throws Exception {
        mockServiceControl.stopException = new WindowsServiceException("Access denied", 5);
        int exitCode = executeMain();
        assertThat(exitCode, equalTo(ExitCodes.CODE_ERROR));
        assertThat(terminal.getErrorOutput(), containsString(getDefaultFailureMessage()));
        assertThat(terminal.getErrorOutput(), containsString("Access denied"));
    }

    public void testStopWaitsForStopped() throws Exception {
        ServiceStatus[] statuses = new ServiceStatus[] {
            new ServiceStatus(ServiceStatus.STOP_PENDING, 0, 1, 1000),
            new ServiceStatus(ServiceStatus.STOP_PENDING, 0, 2, 1000),
            new ServiceStatus(ServiceStatus.STOPPED, 0, 0, 0) };
        Command cmd = newStopCommand(statuses);
        String output = execute(cmd);
        assertThat(output, containsString("The service 'elasticsearch-service-x64' has been stopped"));
    }

    public void testStopImmediatelyStopped() throws Exception {
        ServiceStatus[] statuses = new ServiceStatus[] { new ServiceStatus(ServiceStatus.STOPPED, 0, 0, 0) };
        Command cmd = newStopCommand(statuses);
        String output = execute(cmd);
        assertThat(output, containsString("The service 'elasticsearch-service-x64' has been stopped"));
    }

    public void testStopTimeout() throws Exception {
        envVars.put("ES_STOP_TIMEOUT", "1");
        WindowsServiceStopCommand cmd = new WindowsServiceStopCommand(mockServiceControl) {
            @Override
            ServiceStatus queryStatus(String serviceId) {
                return new ServiceStatus(ServiceStatus.STOP_PENDING, 0, 1, 1000);
            }
        };
        int exitCode = cmd.main(new String[0], terminal, new ProcessInfo(sysprops, envVars, esHomeDir));
        assertThat(exitCode, equalTo(1));
        assertThat(terminal.getErrorOutput(), containsString("Timed out"));
    }

    public void testComputePollIntervalFromWaitHint() {
        ServiceStatus status = new ServiceStatus(ServiceStatus.STOP_PENDING, 0, 1, 30000);
        Duration interval = WindowsServiceStopCommand.computePollInterval(status);
        assertThat(interval, equalTo(Duration.ofSeconds(3)));
    }

    public void testComputePollIntervalMinimum() {
        ServiceStatus status = new ServiceStatus(ServiceStatus.STOP_PENDING, 0, 1, 100);
        Duration interval = WindowsServiceStopCommand.computePollInterval(status);
        assertThat(interval, equalTo(WindowsServiceStopCommand.MIN_POLL_INTERVAL));
    }

    public void testComputePollIntervalMaximum() {
        ServiceStatus status = new ServiceStatus(ServiceStatus.STOP_PENDING, 0, 1, 500000);
        Duration interval = WindowsServiceStopCommand.computePollInterval(status);
        assertThat(interval, equalTo(WindowsServiceStopCommand.MAX_POLL_INTERVAL));
    }

    public void testComputePollIntervalNoHint() {
        ServiceStatus status = new ServiceStatus(ServiceStatus.STOP_PENDING, 0, 1, 0);
        Duration interval = WindowsServiceStopCommand.computePollInterval(status);
        assertThat(interval, equalTo(Duration.ofSeconds(2)));
    }

    public void testQueryStatusFailureReturnsUnknown() {
        mockServiceControl.queryException = new WindowsServiceException("Query failed", 1);
        WindowsServiceStopCommand cmd = new WindowsServiceStopCommand(mockServiceControl);
        ServiceStatus result = cmd.queryStatus("test-service");
        assertThat(result.state(), equalTo(ServiceStatus.UNKNOWN));
    }

    public void testUnknownStatusKeepsPolling() throws Exception {
        ServiceStatus[] statuses = new ServiceStatus[] {
            new ServiceStatus(ServiceStatus.STOP_PENDING, 0, 1, 1000),
            ServiceStatus.unknown(),
            new ServiceStatus(ServiceStatus.STOPPED, 0, 0, 0) };
        Command cmd = newStopCommand(statuses);
        String output = execute(cmd);
        assertThat(output, containsString("The service 'elasticsearch-service-x64' has been stopped"));
    }
}
