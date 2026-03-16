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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class WindowsServiceStopCommandTests extends ScCommandTestCase {

    public WindowsServiceStopCommandTests(boolean spaceInPath) {
        super(spaceInPath);
    }

    @Override
    protected Command newCommand() {
        return newStopCommand(null);
    }

    private WindowsServiceStopCommand newStopCommand(String[] queryStates) {
        return new WindowsServiceStopCommand() {
            private final AtomicInteger queryCount = new AtomicInteger(0);

            @Override
            Process startProcess(ProcessBuilder processBuilder) throws IOException {
                return mockProcess(processBuilder);
            }

            @Override
            String queryServiceState(String serviceId) {
                if (queryStates == null) {
                    return "STOPPED";
                }
                int idx = Math.min(queryCount.getAndIncrement(), queryStates.length - 1);
                return queryStates[idx];
            }
        };
    }

    @Override
    protected String getScVerb() {
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

    public void testStopWaitsForStopped() throws Exception {
        Command cmd = newStopCommand(new String[] { "STOP_PENDING", "STOP_PENDING", "STOPPED" });
        String output = execute(cmd);
        assertThat(output, containsString("The service 'elasticsearch-service-x64' has been stopped"));
    }

    public void testStopImmediatelyStopped() throws Exception {
        Command cmd = newStopCommand(new String[] { "STOPPED" });
        String output = execute(cmd);
        assertThat(output, containsString("The service 'elasticsearch-service-x64' has been stopped"));
    }

    public void testStopTimeout() throws Exception {
        envVars.put("ES_STOP_TIMEOUT", "1");
        WindowsServiceStopCommand cmd = new WindowsServiceStopCommand() {
            @Override
            Process startProcess(ProcessBuilder processBuilder) throws IOException {
                return mockProcess(processBuilder);
            }

            @Override
            String queryServiceState(String serviceId) {
                return "STOP_PENDING";
            }
        };
        int exitCode = cmd.main(new String[0], terminal, new org.elasticsearch.cli.ProcessInfo(sysprops, envVars, esHomeDir));
        assertThat(exitCode, equalTo(1));
        assertThat(terminal.getErrorOutput(), containsString("Timed out"));
    }

    public void testParseStateStopped() {
        String output = """
            SERVICE_NAME: elasticsearch-service-x64
                    TYPE               : 10  WIN32_OWN_PROCESS
                    STATE              : 1  STOPPED
                    WIN32_EXIT_CODE    : 0  (0x0)
                    SERVICE_EXIT_CODE  : 0  (0x0)
                    CHECKPOINT         : 0x0
                    WAIT_HINT          : 0x0
            """;
        assertThat(WindowsServiceStopCommand.parseState(output), equalTo("STOPPED"));
    }

    public void testParseStateRunning() {
        String output = """
            SERVICE_NAME: elasticsearch-service-x64
                    TYPE               : 10  WIN32_OWN_PROCESS
                    STATE              : 4  RUNNING
                    WIN32_EXIT_CODE    : 0  (0x0)
                    SERVICE_EXIT_CODE  : 0  (0x0)
                    CHECKPOINT         : 0x0
                    WAIT_HINT          : 0x0
            """;
        assertThat(WindowsServiceStopCommand.parseState(output), equalTo("RUNNING"));
    }

    public void testParseStateStopPending() {
        String output = """
            SERVICE_NAME: elasticsearch-service-x64
                    TYPE               : 10  WIN32_OWN_PROCESS
                    STATE              : 3  STOP_PENDING
                    WIN32_EXIT_CODE    : 0  (0x0)
                    SERVICE_EXIT_CODE  : 0  (0x0)
                    CHECKPOINT         : 0x0
                    WAIT_HINT          : 0x0
            """;
        assertThat(WindowsServiceStopCommand.parseState(output), equalTo("STOP_PENDING"));
    }

    public void testParseStateUnknownOutput() {
        assertThat(WindowsServiceStopCommand.parseState("garbage output"), equalTo("UNKNOWN"));
    }
}
