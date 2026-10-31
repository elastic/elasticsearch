/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.windows.service;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cli.CommandTestCase;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Base test case for commands that use {@code sc.exe} for service control.
 */
public abstract class ScCommandTestCase extends CommandTestCase {

    int mockProcessExit = 0;
    String mockProcessOutput = "";
    ScCallValidator mockScCallValidator = null;

    @ParametersFactory
    public static Iterable<Object[]> spaceInPathProvider() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    protected ScCommandTestCase(boolean spaceInPath) {
        super(spaceInPath);
    }

    interface ScCallValidator {
        void validate(List<String> command);
    }

    record ScCall(String verb, String serviceId) {}

    class MockProcess extends Process {
        private final InputStream stdout;

        MockProcess(String output) {
            this.stdout = new ByteArrayInputStream(output.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public OutputStream getOutputStream() {
            throw new AssertionError("should not access output stream");
        }

        @Override
        public InputStream getInputStream() {
            return stdout;
        }

        @Override
        public InputStream getErrorStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public int waitFor() {
            return mockProcessExit;
        }

        @Override
        public int exitValue() {
            return mockProcessExit;
        }

        @Override
        public void destroy() {
            throw new AssertionError("should not kill sc.exe process");
        }
    }

    protected Process mockProcess(ProcessBuilder processBuilder) throws IOException {
        if (mockScCallValidator != null) {
            mockScCallValidator.validate(processBuilder.command());
        }
        return new MockProcess(mockProcessOutput);
    }

    static ScCall parseScCall(List<String> command) {
        assertThat(command, hasSize(3));
        assertThat(command.get(0), equalTo("sc.exe"));
        return new ScCall(command.get(1), command.get(2));
    }

    @Before
    public void resetMockProcess() {
        mockProcessExit = 0;
        mockProcessOutput = "";
        mockScCallValidator = null;
    }

    protected abstract String getScVerb();

    protected abstract String getDefaultSuccessMessage();

    protected abstract String getDefaultFailureMessage();

    public void testDefaultCommand() throws Exception {
        mockScCallValidator = (command) -> {
            ScCall scCall = parseScCall(command);
            assertThat(scCall.verb(), equalTo(getScVerb()));
            assertThat(scCall.serviceId(), equalTo("elasticsearch-service-x64"));
        };
        assertOkWithOutput(containsString(getDefaultSuccessMessage()), emptyString());
    }

    public void testFailure() throws Exception {
        mockProcessExit = 5;
        assertThat(executeMain(), equalTo(5));
        assertThat(terminal.getErrorOutput(), containsString(getDefaultFailureMessage()));
    }

    public void testServiceId() throws Exception {
        assertUsage(containsString("too many arguments"), "servicename", "servicename");
        terminal.reset();
        mockScCallValidator = (command) -> {
            ScCall scCall = parseScCall(command);
            assertThat(scCall.serviceId(), equalTo("my-service-id"));
        };
        assertOkWithOutput(
            containsString(getDefaultSuccessMessage().replace("elasticsearch-service-x64", "my-service-id")),
            emptyString(),
            "my-service-id"
        );
    }

    public void testServiceIdFromEnv() throws Exception {
        envVars.put("SERVICE_ID", "my-service-id");
        mockScCallValidator = (command) -> {
            ScCall scCall = parseScCall(command);
            assertThat(scCall.serviceId(), equalTo("my-service-id"));
        };
        assertOkWithOutput(containsString(getDefaultSuccessMessage().replace("elasticsearch-service-x64", "my-service-id")), emptyString());
    }
}
