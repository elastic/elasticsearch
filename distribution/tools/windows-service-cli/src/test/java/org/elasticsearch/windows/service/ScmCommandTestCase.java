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
import org.elasticsearch.service.windows.ServiceStatus;
import org.elasticsearch.service.windows.WindowsServiceControl;
import org.elasticsearch.service.windows.WindowsServiceException;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Base test case for commands that use {@link WindowsServiceControl} for service control.
 * Provides a {@link MockWindowsServiceControl} that tests can configure.
 */
public abstract class ScmCommandTestCase extends CommandTestCase {

    MockWindowsServiceControl mockServiceControl;

    @ParametersFactory
    public static Iterable<Object[]> spaceInPathProvider() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    protected ScmCommandTestCase(boolean spaceInPath) {
        super(spaceInPath);
    }

    /**
     * A mock implementation of {@link WindowsServiceControl} that records calls and can be
     * configured to throw exceptions.
     */
    static class MockWindowsServiceControl implements WindowsServiceControl {
        String lastOperation;
        String lastServiceId;
        WindowsServiceException startException;
        WindowsServiceException stopException;
        WindowsServiceException deleteException;
        WindowsServiceException queryException;
        ServiceStatus queryResult = new ServiceStatus(ServiceStatus.STOPPED, 0, 0, 0);

        @Override
        public void startService(String serviceId) throws WindowsServiceException {
            lastOperation = "start";
            lastServiceId = serviceId;
            if (startException != null) {
                throw startException;
            }
        }

        @Override
        public void stopService(String serviceId) throws WindowsServiceException {
            lastOperation = "stop";
            lastServiceId = serviceId;
            if (stopException != null) {
                throw stopException;
            }
        }

        @Override
        public void deleteService(String serviceId) throws WindowsServiceException {
            lastOperation = "delete";
            lastServiceId = serviceId;
            if (deleteException != null) {
                throw deleteException;
            }
        }

        @Override
        public ServiceStatus queryStatus(String serviceId) throws WindowsServiceException {
            if (queryException != null) {
                throw queryException;
            }
            return queryResult;
        }
    }

    @Before
    public void resetMock() {
        mockServiceControl = new MockWindowsServiceControl();
    }

    protected abstract String getDefaultSuccessMessage();

    protected abstract String getDefaultFailureMessage();

    protected abstract String getExpectedOperation();

    public void testDefaultCommand() throws Exception {
        assertOkWithOutput(containsString(getDefaultSuccessMessage()), emptyString());
        assertThat(mockServiceControl.lastOperation, equalTo(getExpectedOperation()));
        assertThat(mockServiceControl.lastServiceId, equalTo("elasticsearch-service-x64"));
    }

    public void testServiceId() throws Exception {
        assertUsage(containsString("too many arguments"), "servicename", "servicename");
        terminal.reset();
        assertOkWithOutput(
            containsString(getDefaultSuccessMessage().replace("elasticsearch-service-x64", "my-service-id")),
            emptyString(),
            "my-service-id"
        );
        assertThat(mockServiceControl.lastServiceId, equalTo("my-service-id"));
    }

    public void testServiceIdFromEnv() throws Exception {
        envVars.put("SERVICE_ID", "my-service-id");
        assertOkWithOutput(containsString(getDefaultSuccessMessage().replace("elasticsearch-service-x64", "my-service-id")), emptyString());
        assertThat(mockServiceControl.lastServiceId, equalTo("my-service-id"));
    }
}
