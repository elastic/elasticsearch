/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.datasources.FaultInjectingS3HttpHandler;
import org.elasticsearch.xpack.esql.datasources.FaultInjectingS3HttpHandler.FaultType;
import org.junit.After;

import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;

/**
 * Integration tests verifying that distributed external source queries recover from
 * transient S3 faults (503, 500, connection reset) via the retry policy, and fail
 * cleanly when faults exceed the retry budget.
 */
@SuppressForbidden(reason = "uses FaultInjectingS3HttpHandler which wraps HttpHandler for S3 fault injection")
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
public class ExternalDistributedResilienceIT extends AbstractExternalDistributedIT {

    @After
    public void clearFaults() {
        faultHandler().clearFault();
    }

    private FaultInjectingS3HttpHandler faultHandler() {
        return s3Fixture.getFaultHandler();
    }

    private String employeesQuery() {
        return externalS3Query(WAREHOUSE + "/standalone/employees.parquet") + " | KEEP emp_no, first_name, salary | SORT emp_no | LIMIT 5";
    }

    public void testNoFaultQuerySucceeds() throws Exception {
        s3Fixture.loadFixturesFromResources();
        faultHandler().clearFault();

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values in result for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty results for mode %s", mode), values.isEmpty());
        }
    }

    public void testTransient503RecoveryDuringDistributedQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_503, 2);

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values after 503 recovery for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty results after 503 recovery for mode %s", mode), values.isEmpty());
            assertEquals(Strings.format("All faults should have been consumed for mode %s", mode), 0, faultHandler().remainingFaults());
        }
    }

    public void testTransient500RecoveryDuringDistributedQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_500, 2);

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values after 500 recovery for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty results after 500 recovery for mode %s", mode), values.isEmpty());
            assertEquals(Strings.format("All faults should have been consumed for mode %s", mode), 0, faultHandler().remainingFaults());
        }
    }

    public void testConnectionResetRecoveryDuringDistributedQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.CONNECTION_RESET, 2);

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values after connection reset recovery for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty results after connection reset recovery for mode %s", mode), values.isEmpty());
            assertEquals(Strings.format("All faults should have been consumed for mode %s", mode), 0, faultHandler().remainingFaults());
        }
    }

    public void testPersistentFaultsFailQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_503, 100);

            Exception ex = expectThrows(Exception.class, () -> runQueryWithMode(employeesQuery(), mode));
            if (ex instanceof ResponseException responseEx) {
                String responseBody = new String(responseEx.getResponse().getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
                assertTrue(
                    Strings.format("Expected storage error in response for mode %s, got: %s", mode, responseBody),
                    responseBody.contains("503")
                        || responseBody.contains("Service Unavailable")
                        || responseBody.contains("SlowDown")
                        || responseBody.contains("storage")
                        || responseBody.contains("error")
                );
            } else {
                // A Parquet read involves multiple sequential S3 operations, each with its own
                // retry budget. Under persistent throttling the cumulative retry time can exceed
                // the REST client's socket timeout, which is an acceptable failure mode.
                assertTrue(
                    Strings.format("Expected ResponseException or SocketTimeoutException for mode %s, got: %s", mode, ex.getClass()),
                    hasCause(ex, SocketTimeoutException.class)
                );
            }

            faultHandler().clearFault();
        }
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }

    public void testPathFilteredFaultsOnlyAffectParquetReads() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_503, 2, path -> path.endsWith(".parquet"));

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values after path-filtered fault recovery for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty results for mode %s", mode), values.isEmpty());
        }
    }
}
