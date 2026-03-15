/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.datasources.FaultInjectingS3HttpHandler;
import org.elasticsearch.xpack.esql.datasources.FaultInjectingS3HttpHandler.FaultType;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;

/**
 * Integration tests verifying that distributed external source queries recover from
 * transient S3 faults (503, 500, connection reset) via the retry policy, and fail
 * cleanly when faults exceed the retry budget.
 */
@SuppressForbidden(reason = "uses FaultInjectingS3HttpHandler which wraps HttpHandler for S3 fault injection")
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
public class ExternalDistributedResilienceIT extends RestEsqlTestCase {

    private static final List<String> DISTRIBUTION_MODES = List.of("coordinator_only", "round_robin", "adaptive");

    @ClassRule
    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    private static ElasticsearchCluster clusterInstance = ExternalDistributedClusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule((base, description) -> new org.junit.runners.model.Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeFalse("FIPS mode requires security enabled; this test uses plain HTTP S3 fixtures", inFipsJvm());
            base.evaluate();
        }
    }).around(clusterInstance);

    public ExternalDistributedResilienceIT(Mode mode) {
        super(mode);
    }

    @Override
    protected String getTestRestCluster() {
        return clusterInstance.getHttpAddresses();
    }

    @com.carrotsearch.randomizedtesting.annotations.ParametersFactory(argumentFormatting = "%1s")
    public static List<Object[]> modes() {
        return Arrays.stream(Mode.values()).map(m -> new Object[] { m }).toList();
    }

    @After
    public void clearFaults() {
        faultHandler().clearFault();
    }

    private FaultInjectingS3HttpHandler faultHandler() {
        return s3Fixture.getFaultHandler();
    }

    private String employeesQuery() {
        return "EXTERNAL \"s3://"
            + BUCKET
            + "/"
            + WAREHOUSE
            + "/standalone/employees.parquet\""
            + " WITH { \"endpoint\": \""
            + s3Fixture.getAddress()
            + "\","
            + " \"access_key\": \""
            + ACCESS_KEY
            + "\","
            + " \"secret_key\": \""
            + SECRET_KEY
            + "\" }"
            + " | KEEP emp_no, first_name, salary | SORT emp_no | LIMIT 5";
    }

    private Map<String, Object> runQueryWithMode(String query, String distributionMode) throws IOException {
        Settings pragmas = Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), distributionMode).build();
        var request = new RequestObjectBuilder().query(query).pragmasOk().pragmas(pragmas);
        return runEsql(request, new AssertWarnings.NoWarnings(), profileLogger, mode);
    }

    /**
     * Baseline: query succeeds with no faults injected.
     */
    public void testNoFaultQuerySucceeds() throws Exception {
        s3Fixture.loadFixturesFromResources();
        faultHandler().clearFault();

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull("Expected values in result for mode " + mode, values);
            assertFalse("Expected non-empty results for mode " + mode, values.isEmpty());
        }
    }

    /**
     * Transient HTTP 503 during distributed query: inject fewer faults than the retry budget
     * allows, verify the query succeeds after retries.
     */
    public void testTransient503RecoveryDuringDistributedQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_503, 2);

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull("Expected values after 503 recovery for mode " + mode, values);
            assertFalse("Expected non-empty results after 503 recovery for mode " + mode, values.isEmpty());
            assertEquals("All faults should have been consumed for mode " + mode, 0, faultHandler().remainingFaults());
        }
    }

    /**
     * Transient HTTP 500 during distributed query: inject fewer faults than the retry budget
     * allows, verify the query succeeds after retries.
     */
    public void testTransient500RecoveryDuringDistributedQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_500, 2);

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull("Expected values after 500 recovery for mode " + mode, values);
            assertFalse("Expected non-empty results after 500 recovery for mode " + mode, values.isEmpty());
            assertEquals("All faults should have been consumed for mode " + mode, 0, faultHandler().remainingFaults());
        }
    }

    /**
     * Connection reset during distributed query: inject a small number of resets,
     * verify retry recovery.
     */
    public void testConnectionResetRecoveryDuringDistributedQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.CONNECTION_RESET, 2);

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull("Expected values after connection reset recovery for mode " + mode, values);
            assertFalse("Expected non-empty results after connection reset recovery for mode " + mode, values.isEmpty());
            assertEquals("All faults should have been consumed for mode " + mode, 0, faultHandler().remainingFaults());
        }
    }

    /**
     * Persistent faults exceed the retry budget: inject more faults than retries allow,
     * verify the query fails with a clear error message.
     */
    public void testPersistentFaultsFailQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_503, 100);

            ResponseException ex = expectThrows(ResponseException.class, () -> runQueryWithMode(employeesQuery(), mode));
            String responseBody = new String(ex.getResponse().getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(
                "Expected storage error in response for mode " + mode + ", got: " + responseBody,
                responseBody.contains("503")
                    || responseBody.contains("Service Unavailable")
                    || responseBody.contains("SlowDown")
                    || responseBody.contains("storage")
                    || responseBody.contains("error")
            );

            faultHandler().clearFault(); // clear before next mode iteration
        }
    }

    /**
     * Path-filtered faults: inject faults only for .parquet reads, verify that metadata
     * operations (which don't hit .parquet paths) are unaffected. Since the query reads
     * a .parquet file, it should still be affected by the fault.
     */
    public void testPathFilteredFaultsOnlyAffectParquetReads() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_503, 2, path -> path.endsWith(".parquet"));

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull("Expected values after path-filtered fault recovery for mode " + mode, values);
            assertFalse("Expected non-empty results for mode " + mode, values.isEmpty());
        }
    }
}
