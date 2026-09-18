/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Multi-JVM coverage for external-text aggregate-metadata pushdown.
 * <p>
 * The single-JVM {@code internalClusterTest}s (e.g. {@code ExternalCsvAggregatePushdownIT}) cannot
 * exercise the cross-node stats return-flow: every node shares one JVM-static {@code ExternalStats},
 * so a data node's capture is visible to the coordinator through the static cache regardless of whether
 * {@code DriverCompletionInfo.capturedSourceMetadata} works. This test runs a real two-process cluster
 * with split roles (coordinator-only node 0, data-only node 1). Both queries target node 0; the cold
 * scan runs on node 1 (separate JVM, separate static cache), so the only way node 0's planning-time
 * cache can be populated for the warm query is the return-flow. A warm {@code documents_found == 0}
 * therefore proves the coordinator reconciled the data node's shipped-back stats.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ExternalCsvMultiNodePushdownIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(ExternalCsvMultiNodePushdownIT.class);

    /** standalone/employees.csv carries 100 data rows (101 lines incl. header). */
    private static final long EMPLOYEE_ROWS = 100L;

    private static final String LOCAL_DATA_SOURCE = "local_csv_ds";
    private static final String DATASET = "employees_local";

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    public static ElasticsearchCluster cluster = Clusters.multiNodeTestCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    private static Path localFixturesPath;

    /** Datasets and {@code local} data sources are gated to snapshot builds today (same gate as {@code DataSourceCrudRestIT}). */
    @BeforeClass
    public static void requireSnapshotBuild() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void resolveLocalFixtures() {
        localFixturesPath = FixtureUtils.resolveLocalFixturesPath(logger, ExternalCsvMultiNodePushdownIT.class);
    }

    @AfterClass
    public static void cleanupDatasets() throws Exception {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    @Override
    protected String getTestRestCluster() {
        // Pin every request to node 0 (coordinator-only). Both the cold and warm query must hit the
        // same coordinator: the reconciled schema cache is per-coordinator, not cluster-replicated.
        return cluster.getHttpAddress(0);
    }

    public void testCountStarColdThenWarmShortCircuitsAcrossNodes() throws Exception {
        assumeTrue("LOCAL fixtures unavailable (packaged in a JAR)", localFixturesPath != null);
        String uri = localFixturesPath.resolve("standalone/employees.csv").toUri().toString();
        // Register a local-file data source + dataset over the fixture, then read it via FROM <dataset>. The
        // multi_value_syntax=brackets dataset setting mirrors the old inline WITH: employees.csv encodes
        // multi-value columns with bracket syntax (e.g. [val1,val2]); without it the commas inside brackets
        // are treated as column separators, producing a column-count mismatch at row 1. Format is inferred
        // from the .csv extension.
        DatasetRegistry.ensureDataSource(client(), LOCAL_DATA_SOURCE, "local", Map.of());
        DatasetRegistry.ensureDataset(client(), DATASET, LOCAL_DATA_SOURCE, uri, "{ \"multi_value_syntax\": \"brackets\" }");
        String query = "FROM " + DATASET + " | STATS c = COUNT(*)";

        // Cold: coordinator (no data role) dispatches the scan to the data node, which captures the
        // file's stats in its own JVM and ships them back via DriverCompletionInfo.
        Map<String, Object> cold = runEsql(query);
        assertEquals("cold COUNT(*) must be exact", EMPLOYEE_ROWS, count(cold));
        assertThat("cold run must scan rows", documentsFound(cold), greaterThan(0L));
        assertEquals("the external scan must run on the data node, not the coordinator", "data-node", scanNodeName(cold));

        // Warm: node 0's planning-time cache was enriched only by the return-flow (its own static
        // cache never saw the scan), so the optimizer short-circuits to a LocalSourceExec.
        Map<String, Object> warm = runEsql(query);
        assertEquals("warm COUNT(*) must stay exact", EMPLOYEE_ROWS, count(warm));
        assertEquals("warm run must short-circuit via the cross-node return-flow (zero docs scanned)", 0L, documentsFound(warm));
    }

    private Map<String, Object> runEsql(String query) throws Exception {
        Request request = new Request("POST", "/_query");
        String escaped = query.replace("\\", "\\\\").replace("\"", "\\\"");
        // external_distribution=round_robin forces the scan onto a data node (the adaptive default
        // would run a single small split locally on the coordinator, defeating the cross-JVM intent).
        request.setJsonEntity("{\"query\":\"" + escaped + "\",\"profile\":true,\"pragma\":{\"external_distribution\":\"round_robin\"}}");
        // ES|QL warns "No limit defined" for a bare aggregate; that's expected here, not a failure.
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    /** Name of the node whose driver ran the external-source operator (the file scan). */
    @SuppressWarnings("unchecked")
    private static String scanNodeName(Map<String, Object> response) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        List<Map<String, Object>> drivers = (List<Map<String, Object>>) profile.get("drivers");
        for (Map<String, Object> driver : drivers) {
            List<Map<String, Object>> operators = (List<Map<String, Object>>) driver.get("operators");
            for (Map<String, Object> operator : operators) {
                // The operator class is {@code AsyncExternalSourceOperator} but its profile-facing
                // toString() drops the {@code Async} prefix for user readability — match on the
                // public name to stay in sync with the profile output.
                if (String.valueOf(operator.get("operator")).startsWith("ExternalDataSourceOperator")) {
                    return String.valueOf(driver.get("node_name"));
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static long count(Map<String, Object> response) {
        List<List<Object>> values = (List<List<Object>>) response.get("values");
        return ((Number) values.get(0).get(0)).longValue();
    }

    private static long documentsFound(Map<String, Object> response) {
        return ((Number) response.get("documents_found")).longValue();
    }
}
