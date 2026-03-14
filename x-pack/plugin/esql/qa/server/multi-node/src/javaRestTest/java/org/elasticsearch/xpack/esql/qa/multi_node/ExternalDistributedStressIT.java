/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
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
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.addBlobToFixture;

/**
 * Distributed stress tests for many-split external queries. Verifies that the
 * distributed execution framework handles 500–1500+ splits without split starvation,
 * exchange deadlocks, or memory issues. Uses synthetic CSV files with deterministic
 * content for formula-based assertions across all distribution modes.
 */
@SuppressForbidden(reason = "uses S3 fixture handler for direct blob population")
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
public class ExternalDistributedStressIT extends RestEsqlTestCase {

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

    public ExternalDistributedStressIT(Mode mode) {
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

    /**
     * Generate CSV content for a single split with schema split_id,row_id,value
     * where value = splitId * rowsPerSplit + rowId.
     */
    private static byte[] generateCsvSplit(int splitId, int numRows, int rowsPerSplit) {
        StringBuilder sb = new StringBuilder();
        sb.append("split_id,row_id,value\n");
        for (int rowId = 0; rowId < numRows; rowId++) {
            int value = splitId * rowsPerSplit + rowId;
            sb.append(splitId).append(",").append(rowId).append(",").append(value).append("\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Populate the S3 fixture with uniform stress splits. Each split has rowsPerSplit rows.
     */
    private void populateStressSplits(int numSplits, int rowsPerSplit) {
        var handler = s3Fixture.getHandler();
        for (int i = 0; i < numSplits; i++) {
            String key = WAREHOUSE + "/stress/part-" + String.format("%05d", i) + ".csv";
            byte[] content = generateCsvSplit(i, rowsPerSplit, rowsPerSplit);
            addBlobToFixture(handler, key, content);
        }
    }

    /**
     * Populate the S3 fixture with heterogeneous splits. Each split i has rowCounts[i] rows.
     * Value formula: value = globalRowOffset + rowId where globalRowOffset is the cumulative
     * row count of all previous splits.
     */
    private void populateHeterogeneousSplits(int numSplits, int[] rowCounts) {
        var handler = s3Fixture.getHandler();
        int globalOffset = 0;
        for (int i = 0; i < numSplits; i++) {
            int numRows = rowCounts[i];
            StringBuilder sb = new StringBuilder();
            sb.append("split_id,row_id,value\n");
            for (int rowId = 0; rowId < numRows; rowId++) {
                int value = globalOffset + rowId;
                sb.append(i).append(",").append(rowId).append(",").append(value).append("\n");
            }
            globalOffset += numRows;
            String key = WAREHOUSE + "/stress/part-" + String.format("%05d", i) + ".csv";
            addBlobToFixture(handler, key, sb.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    private String stressQuery(String globPattern) {
        return "EXTERNAL \"s3://"
            + BUCKET
            + "/"
            + globPattern
            + "\""
            + " WITH { \"endpoint\": \""
            + s3Fixture.getAddress()
            + "\","
            + " \"access_key\": \""
            + ACCESS_KEY
            + "\","
            + " \"secret_key\": \""
            + SECRET_KEY
            + "\" }";
    }

    private Map<String, Object> runQueryWithMode(String query, String distributionMode) throws IOException {
        Settings pragmas = Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), distributionMode).build();
        var request = new RequestObjectBuilder().query(query).pragmasOk().pragmas(pragmas);
        return runEsql(request, new AssertWarnings.NoWarnings(), profileLogger, mode);
    }

    public void testManyUniformSplits() throws Exception {
        int numSplits = randomIntBetween(500, 1500);
        int rowsPerSplit = randomIntBetween(50, 200);
        populateStressSplits(numSplits, rowsPerSplit);

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(stressQuery(WAREHOUSE + "/stress/*.csv") + " | STATS count = COUNT(*)", mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull("Expected values for mode " + mode, values);
            assertEquals("Expected single row for mode " + mode, 1, values.size());
            long count = ((Number) values.get(0).get(0)).longValue();
            assertEquals("Row count mismatch for mode " + mode, (long) numSplits * rowsPerSplit, count);
        }
    }

    public void testManyUniformSplitsWithAggregation() throws Exception {
        int numSplits = randomIntBetween(500, 1500);
        int rowsPerSplit = randomIntBetween(50, 200);
        populateStressSplits(numSplits, rowsPerSplit);

        long totalRows = (long) numSplits * rowsPerSplit;
        long expectedTotal = totalRows * (totalRows - 1) / 2;

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(
                stressQuery(WAREHOUSE + "/stress/*.csv") + " | STATS count = COUNT(*), total = SUM(value)",
                mode
            );
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull("Expected values for mode " + mode, values);
            assertEquals("Expected single row for mode " + mode, 1, values.size());
            long count = ((Number) values.get(0).get(0)).longValue();
            long total = ((Number) values.get(0).get(1)).longValue();
            assertEquals("Row count mismatch for mode " + mode, totalRows, count);
            assertEquals("Sum mismatch for mode " + mode, expectedTotal, total);
        }
    }

    public void testManyUniformSplitsWithTopN() throws Exception {
        int numSplits = randomIntBetween(500, 1500);
        int rowsPerSplit = randomIntBetween(50, 200);
        populateStressSplits(numSplits, rowsPerSplit);

        long totalRows = (long) numSplits * rowsPerSplit;

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(
                stressQuery(WAREHOUSE + "/stress/*.csv") + " | KEEP split_id, row_id, value | SORT value DESC | LIMIT 10",
                mode
            );
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull("Expected values for mode " + mode, values);
            assertEquals("Expected 10 rows for mode " + mode, 10, values.size());
            for (int i = 0; i < 10; i++) {
                long expectedValue = totalRows - 1 - i;
                long actualValue = ((Number) values.get(i).get(2)).longValue();
                assertEquals("Top-N value mismatch at index " + i + " for mode " + mode, expectedValue, actualValue);
            }
        }
    }

    public void testHeterogeneousSplitSizes() throws Exception {
        int numSplits = randomIntBetween(500, 1500);
        int[] rowCounts = new int[numSplits];
        long totalRows = 0;
        for (int i = 0; i < numSplits; i++) {
            rowCounts[i] = randomIntBetween(10, 500);
            totalRows += rowCounts[i];
        }
        populateHeterogeneousSplits(numSplits, rowCounts);

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(stressQuery(WAREHOUSE + "/stress/*.csv") + " | STATS count = COUNT(*)", mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull("Expected values for mode " + mode, values);
            assertEquals("Expected single row for mode " + mode, 1, values.size());
            long count = ((Number) values.get(0).get(0)).longValue();
            assertEquals("Row count mismatch for mode " + mode, totalRows, count);
        }
    }
}
