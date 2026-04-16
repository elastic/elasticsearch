/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.addBlobToFixture;

/**
 * Distributed stress tests for many-split external queries. Verifies that the
 * distributed execution framework handles 50-200 splits without split starvation,
 * exchange deadlocks, or memory issues. Uses synthetic CSV files with deterministic
 * content for formula-based assertions across all distribution modes.
 * <p>
 * All synthetic data is held entirely in-memory via the S3 fixture's blob map.
 * <p>
 * Each test method uses a unique sub-prefix under {@code warehouse/stress/} so the
 * coordinator's external source listing cache (keyed by glob path) cannot return a
 * stale {@code FileList} from a previous method after blobs were cleared and
 * repopulated (see elasticsearch#145559).
 */
@SuppressForbidden(reason = "uses S3 fixture handler for direct blob population")
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
public class ExternalDistributedStressIT extends AbstractExternalDistributedIT {

    /** Per-test run prefix; includes a random segment so listing cache keys differ each method. */
    private String stressRunPrefix;

    @Before
    public void initStressRunPrefix() {
        stressRunPrefix = WAREHOUSE + "/stress/" + UUID.randomUUID() + "/";
    }

    @org.junit.After
    public void clearStressSplits() {
        var handler = s3Fixture.getHandler();
        String blobPrefix = Strings.format("/%s/%s", BUCKET, stressRunPrefix);
        Iterator<String> it = handler.blobs().keySet().iterator();
        while (it.hasNext()) {
            if (it.next().startsWith(blobPrefix)) {
                it.remove();
            }
        }
    }

    private static byte[] generateCsvSplit(int splitId, int rowsPerSplit) {
        StringBuilder sb = new StringBuilder();
        sb.append("split_id,row_id,value\n");
        for (int rowId = 0; rowId < rowsPerSplit; rowId++) {
            int value = splitId * rowsPerSplit + rowId;
            sb.append(splitId).append(",").append(rowId).append(",").append(value).append("\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private void populateStressSplits(int numSplits, int rowsPerSplit) {
        var handler = s3Fixture.getHandler();
        for (int i = 0; i < numSplits; i++) {
            String key = String.format(Locale.ROOT, "%spart-%05d.csv", stressRunPrefix, i);
            addBlobToFixture(handler, key, generateCsvSplit(i, rowsPerSplit));
        }
    }

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
            String key = String.format(Locale.ROOT, "%spart-%05d.csv", stressRunPrefix, i);
            addBlobToFixture(handler, key, sb.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    private String stressQuery(String suffix) {
        return externalS3Query(stressRunPrefix + "*.csv") + suffix;
    }

    public void testManyUniformSplits() throws Exception {
        int numSplits = randomIntBetween(50, 200);
        int rowsPerSplit = randomIntBetween(10, 50);
        populateStressSplits(numSplits, rowsPerSplit);

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(stressQuery(" | STATS count = COUNT(*)"), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values for mode %s", mode), values);
            assertEquals(Strings.format("Expected single row for mode %s", mode), 1, values.size());
            long count = ((Number) values.get(0).get(0)).longValue();
            assertEquals(Strings.format("Row count mismatch for mode %s", mode), (long) numSplits * rowsPerSplit, count);
        }
    }

    public void testManyUniformSplitsWithAggregation() throws Exception {
        int numSplits = randomIntBetween(50, 200);
        int rowsPerSplit = randomIntBetween(10, 50);
        populateStressSplits(numSplits, rowsPerSplit);

        long totalRows = (long) numSplits * rowsPerSplit;
        long expectedTotal = totalRows * (totalRows - 1) / 2;

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(stressQuery(" | STATS count = COUNT(*), total = SUM(value)"), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values for mode %s", mode), values);
            assertEquals(Strings.format("Expected single row for mode %s", mode), 1, values.size());
            long count = ((Number) values.get(0).get(0)).longValue();
            long total = ((Number) values.get(0).get(1)).longValue();
            assertEquals(Strings.format("Row count mismatch for mode %s", mode), totalRows, count);
            assertEquals(Strings.format("Sum mismatch for mode %s", mode), expectedTotal, total);
        }
    }

    public void testManyUniformSplitsWithTopN() throws Exception {
        int numSplits = randomIntBetween(50, 200);
        int rowsPerSplit = randomIntBetween(10, 50);
        populateStressSplits(numSplits, rowsPerSplit);

        long totalRows = (long) numSplits * rowsPerSplit;

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(
                stressQuery(" | KEEP split_id, row_id, value | SORT value DESC | LIMIT 10"),
                mode
            );
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values for mode %s", mode), values);
            assertEquals(Strings.format("Expected 10 rows for mode %s", mode), 10, values.size());
            for (int i = 0; i < 10; i++) {
                long expectedValue = totalRows - 1 - i;
                long actualValue = ((Number) values.get(i).get(2)).longValue();
                assertEquals(Strings.format("Top-N value mismatch at index %d for mode %s", i, mode), expectedValue, actualValue);
            }
        }
    }

    public void testHeterogeneousSplitSizes() throws Exception {
        int numSplits = randomIntBetween(50, 200);
        int[] rowCounts = new int[numSplits];
        long totalRows = 0;
        for (int i = 0; i < numSplits; i++) {
            rowCounts[i] = randomIntBetween(5, 50);
            totalRows += rowCounts[i];
        }
        populateHeterogeneousSplits(numSplits, rowCounts);

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(stressQuery(" | STATS count = COUNT(*)"), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values for mode %s", mode), values);
            assertEquals(Strings.format("Expected single row for mode %s", mode), 1, values.size());
            long count = ((Number) values.get(0).get(0)).longValue();
            assertEquals(Strings.format("Row count mismatch for mode %s", mode), totalRows, count);
        }
    }
}
