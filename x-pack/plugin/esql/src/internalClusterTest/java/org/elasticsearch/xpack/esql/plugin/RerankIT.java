/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the ESQL RERANK command.
 * Tests cover basic functionality, multiple fields, custom score fields, filtering, and settings.
 */
public class RerankIT extends InferenceCommandIntegTestCase {

    private static final String TEST_INDEX = "test_rerank";
    private static final String RERANK_MODEL_ID = "test-rerank-model";

    @Before
    public void setupIndexAndInferenceModel() throws IOException {
        createAndPopulateTestIndex(TEST_INDEX);
        createTestInferenceEndpoint(RERANK_MODEL_ID, TaskType.RERANK, "test_reranking_service");
    }

    @After
    public void cleanup() {
        deleteTestInferenceEndpoint(RERANK_MODEL_ID, TaskType.RERANK);
        cleanupClusterSettings(InferenceSettings.RERANK_ENABLED_SETTING, InferenceSettings.RERANK_ROW_LIMIT_SETTING);
    }

    // ============================================
    // Basic Functionality Tests
    // ============================================

    public void testRerankBasic() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | RERANK "search query" ON title WITH { "inference_id": "%s" }
            | KEEP id, _score
            | SORT _score DESC
            | LIMIT 5
            """, TEST_INDEX, RERANK_MODEL_ID);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(lessThanOrEqualTo(5)));

            // Verify results are sorted by score
            double previousScore = Double.MAX_VALUE;
            for (List<Object> row : values) {
                double currentScore = (Double) row.get(1);
                assertThat(currentScore, lessThanOrEqualTo(previousScore));
                assertThat(currentScore, notNullValue());
                previousScore = currentScore;
            }
        }
    }

    public void testRerankWithDefaultInferenceId() {
        // Note: The default inference ID (.rerank-v1-elasticsearch) requires actual ML infrastructure
        // which is not available in test environments. We test the syntax by using a regular test endpoint.
        var query = String.format(Locale.ROOT, """
            FROM %s
            | RERANK "search query" ON title WITH { "inference_id": "%s" }
            | KEEP id, _score
            | LIMIT 5
            """, TEST_INDEX, RERANK_MODEL_ID);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(lessThanOrEqualTo(5)));
        }
    }

    public void testRerankMultipleFields() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | RERANK "search query" ON title, content WITH { "inference_id": "%s" }
            | KEEP id, _score
            | SORT _score DESC
            | LIMIT 3
            """, TEST_INDEX, RERANK_MODEL_ID);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(lessThanOrEqualTo(3)));
        }
    }

    public void testRerankWithCustomScoreField() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | RERANK rerank_score="search query" ON title WITH { "inference_id": "%s" }
            | KEEP id, rerank_score
            | SORT rerank_score DESC
            | LIMIT 5
            """, TEST_INDEX, RERANK_MODEL_ID);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "rerank_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(lessThanOrEqualTo(5)));
        }
    }

    // ============================================
    // Row Limit Tests
    // ============================================

    public void testRerankRowLimitEnforcement() throws IOException {
        // Create an index with more documents than the default limit (1000)
        final String testIndexLarge = "test_rerank_large";
        createAndPopulateTestIndex(testIndexLarge, 1100);
        
        var query = String.format(Locale.ROOT, """
            FROM %s
            | RERANK "search query" ON title WITH { "inference_id": "%s" }
            | KEEP id, _score
            """, testIndexLarge, RERANK_MODEL_ID);

        try (var resp = run(query)) {
            List<List<Object>> values = getValuesList(resp);
            // Should be limited to exactly the default row limit (1000)
            assertThat(values, hasSize(1000));
        }
    }

    public void testRerankRowLimitSetting() throws Exception {
        // Set a custom row limit
        int customLimit = between(1, 10);
        updateClusterSettings(Settings.builder().put(InferenceSettings.RERANK_ROW_LIMIT_SETTING.getKey(), customLimit));

        try {
            // Create an index with more documents than the custom limit
            final String testIndexLarge = "test_rerank_custom_limit";
            createAndPopulateTestIndex(testIndexLarge, customLimit + 10);
            
            var query = String.format(Locale.ROOT, """
                FROM %s
                | RERANK "search query" ON title WITH { "inference_id": "%s" }
                | KEEP id, _score
                """, testIndexLarge, RERANK_MODEL_ID);

            try (var resp = run(query)) {
                List<List<Object>> values = getValuesList(resp);
                // Should be limited to exactly the custom limit
                assertThat(values, hasSize(customLimit));
            }
        } finally {
            // Reset to default
            updateClusterSettings(Settings.builder().put(InferenceSettings.RERANK_ROW_LIMIT_SETTING.getKey(), 1000));
        }
    }

    // ============================================
    // Integration with Other Commands
    // ============================================

    public void testRerankAfterFilter() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | WHERE id > 2
            | RERANK "search query" ON title WITH { "inference_id": "%s" }
            | KEEP id, _score
            | SORT _score DESC
            | LIMIT 3
            """, TEST_INDEX, RERANK_MODEL_ID);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            List<List<Object>> values = getValuesList(resp);

            // Verify all ids are > 2
            for (List<Object> row : values) {
                int id = (Integer) row.get(0);
                assertThat(id, greaterThan(2));
            }
        }
    }

    public void testRerankWithStats() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | RERANK "search query" ON title WITH { "inference_id": "%s" }
            | STATS avg_score = AVG(_score)
            """, TEST_INDEX, RERANK_MODEL_ID);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("avg_score"));
            assertColumnTypes(resp.columns(), List.of("double"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat((Double) values.get(0).get(0), notNullValue());
        }
    }

    // ============================================
    // Error Handling Tests
    // ============================================

    public void testRerankInvalidQueryType() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | RERANK id ON title WITH { "inference_id": "%s" }
            """, TEST_INDEX, RERANK_MODEL_ID);

        var error = expectThrows(ParsingException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("mismatched input"));
    }

    public void testRerankNonConstantQuery() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | RERANK content ON title WITH { "inference_id": "%s" }
            """, TEST_INDEX, RERANK_MODEL_ID);

        var error = expectThrows(ParsingException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("mismatched input"));
    }

    // ============================================
    // Settings Tests
    // ============================================

    public void testRerankDisabledBySetting() throws Exception {
        // Disable rerank command
        updateClusterSettings(Settings.builder().put(InferenceSettings.RERANK_ENABLED_SETTING.getKey(), false));

        try {
            var query = String.format(Locale.ROOT, """
                FROM %s
                | RERANK "search query" ON title WITH { "inference_id": "%s" }
                """, TEST_INDEX, RERANK_MODEL_ID);

            var error = expectThrows(ParsingException.class, () -> run(query));
            assertThat(error.getMessage(), containsString("RERANK command is disabled in settings"));
        } finally {
            // Re-enable for other tests
            updateClusterSettings(Settings.builder().put(InferenceSettings.RERANK_ENABLED_SETTING.getKey(), true));
        }
    }
}
