/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the ESQL COMPLETION command.
 * Tests cover basic functionality, custom field names, filtering, concatenated prompts, and settings.
 */
public class CompletionIT extends InferenceCommandIntegTestCase {

    private static final String TEST_INDEX = "test_completion";
    private static final String COMPLETION_MODEL_ID = "test-completion-model";

    @Before
    public void setupIndexAndInferenceModel() throws IOException {
        createAndPopulateTestIndex(TEST_INDEX);
        createTestInferenceEndpoint(COMPLETION_MODEL_ID, TaskType.COMPLETION, "completion_test_service");
    }

    @After
    public void cleanup() {
        deleteTestInferenceEndpoint(COMPLETION_MODEL_ID, TaskType.COMPLETION);
        cleanupClusterSettings(InferenceSettings.COMPLETION_ENABLED_SETTING, InferenceSettings.COMPLETION_ROW_LIMIT_SETTING);
    }

    // ============================================
    // Basic Functionality Tests
    // ============================================

    public void testCompletionBasic() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | COMPLETION title WITH { "inference_id": "%s" }
            | KEEP id, completion
            | LIMIT 5
            """, TEST_INDEX, COMPLETION_MODEL_ID);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "completion"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(lessThanOrEqualTo(5)));

            // Verify completion field is not null
            for (List<Object> row : values) {
                assertThat(row.get(1), notNullValue());
            }
        }
    }

    public void testCompletionWithCustomFieldName() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | COMPLETION generated_text = title WITH { "inference_id": "%s" }
            | KEEP id, generated_text
            | LIMIT 3
            """, TEST_INDEX, COMPLETION_MODEL_ID);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "generated_text"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(lessThanOrEqualTo(3)));
        }
    }

    public void testCompletionConcatenatedPrompt() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | COMPLETION summary = CONCAT(title, ": ", content) WITH { "inference_id": "%s" }
            | KEEP id, summary
            | LIMIT 5
            """, TEST_INDEX, COMPLETION_MODEL_ID);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "summary"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(lessThanOrEqualTo(5)));

            // Verify summary field is not null
            for (List<Object> row : values) {
                assertThat(row.get(1), notNullValue());
            }
        }
    }

    // ============================================
    // Row Limit Tests
    // ============================================

    public void testCompletionRowLimitEnforcement() throws IOException {
        // Create an index with more documents than the default limit (100)
        final String testIndexLarge = "test_completion_large";
        createAndPopulateTestIndex(testIndexLarge, 150);
        
        var query = String.format(Locale.ROOT, """
            FROM %s
            | COMPLETION title WITH { "inference_id": "%s" }
            | KEEP id, completion
            """, testIndexLarge, COMPLETION_MODEL_ID);

        try (var resp = run(query)) {
            List<List<Object>> values = getValuesList(resp);
            // Should be limited to exactly the default row limit (100)
            assertThat(values, hasSize(100));
        }
    }

    public void testCompletionRowLimitSetting() throws Exception {
        // Set a custom row limit
        int customLimit = between(1, 10);
        updateClusterSettings(Settings.builder().put(InferenceSettings.COMPLETION_ROW_LIMIT_SETTING.getKey(), customLimit));

        try {
            // Create an index with more documents than the custom limit
            final String testIndexLarge = "test_completion_custom_limit";
            createAndPopulateTestIndex(testIndexLarge, customLimit + 10);
            
            var query = String.format(Locale.ROOT, """
                FROM %s
                | COMPLETION title WITH { "inference_id": "%s" }
                | KEEP id, completion
                """, testIndexLarge, COMPLETION_MODEL_ID);

            try (var resp = run(query)) {
                List<List<Object>> values = getValuesList(resp);
                // Should be limited to exactly the custom limit
                assertThat(values, hasSize(customLimit));
            }
        } finally {
            // Reset to default
            updateClusterSettings(Settings.builder().put(InferenceSettings.COMPLETION_ROW_LIMIT_SETTING.getKey(), 100));
        }
    }

    // ============================================
    // Integration with Other Commands
    // ============================================

    public void testCompletionWithFilter() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | WHERE id <= 3
            | COMPLETION title WITH { "inference_id": "%s" }
            | KEEP id, completion
            | SORT id
            """, TEST_INDEX, COMPLETION_MODEL_ID);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "completion"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(lessThanOrEqualTo(3)));

            // Verify all ids are <= 3
            for (List<Object> row : values) {
                int id = (Integer) row.get(0);
                assertThat(id, lessThanOrEqualTo(3));
            }
        }
    }

    // ============================================
    // Error Handling Tests
    // ============================================

    public void testCompletionInvalidPromptType() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | COMPLETION id WITH { "inference_id": "%s" }
            """, TEST_INDEX, COMPLETION_MODEL_ID);

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("prompt must be of type"));
    }

    // ============================================
    // Settings Tests
    // ============================================

    public void testCompletionDisabledBySetting() throws Exception {
        // Disable completion command
        updateClusterSettings(Settings.builder().put(InferenceSettings.COMPLETION_ENABLED_SETTING.getKey(), false));

        try {
            var query = String.format(Locale.ROOT, """
                FROM %s
                | COMPLETION title WITH { "inference_id": "%s" }
                """, TEST_INDEX, COMPLETION_MODEL_ID);

            var error = expectThrows(ParsingException.class, () -> run(query));
            assertThat(error.getMessage(), containsString("COMPLETION command is disabled in settings"));
        } finally {
            // Re-enable for other tests
            updateClusterSettings(Settings.builder().put(InferenceSettings.COMPLETION_ENABLED_SETTING.getKey(), true));
        }
    }
}
