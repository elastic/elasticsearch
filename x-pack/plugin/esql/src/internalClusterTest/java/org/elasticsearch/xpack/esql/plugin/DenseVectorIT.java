/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration tests for the ESQL DENSE_VECTOR command's inference-endpoint resolution and command settings.
 * <p>
 * The built-in default endpoint ({@link org.elasticsearch.xpack.esql.plan.logical.inference.DenseVector#DEFAULT_INFERENCE_ID},
 * E5) requires real ML infrastructure that is unavailable in tests, so the "no WITH" path is exercised through the
 * cluster-level default setting ({@code esql.command.dense_vector.default_inference_id}) pointed at a mock endpoint; this
 * covers the same resolution branch. The pure built-in-default injection is covered by the parser-level tests.
 */
public class DenseVectorIT extends InferenceCommandIntegTestCase {

    private static final String TEST_INDEX = "test_dense_vector";
    private static final String DENSE_VECTOR_MODEL_ID = "test-dense-vector-model";

    @Before
    public void setupIndexAndInferenceModel() throws IOException {
        assumeTrue("DENSE_VECTOR is only enabled on snapshot builds", Build.current().isSnapshot());
        createAndPopulateTestIndex(TEST_INDEX);
        createTestInferenceEndpoint(DENSE_VECTOR_MODEL_ID, TaskType.TEXT_EMBEDDING, "text_embedding_test_service");
    }

    @After
    public void cleanup() {
        deleteTestInferenceEndpoint(DENSE_VECTOR_MODEL_ID, TaskType.TEXT_EMBEDDING);
        cleanupClusterSettings(
            InferenceSettings.DENSE_VECTOR_ENABLED_SETTING,
            InferenceSettings.DENSE_VECTOR_ROW_LIMIT_SETTING,
            InferenceSettings.DENSE_VECTOR_DEFAULT_INFERENCE_ID_SETTING
        );
    }

    public void testDenseVectorWithExplicitInferenceId() {
        var query = String.format(Locale.ROOT, """
            FROM %s
            | DENSE_VECTOR title WITH { "inference_id": "%s" }
            | KEEP id, title, title_dense_vector
            | LIMIT 5
            """, TEST_INDEX, DENSE_VECTOR_MODEL_ID);

        try (var resp = run(query)) {
            assertThat(resp.columns().stream().map(c -> c.name()).toList(), hasItem("title_dense_vector"));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(lessThanOrEqualTo(5)));
        }
    }

    public void testDenseVectorUsesClusterDefaultInferenceId() throws Exception {
        // No WITH: the cluster-level default endpoint is used, exercising the cluster > built-in resolution branch.
        updateClusterSettings(
            Settings.builder().put(InferenceSettings.DENSE_VECTOR_DEFAULT_INFERENCE_ID_SETTING.getKey(), DENSE_VECTOR_MODEL_ID)
        );

        var query = String.format(Locale.ROOT, """
            FROM %s
            | DENSE_VECTOR title
            | KEEP id, title, title_dense_vector
            | LIMIT 5
            """, TEST_INDEX);

        try (var resp = run(query)) {
            assertThat(resp.columns().stream().map(c -> c.name()).toList(), hasItem("title_dense_vector"));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(lessThanOrEqualTo(5)));
        }
    }

    public void testDenseVectorWithOptionOverridesClusterDefault() throws Exception {
        // A WITH id must take precedence over the cluster default; point the cluster default at a non-existent endpoint
        // to prove the WITH id is the one actually used (the query would fail if the cluster default were used).
        updateClusterSettings(
            Settings.builder().put(InferenceSettings.DENSE_VECTOR_DEFAULT_INFERENCE_ID_SETTING.getKey(), "non-existent-endpoint")
        );

        var query = String.format(Locale.ROOT, """
            FROM %s
            | DENSE_VECTOR title WITH { "inference_id": "%s" }
            | KEEP id, title_dense_vector
            | LIMIT 5
            """, TEST_INDEX, DENSE_VECTOR_MODEL_ID);

        try (var resp = run(query)) {
            assertThat(resp.columns().stream().map(c -> c.name()).toList(), hasItem("title_dense_vector"));
        }
    }

    public void testDenseVectorRowLimitSetting() throws Exception {
        int customLimit = between(1, 10);
        updateClusterSettings(Settings.builder().put(InferenceSettings.DENSE_VECTOR_ROW_LIMIT_SETTING.getKey(), customLimit));

        final String largeIndex = "test_dense_vector_custom_limit";
        createAndPopulateTestIndex(largeIndex, customLimit + 10);

        var query = String.format(Locale.ROOT, """
            FROM %s
            | DENSE_VECTOR title WITH { "inference_id": "%s" }
            | KEEP id, title_dense_vector
            """, largeIndex, DENSE_VECTOR_MODEL_ID);

        try (var resp = run(query)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(customLimit));
        }
    }

    public void testDenseVectorChainedClausesWithDistinctEndpoints() throws IOException {
        // Per-field endpoints: each chained DENSE_VECTOR clause uses its own inference endpoint.
        final String secondModelId = "test-dense-vector-model-2";
        createTestInferenceEndpoint(secondModelId, TaskType.TEXT_EMBEDDING, "text_embedding_test_service");
        try {
            var query = String.format(Locale.ROOT, """
                FROM %s
                | DENSE_VECTOR title WITH { "inference_id": "%s" }
                | DENSE_VECTOR content WITH { "inference_id": "%s" }
                | KEEP id, title_dense_vector, content_dense_vector
                | LIMIT 5
                """, TEST_INDEX, DENSE_VECTOR_MODEL_ID, secondModelId);

            try (var resp = run(query)) {
                var columnNames = resp.columns().stream().map(c -> c.name()).toList();
                assertThat(columnNames, hasItem("title_dense_vector"));
                assertThat(columnNames, hasItem("content_dense_vector"));
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(lessThanOrEqualTo(5)));
            }
        } finally {
            deleteTestInferenceEndpoint(secondModelId, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testDenseVectorDisabledBySetting() throws Exception {
        updateClusterSettings(Settings.builder().put(InferenceSettings.DENSE_VECTOR_ENABLED_SETTING.getKey(), false));

        var query = String.format(Locale.ROOT, """
            FROM %s
            | DENSE_VECTOR title WITH { "inference_id": "%s" }
            """, TEST_INDEX, DENSE_VECTOR_MODEL_ID);

        var error = expectThrows(ParsingException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("DENSE_VECTOR command is disabled in settings"));
    }
}
