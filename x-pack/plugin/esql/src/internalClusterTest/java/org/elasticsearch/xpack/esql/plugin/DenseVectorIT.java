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
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension.TestInferenceService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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

    /** Dimension count pinned on the second endpoint of the chained-clauses test, to tell its vectors apart from the first's. */
    private static final int SECOND_ENDPOINT_DIMENSIONS = 64;

    /** A title the mock inference service refuses to embed, used to trigger a single-row inference failure. */
    private static final String FAILING_TITLE = TestInferenceService.FAILING_INPUT_PREFIX + " title";

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
            assertVectorsPresent(getValuesList(resp), 2);
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
            assertVectorsPresent(getValuesList(resp), 2);
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
            assertVectorsPresent(getValuesList(resp), 1);
        }
    }

    public void testDenseVectorUnresolvableClusterDefaultFailsTheQuery() throws Exception {
        // The control for testDenseVectorWithOptionOverridesClusterDefault: with the cluster default pointing at an endpoint
        // that does not exist, a query that omits WITH fails at analysis.
        updateClusterSettings(
            Settings.builder().put(InferenceSettings.DENSE_VECTOR_DEFAULT_INFERENCE_ID_SETTING.getKey(), "non-existent-endpoint")
        );

        var query = String.format(Locale.ROOT, """
            FROM %s
            | DENSE_VECTOR title
            | KEEP id, title_dense_vector
            | LIMIT 5
            """, TEST_INDEX);

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("non-existent-endpoint"));
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
        // Per-field endpoints: each chained DENSE_VECTOR clause uses its own inference endpoint. The second endpoint produces
        // narrower vectors than the first, so which endpoint served which column is visible in the output.
        final String secondModelId = "test-dense-vector-model-2";
        createTestInferenceEndpoint(secondModelId, TaskType.TEXT_EMBEDDING, "text_embedding_test_service", SECOND_ENDPOINT_DIMENSIONS);
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
                assertThat(values, hasSize(5));
                for (List<Object> row : values) {
                    List<?> titleVector = (List<?>) row.get(1);
                    List<?> contentVector = (List<?>) row.get(2);
                    assertThat(titleVector, notNullValue());
                    assertThat(contentVector, hasSize(SECOND_ENDPOINT_DIMENSIONS));
                    assertThat(titleVector.size(), not(equalTo(SECOND_ENDPOINT_DIMENSIONS)));
                }
            }
        } finally {
            deleteTestInferenceEndpoint(secondModelId, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testDenseVectorToleratesPerRowInferenceFailure() {
        // One row's text makes its inference request fail. The query must still succeed, with that row's vector null and
        // every other row keeping a real vector.
        indexDocumentWithTitle(TEST_INDEX, 99, FAILING_TITLE);

        try (var resp = run(failingRowQuery())) {
            List<List<Object>> values = getValuesList(resp);
            // The six documents created by the base fixture, plus the failing one added above.
            assertThat(values, hasSize(7));

            int failedRows = 0;
            for (List<Object> row : values) {
                if (FAILING_TITLE.equals(row.get(0))) {
                    assertThat(row.get(1), nullValue());
                    failedRows++;
                } else {
                    assertThat(row.get(1), notNullValue());
                }
            }
            assertThat(failedRows, equalTo(1));
        }
    }

    public void testDenseVectorFailureWarningCarriesCommandSource() throws Exception {
        // A tolerated failure must tell the user which command failed and where, so the warning carries the DENSE_VECTOR
        // command's own text and a real line/column.
        indexDocumentWithTitle(TEST_INDEX, 99, FAILING_TITLE);

        List<String> warnings = new CopyOnWriteArrayList<>();
        runCollectingWarnings(failingRowQuery(), warnings);

        String failureWarning = warnings.stream()
            .filter(w -> w.contains("treating result as null"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected a tolerated-failure warning, got " + warnings));
        assertThat(failureWarning, containsString("DENSE_VECTOR title"));
        assertThat(failureWarning, matchesRegex(".*Line [1-9][0-9]*:[1-9][0-9]*: evaluation of .*"));
    }

    /** A query over an index containing {@link #FAILING_TITLE}, so exactly one row's inference request fails. */
    private String failingRowQuery() {
        return String.format(Locale.ROOT, """
            FROM %s
            | DENSE_VECTOR title WITH { "inference_id": "%s" }
            | KEEP title, title_dense_vector
            """, TEST_INDEX, DENSE_VECTOR_MODEL_ID);
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

    /**
     * Asserts the query returned a full page of {@code LIMIT 5} rows, each carrying a real vector in the given column. The
     * per-row null check matters because a tolerated inference failure still yields a correctly shaped response, just one
     * full of nulls.
     */
    private static void assertVectorsPresent(List<List<Object>> values, int vectorColumn) {
        assertThat(values, hasSize(5));
        for (List<Object> row : values) {
            assertThat(row.get(vectorColumn), notNullValue());
        }
    }
}
