/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration tests for KNN function operating on runtime expressions derived from plain float fields.
 *
 * <p>Unlike {@link KnnFunctionIT}, no {@code dense_vector} mapping is used. The test index stores
 * each vector as a multi-valued {@code float} field. Queries convert that field to a
 * {@code dense_vector} at runtime with {@code to_dense_vector(floats)}, which forces the KNN
 * function into runtime mode enabled via {@code runtime_knn_search} query pragma (snapshot builds only).
 *
 * <p>The test index contains six 3-dim unit vectors ({@code VECTORS}) whose cosine similarity to the
 * query vector {@code [1.0, 0.0, 0.0]} is deterministic and easy to reason about:
 * <ul>
 *   <li>id=0  [1.0000, 0.0000, 0.0000] – similarity 1.0000 (perfect match)</li>
 *   <li>id=1  [0.0000, 1.0000, 0.0000] – similarity 0.0000</li>
 *   <li>id=2  [0.0000, 0.0000, 1.0000] – similarity 0.0000</li>
 *   <li>id=3  [0.7071, 0.7071, 0.0000] – similarity ≈ 0.7071</li>
 *   <li>id=4  [0.8944, 0.4472, 0.0000] – similarity ≈ 0.8944</li>
 *   <li>id=5  [0.5000, 0.5000, 0.7071] – similarity 0.5000</li>
 * </ul>
 */
public class KnnRuntimeIT extends AbstractEsqlIntegTestCase {

    // All vectors are unit vectors so cosine similarity == dot product with the query.
    private static final float[][] VECTORS = {
        { 1.0f, 0.0f, 0.0f },       // id=0 cosine([1,0,0]) = 1.0
        { 0.0f, 1.0f, 0.0f },       // id=1 cosine([1,0,0]) = 0.0
        { 0.0f, 0.0f, 1.0f },       // id=2 cosine([1,0,0]) = 0.0
        { 0.7071f, 0.7071f, 0.0f }, // id=3 cosine([1,0,0]) ≈ 0.7071
        { 0.8944f, 0.4472f, 0.0f }, // id=4 cosine([1,0,0]) ≈ 0.8944
        { 0.5f, 0.5f, 0.7071f },    // id=5 cosine([1,0,0]) = 0.5
    };

    @Override
    protected QueryPragmas getPragmas() {
        // The runtime_knn_search pragma requires a snapshot build; skip in release builds.
        assumeTrue("Runtime KNN search requires a snapshot build", canUseQueryPragmas());
        return new QueryPragmas(Settings.builder().put(QueryPragmas.RUNTIME_KNN_SEARCH.getKey(), true).build());
    }

    @Before
    public void setup() throws IOException {
        // The vector field is a plain multi-valued float field, not a dense_vector.
        // Tests convert it to dense_vector at query time with to_dense_vector(), exercising the runtime KNN path.
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject()
            .startObject("floats")
            .field("type", "float")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping));

        IndexRequestBuilder[] docs = new IndexRequestBuilder[VECTORS.length];
        for (int i = 0; i < VECTORS.length; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("id", i, "floats", floatListOf(VECTORS[i]));
        }
        indexRandom(true, docs);
    }

    /**
     * Tests KNN on a dense_vector derived from inline ROW data.
     *
     * <p>{@code to_dense_vector} wrapping a list literal produces an expression that is not a
     * {@code FieldAttribute}, which forces the KNN function into runtime mode.
     */
    public void testKnnOnRowData() {
        // Single row: [1, 0, 0] should match the query [1, 0, 0] with cosine similarity 1.0.
        try (var resp = run("""
            ROW v = [1.0, 0.0, 0.0]
            | EVAL dv = to_dense_vector(v)
            | WHERE knn(dv, [1.0, 0.0, 0.0])
            | KEEP dv
            | LIMIT 5
            """)) {
            List<List<Object>> respRows = EsqlTestUtils.getValuesList(resp);
            assertEquals(1, respRows.size());
        }
    }

    /**
     * Tests KNN on a dense_vector derived from a plain multi-valued {@code float} field.
     *
     * <p>{@code to_dense_vector(floats)} converts the stored floats into a dense_vector expression
     * at query time; because this expression is not a {@code FieldAttribute}, the KNN function runs
     * in runtime mode (row-by-row cosine similarity) rather than being pushed down to Lucene.
     */
    public void testKnnOnDerivedFromIndexed() {
        try (var resp = run("""
            FROM test
            | EVAL dv = to_dense_vector(floats)
            | WHERE knn(dv, [1.0, 0.0, 0.0])
            | KEEP id
            | LIMIT 10
            """)) {
            List<List<Object>> respRows = EsqlTestUtils.getValuesList(resp);
            // All six documents should be returned (no similarity threshold).
            assertEquals(VECTORS.length, respRows.size());
        }
    }

    /**
     * Tests that results are sorted by score in descending order and that LIMIT caps the result set.
     *
     * <p>Doc id=0 has cosine similarity 1.0 to {@code [1, 0, 0]} and must appear first.
     * With LIMIT 3, only the three closest vectors are returned.
     */
    public void testKnnSortingAndLimit() {
        try (var resp = run("""
            FROM test METADATA _score
            | EVAL dv = to_dense_vector(floats)
            | WHERE knn(dv, [1.0, 0.0, 0.0])
            | SORT _score DESC, id ASC
            | KEEP id, dv, _score
            | LIMIT 10
            """)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));

            List<List<Object>> respRows = EsqlTestUtils.getValuesList(resp);
            assertEquals(3, respRows.size());

            // Doc 0 ([1,0,0]) has cosine 1.0 and must be the top result.
            assertEquals(0, respRows.get(0).get(0));
            // Scores must be non-increasing.
            double previousScore = Double.MAX_VALUE;
            for (List<Object> row : respRows) {
                double score = (Double) row.get(1);
                assertThat(score, lessThanOrEqualTo(previousScore));
                previousScore = score;
            }
        }
    }

    /**
     * Tests that the {@code similarity} threshold option filters out vectors whose cosine
     * similarity to the query falls below the given value.
     *
     * <p>With threshold 0.8, only doc 0 (similarity 1.0) and doc 4 (similarity ≈ 0.8944)
     * should be returned.
     *
     * <p>Note: options are not yet supported for runtime KNN. Remove the {@code expectThrows}
     * wrapper and keep the inner assertion once the verifier restriction is lifted.
     */
    public void testKnnSimilarityThreshold() {
        // TODO: flip to a happy-path test once options are supported for runtime KNN.
        var ex = expectThrows(Exception.class, () -> {
            try (var resp = run("""
                FROM test
                | EVAL dv = to_dense_vector(floats)
                | WHERE knn(dv, [1.0, 0.0, 0.0], {"similarity": 0.8})
                | SORT id ASC
                | KEEP id
                | LIMIT 10
                """)) {
                // Intended behaviour once supported: only ids 0 and 4 match (cosine >= 0.8).
                List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
                assertEquals(2, rows.size());
                assertEquals(0, rows.get(0).get(0));
                assertEquals(4, rows.get(1).get(0));
            }
        });
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("not supported"));
    }

    /**
     * Tests that the {@code boost} option scales the KNN scores by the given multiplier.
     *
     * <p>Note: options are not yet supported for runtime KNN. Remove the {@code expectThrows}
     * wrapper and keep the inner assertion once the verifier restriction is lifted.
     */
    public void testKnnBoost() {
        // TODO: flip to a happy-path test once options are supported for runtime KNN.
        var ex = expectThrows(Exception.class, () -> {
            // Collect base scores (no boost).
            List<List<Object>> baseRows;
            try (var resp = run("""
                FROM test METADATA _score
                | EVAL dv = to_dense_vector(floats)
                | WHERE knn(dv, [1.0, 0.0, 0.0])
                | SORT id ASC
                | KEEP id, _score
                | LIMIT 10
                """)) {
                baseRows = EsqlTestUtils.getValuesList(resp);
            }

            // Collect boosted scores (boost = 2.0).
            List<List<Object>> boostedRows;
            try (var resp = run("""
                FROM test METADATA _score
                | EVAL dv = to_dense_vector(floats)
                | WHERE knn(dv, [1.0, 0.0, 0.0], {"boost": 2.0})
                | SORT id ASC
                | KEEP id, _score
                | LIMIT 10
                """)) {
                boostedRows = EsqlTestUtils.getValuesList(resp);
            }

            assertEquals(baseRows.size(), boostedRows.size());
            for (int i = 0; i < baseRows.size(); i++) {
                double base = (Double) baseRows.get(i).get(1);
                double boosted = (Double) boostedRows.get(i).get(1);
                assertThat("boost=2.0 should double the score", boosted, closeTo(base * 2.0, 1e-5));
            }
        });
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("not supported"));
    }

    /**
     * Tests that KNN returns a non-null, positive _score for every matched row.
     */
    public void testKnnScoreIsPositive() {
        try (var resp = run("""
            FROM test METADATA _score
            | EVAL dv = to_dense_vector(floats)
            | WHERE knn(dv, [1.0, 0.0, 0.0])
            | KEEP id, _score
            | LIMIT 10
            """)) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            assertThat(rows.size(), greaterThan(0));
            for (List<Object> row : rows) {
                double score = (Double) row.get(1);
                assertNotNull("score must not be null for matched row", score);
                assertThat("score must be positive", score, greaterThan(0.0));
            }
        }
    }

    private static List<Float> floatListOf(float[] arr) {
        List<Float> list = new ArrayList<>(arr.length);
        for (float f : arr) {
            list.add(f);
        }
        return list;
    }
}
