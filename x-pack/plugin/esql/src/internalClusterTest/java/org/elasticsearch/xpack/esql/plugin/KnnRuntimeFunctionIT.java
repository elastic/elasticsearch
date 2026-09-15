/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Checks that runtime KNN uses the selected vector similarity for filtering, scoring, and ranking. The expected
 * results are calculated with the ES|QL vector functions so that the test does not duplicate KNN's implementation.
 */
public class KnnRuntimeFunctionIT extends AbstractEsqlIntegTestCase {

    private static final String QUERY_VECTOR = "[1.0, 0.0, 0.0]";

    private final String metric;
    private final String similarityFunction;
    private final double threshold;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(
            new Object[] { "cosine", "v_cosine", 0.5 },
            new Object[] { "dot_product", "v_dot_product", 0.0 },
            new Object[] { "l2_norm", "v_l2_norm", 5.0 },
            new Object[] { "max_inner_product", "v_dot_product", 4.0 }
        );
    }

    public KnnRuntimeFunctionIT(
        @Name("metric") String metric,
        @Name("similarityFunction") String similarityFunction,
        @Name("threshold") double threshold
    ) {
        this.metric = metric;
        this.similarityFunction = similarityFunction;
        this.threshold = threshold;
    }

    public void testScoresMatchVectorFunction() {
        String query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | EVAL vector = to_dense_vector(vector_hex)
            | EVAL raw_similarity = %s(vector, %s)
            | EVAL expected_score = %s
            | WHERE knn(vector, %s, {"vector_similarity": "%s"})
            | SORT id ASC
            | KEEP id, _score, expected_score
            """, similarityFunction, QUERY_VECTOR, expectedScoreExpression(), QUERY_VECTOR, metric);

        try (var response = run(query)) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(response);
            assertEquals(testVectors().size(), rows.size());
            for (List<Object> row : rows) {
                assertEquals("metric [" + metric + "] id [" + row.get(0) + "]", (Double) row.get(2), (Double) row.get(1), 1e-6);
            }
        }
    }

    public void testFilteringMatchesVectorFunction() {
        String knnQuery = String.format(Locale.ROOT, """
            FROM test
            | EVAL vector = to_dense_vector(vector_hex)
            | WHERE knn(vector, %s, {"vector_similarity": "%s", "similarity": %s})
            | SORT id ASC
            | KEEP id
            """, QUERY_VECTOR, metric, threshold);
        String baselineQuery = String.format(Locale.ROOT, """
            FROM test
            | EVAL vector = to_dense_vector(vector_hex)
            | EVAL raw_similarity = %s(vector, %s)
            | WHERE raw_similarity %s %s
            | SORT id ASC
            | KEEP id
            """, similarityFunction, QUERY_VECTOR, thresholdComparison(), threshold);

        try (var knnResponse = run(knnQuery); var baselineResponse = run(baselineQuery)) {
            assertEquals(
                "filtered ids for metric [" + metric + "]",
                EsqlTestUtils.getValuesList(baselineResponse),
                EsqlTestUtils.getValuesList(knnResponse)
            );
        }
    }

    public void testRankingMatchesVectorFunction() {
        String knnQuery = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | EVAL vector = to_dense_vector(vector_hex)
            | WHERE knn(vector, %s, {"vector_similarity": "%s"})
            | SORT _score DESC, id ASC
            | KEEP id
            """, QUERY_VECTOR, metric);
        String baselineQuery = String.format(Locale.ROOT, """
            FROM test
            | EVAL vector = to_dense_vector(vector_hex)
            | EVAL raw_similarity = %s(vector, %s)
            | EVAL expected_score = %s
            | SORT expected_score DESC, id ASC
            | KEEP id
            """, similarityFunction, QUERY_VECTOR, expectedScoreExpression());

        try (var knnResponse = run(knnQuery); var baselineResponse = run(baselineQuery)) {
            assertEquals(
                "ranked ids for metric [" + metric + "]",
                EsqlTestUtils.getValuesList(baselineResponse),
                EsqlTestUtils.getValuesList(knnResponse)
            );
        }
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put(QueryPragmas.KNN_RUNTIME_FIELD.getKey(), true).build());
    }

    @Before
    public void setupIndex() throws IOException {
        assumeTrue("runtime KNN is only available in snapshot builds", Build.current().isSnapshot());

        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject()
            .startObject("vector_hex")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setMapping(mapping)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                )
        );

        List<TestVector> vectors = testVectors();
        IndexRequestBuilder[] requests = new IndexRequestBuilder[vectors.size()];
        for (int i = 0; i < vectors.size(); i++) {
            TestVector vector = vectors.get(i);
            requests[i] = prepareIndex("test").setId(Integer.toString(vector.id()))
                .setSource("id", vector.id(), "vector_hex", vector.hex());
        }
        indexRandom(true, requests);
    }

    private List<TestVector> testVectors() {
        if (metric.equals("dot_product")) {
            return List.of(
                new TestVector(0, "010000"), // [1, 0, 0]
                new TestVector(1, "000100"), // [0, 1, 0]
                new TestVector(2, "ff0000"), // [-1, 0, 0]
                new TestVector(3, "000001")  // [0, 0, 1]
            );
        }
        return List.of(
            new TestVector(0, "0a0a00"), // [10, 10, 0]
            new TestVector(1, "050000"), // [5, 0, 0]
            new TestVector(2, "010a00"), // [1, 10, 0]
            new TestVector(3, "fe0000")  // [-2, 0, 0]
        );
    }

    private String expectedScoreExpression() {
        return switch (metric) {
            case "cosine", "dot_product" -> "(raw_similarity + 1.0) / 2.0";
            case "l2_norm" -> "1.0 / (1.0 + raw_similarity * raw_similarity)";
            case "max_inner_product" -> "CASE(raw_similarity < 0, 1.0 / (1.0 - raw_similarity), raw_similarity + 1.0)";
            default -> throw new IllegalArgumentException("unexpected vector similarity metric [" + metric + "]");
        };
    }

    private String thresholdComparison() {
        return metric.equals("l2_norm") ? "<=" : ">=";
    }

    private record TestVector(int id, String hex) {}
}
