/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.vector;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.vector.CosineSimilarity;
import org.elasticsearch.xpack.esql.expression.function.vector.DotProduct;
import org.elasticsearch.xpack.esql.expression.function.vector.L1Norm;
import org.elasticsearch.xpack.esql.expression.function.vector.L2Norm;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction.SimilarityEvaluatorFunction;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class VectorSimilarityFunctionsIT extends AbstractEsqlIntegTestCase {

    private List<List<Number>> leftVectors;

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();

        if (EsqlCapabilities.Cap.COSINE_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            params.add(new Object[] { "v_cosine", CosineSimilarity.SIMILARITY_FUNCTION });
        }
        if (EsqlCapabilities.Cap.DOT_PRODUCT_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            params.add(new Object[] { "v_dot_product", DotProduct.SIMILARITY_FUNCTION });
        }
        if (EsqlCapabilities.Cap.L1_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            params.add(new Object[] { "v_l1_norm", L1Norm.SIMILARITY_FUNCTION });
        }
        if (EsqlCapabilities.Cap.L2_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            params.add(new Object[] { "v_l2_norm", L2Norm.SIMILARITY_FUNCTION });
        }
        // if (EsqlCapabilities.Cap.HAMMING_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
        // params.add(new Object[] { "v_hamming", Hamming.SIMILARITY_FUNCTION});
        // }

        return params;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // testDifferentDimensions fails the final driver on the coordinator, leading to cancellation of the entire request.
            // If the exchange sink is opened on a remote node but the compute request hasn't been sent, we cannot close the exchange
            // sink (for now).Here, we reduce the inactive sinks interval to ensure those inactive sinks are removed quickly.
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), InternalExchangePlugin.class);
    }

    private final String functionName;
    private final SimilarityEvaluatorFunction similarityFunction;
    private int numDims;

    public VectorSimilarityFunctionsIT(
        @Name("functionName") String functionName,
        @Name("similarityFunction") SimilarityEvaluatorFunction similarityFunction
    ) {
        this.functionName = functionName;
        this.similarityFunction = similarityFunction;
    }

    @SuppressWarnings("unchecked")
    public void testSimilarityBetweenVectors() {
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL similarity = %s(left_vector, right_vector)
                | KEEP left_vector, right_vector, similarity
            """, functionName);

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            valuesList.forEach(values -> {
                float[] left = readVector((List<Number>) values.get(0));
                float[] right = readVector((List<Number>) values.get(1));
                Double similarity = (Double) values.get(2);
                if (left == null || right == null) {
                    assertNull(similarity);
                } else {
                    assertNotNull(similarity);
                    double expectedSimilarity = similarityFunction.calculateSimilarity(left, right);
                    assertEquals(expectedSimilarity, similarity, 0.0001);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testSimilarityBetweenConstantVectorAndField() {
        var randomVector = randomVectorArray();
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL similarity = %s(left_vector, %s)
                | KEEP left_vector, similarity
            """, functionName, Arrays.toString(randomVector));

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            valuesList.forEach(values -> {
                float[] left = readVector((List<Number>) values.get(0));
                Double similarity = (Double) values.get(1);
                if (left == null || randomVector == null) {
                    assertNull(similarity);
                } else {
                    assertNotNull(similarity);
                    double expectedSimilarity = similarityFunction.calculateSimilarity(left, randomVector);
                    assertEquals(expectedSimilarity, similarity, 0.0001);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testTopNSimilarityBetweenConstantVectorAndField() {
        var randomVector = randomVectorArray();
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL similarity = %s(left_vector, %s)
                | SORT similarity DESC NULLS LAST
                | LIMIT 10
                | KEEP similarity
            """, functionName, Arrays.toString(randomVector));

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            // for (List<Number> leftVector : leftVectors) {
            // System.out.println("Original stored: " + leftVector + "converted: " +
            // calculateSimilarityFloats(similarityFunction, randomVector, leftVector));
            // }

            List<Double> orderedSimilarity = leftVectors.stream()
                .map(v -> calculateSimilarityFloats(similarityFunction, randomVector, v))
                .sorted((d1, d2) -> {
                    if (d1 == null && d2 == null) {
                        return 0;
                    }
                    if (d1 == null) {
                        return 1;
                    }
                    if (d2 == null) {
                        return -1;
                    }
                    return -Double.compare(d1, d2);
                })
                .limit(10)
                .toList();
            for (int i = 0; i < valuesList.size(); i++) {
                Double similarity = (Double) valuesList.get(i).get(0);
                Double expectedSimilarity = orderedSimilarity.get(i);
                if (similarity == null) {
                    assertNull("Expected " + orderedSimilarity + " but got " + valuesList, expectedSimilarity);
                } else {
                    assertEquals("Expected " + orderedSimilarity + " but got " + valuesList, expectedSimilarity, similarity, 0.0001);
                }
            }
        }
    }

    private Double calculateSimilarityFloats(SimilarityEvaluatorFunction similarityFunction, float[] randomVector, List<Number> vector) {
        if (randomVector == null || vector == null) {
            return null;
        }
        return similarityFunction.calculateSimilarity(randomVector, readVector(vector));
    }

    public void testDifferentDimensions() {
        var randomVector = randomVectorArray(randomValueOtherThan(numDims, () -> randomIntBetween(32, 64) * 2));
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL similarity = %s(left_vector, %s)
                | KEEP left_vector, similarity
            """, functionName, Arrays.toString(randomVector));

        EsqlClientException iae = expectThrows(EsqlClientException.class, () -> { run(query); });
        assertTrue(iae.getMessage().contains("Vectors must have the same dimensions"));
    }

    @SuppressWarnings("unchecked")
    public void testSimilarityBetweenConstantVectors() {
        var vectorLeft = randomVectorArray();
        var vectorRight = randomVectorArray();
        var query = String.format(Locale.ROOT, """
                ROW a = 1
                | EVAL similarity = %s(%s, %s)
                | KEEP similarity
            """, functionName, Arrays.toString(vectorLeft), Arrays.toString(vectorRight));

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(1, valuesList.size());

            Double similarity = (Double) valuesList.get(0).get(0);
            if (vectorLeft == null || vectorRight == null) {
                assertNull(similarity);
            } else {
                assertNotNull(similarity);
                double expectedSimilarity = similarityFunction.calculateSimilarity(vectorLeft, vectorRight);
                assertEquals(expectedSimilarity, similarity, 0.0001);
            }
        }
    }

    private static float[] readVector(List<Number> leftVector) {
        if (leftVector == null) {
            return null;
        }
        float[] leftScratch = new float[leftVector.size()];
        for (int i = 0; i < leftVector.size(); i++) {
            leftScratch[i] = leftVector.get(i).floatValue();
        }
        return leftScratch;
    }

    @Before
    public void setup() throws IOException {
        createIndexWithDenseVector("test");

        numDims = randomIntBetween(32, 64) * 2; // min 64, even number
        int numDocs = randomIntBetween(10, 100);
        this.leftVectors = new ArrayList<>();
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            List<Number> leftVector = randomVector();
            List<Number> rightVector = randomVector();
            docs[i] = prepareIndex("test").setId("" + i)
                .setSource("id", String.valueOf(i), "left_vector", leftVector, "right_vector", rightVector);
            leftVectors.add(leftVector);
        }

        indexRandom(true, docs);
    }

    private List<Number> randomVector() {
        assert numDims != 0 : "numDims must be set before calling randomVector()";
        if (rarely()) {
            return null;
        }
        List<Number> vector = new ArrayList<>(numDims);
        for (int j = 0; j < numDims; j++) {
            vector.add(randomFloat());
        }
        return vector;
    }

    private float[] randomVectorArray() {
        assert numDims != 0 : "numDims must be set before calling randomVectorArray()";
        return rarely() ? null : randomVectorArray(numDims);
    }

    private static float[] randomVectorArray(int dimensions) {
        float[] vector = new float[dimensions];
        for (int j = 0; j < dimensions; j++) {
            vector[j] = randomFloat();
        }
        return vector;
    }

    private void createIndexWithDenseVector(String indexName) throws IOException {
        var client = client().admin().indices();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject();
        createDenseVectorField(mapping, "left_vector");
        createDenseVectorField(mapping, "right_vector");
        mapping.endObject().endObject();
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5));

        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping(mapping)
            .setSettings(settingsBuilder.build());
        assertAcked(CreateRequest);
    }

    private void createDenseVectorField(XContentBuilder mapping, String fieldName) throws IOException {
        mapping.startObject(fieldName).field("type", "dense_vector").field("similarity", "l2_norm");
        mapping.endObject();
    }
}
