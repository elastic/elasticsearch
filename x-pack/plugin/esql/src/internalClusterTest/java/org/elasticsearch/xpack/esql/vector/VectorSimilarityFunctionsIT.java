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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.vector.CosineSimilarity;
import org.elasticsearch.xpack.esql.expression.function.vector.DotProduct;
import org.elasticsearch.xpack.esql.expression.function.vector.Hamming;
import org.elasticsearch.xpack.esql.expression.function.vector.L1Norm;
import org.elasticsearch.xpack.esql.expression.function.vector.L2Norm;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class VectorSimilarityFunctionsIT extends AbstractEsqlIntegTestCase {

    private List<List<Number>> leftVectors;

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();

        for (ElementType elementType : Set.of(ElementType.FLOAT, ElementType.BYTE, ElementType.BIT)) {
            if (EsqlCapabilities.Cap.COSINE_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
                params.add(new Object[] { "v_cosine", CosineSimilarity.SIMILARITY_FUNCTION, elementType });
            }
            if (EsqlCapabilities.Cap.DOT_PRODUCT_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
                params.add(new Object[] { "v_dot_product", DotProduct.SIMILARITY_FUNCTION, elementType });
            }
            if (EsqlCapabilities.Cap.L1_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
                params.add(new Object[] { "v_l1_norm", L1Norm.SIMILARITY_FUNCTION, elementType });
            }
            if (EsqlCapabilities.Cap.L2_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
                params.add(new Object[] { "v_l2_norm", L2Norm.SIMILARITY_FUNCTION, elementType });
            }
            if (EsqlCapabilities.Cap.HAMMING_VECTOR_SIMILARITY_FUNCTION.isEnabled() && elementType != ElementType.FLOAT) {
                params.add(new Object[] { "v_hamming", Hamming.EVALUATOR_SIMILARITY_FUNCTION, elementType });
            }
        }

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
    private final DenseVectorFieldMapper.SimilarityFunction similarityFunction;
    private final ElementType elementType;
    private int numDims;

    public VectorSimilarityFunctionsIT(
        @Name("functionName") String functionName,
        @Name("similarityFunction") DenseVectorFieldMapper.SimilarityFunction similarityFunction,
        @Name("elementType") ElementType elementType
    ) {
        this.functionName = functionName;
        this.similarityFunction = similarityFunction;
        this.elementType = elementType;
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
        var randomVector = randomVector();
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL similarity = %s(left_vector, %s)
                | KEEP left_vector, similarity
            """, functionName, randomVector);

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            valuesList.forEach(values -> {
                List<Number> leftAsList = ((List<Number>) values.get(0));
                Double similarity = (Double) values.get(1);
                if (leftAsList == null || randomVector == null) {
                    assertNull(similarity);
                } else {
                    assertNotNull(similarity);
                    double expectedSimilarity = calculateSimilarity(similarityFunction, randomVector, leftAsList);
                    assertEquals(expectedSimilarity, similarity, 0.0001);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testSimilarityWithOneDimVector() {
        var randomVector = randomVector(elementType == ElementType.BIT ? Byte.SIZE : 1);
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL similarity = %s(one_dim_vector, %s)
                | KEEP one_dim_vector, similarity
            """, functionName, randomVector);
        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            valuesList.forEach(values -> {
                List<Number> vecAsList = values.get(0) == null ? null : List.of((Number) values.get(0));
                Double similarity = (Double) values.get(1);
                if (vecAsList == null || randomVector == null) {
                    assertNull(similarity);
                } else {
                    assertNotNull(similarity);
                    double expectedSimilarity = calculateSimilarity(similarityFunction, randomVector, vecAsList);
                    assertEquals(expectedSimilarity, similarity, 0.0001);
                }
            });
        }
    }

    private static float[] asFloatArray(List<Number> randomVector) {
        float[] result = new float[randomVector.size()];
        for (int i = 0; i < randomVector.size(); i++) {
            result[i] = randomVector.get(i).floatValue();
        }
        return result;
    }

    private static byte[] asByteArray(List<Number> randomVector) {
        byte[] result = new byte[randomVector.size()];
        for (int i = 0; i < randomVector.size(); i++) {
            result[i] = randomVector.get(i).byteValue();
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public void testTopNSimilarityBetweenConstantVectorAndField() {
        var randomVector = randomVector();
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL similarity = %s(left_vector, %s)
                | SORT similarity DESC NULLS LAST
                | LIMIT 10
                | KEEP similarity
            """, functionName, randomVector);

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            List<Double> orderedSimilarity = leftVectors.stream()
                .map(v -> calculateSimilarity(similarityFunction, v, randomVector))
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

    private Double calculateSimilarity(
        DenseVectorFieldMapper.SimilarityFunction similarityFunction,
        List<Number> randomVector,
        List<Number> vector
    ) {
        if (randomVector == null || vector == null) {
            return null;
        }
        switch (elementType) {
            case BYTE, BIT -> {
                return (double) similarityFunction.calculateSimilarity(asByteArray(randomVector), asByteArray(vector));
            }
            case FLOAT -> {
                return (double) similarityFunction.calculateSimilarity(asFloatArray(randomVector), asFloatArray(vector));
            }
            default -> throw new IllegalArgumentException("Unexpected element type: " + elementType);
        }
    }

    public void testDifferentDimensions() {
        var randomVector = randomVector(
            randomValueOtherThan(numDims, () -> randomIntBetween(32, 64) * (elementType == ElementType.BIT ? 8 : 2)),
            false
        );
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL similarity = %s(left_vector, %s)
                | KEEP left_vector, similarity
            """, functionName, randomVector);

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> { run(query); });
        assertThat(iae.getMessage(), containsString("vector dimensions differ"));
    }

    @SuppressWarnings("unchecked")
    public void testSimilarityBetweenConstantVectors() {
        var vectorLeft = randomVector();
        var vectorRight = randomVector();
        var query = String.format(Locale.ROOT, """
                ROW a = 1
                | EVAL similarity = %s(%s, %s)
                | KEEP similarity
            """, functionName, vectorLeft, vectorRight);

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(1, valuesList.size());

            Double similarity = (Double) valuesList.get(0).get(0);
            if (vectorLeft == null || vectorRight == null) {
                assertNull(similarity);
            } else {
                assertNotNull(similarity);
                final double expectedSimilarity = calculateSimilarity(similarityFunction, vectorLeft, vectorRight);
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
        numDims = randomIntBetween(10, 20) * (elementType == ElementType.BIT ? 8 : 2);
        createIndexWithDenseVector("test");

        int numDocs = randomIntBetween(10, 100);
        this.leftVectors = new ArrayList<>();
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            List<Number> leftVector = randomVector();
            List<Number> rightVector = randomVector();
            List<Number> oneDimVector = randomVector(elementType == ElementType.BIT ? Byte.SIZE : 1);
            docs[i] = prepareIndex("test").setId("" + i)
                .setSource("id", String.valueOf(i), "left_vector", leftVector, "right_vector", rightVector, "one_dim_vector", oneDimVector);
            leftVectors.add(leftVector);
        }

        indexRandom(true, docs);
    }

    private List<Number> randomVector() {
        return randomVector(numDims);
    }

    private List<Number> randomVector(int numDims) {
        return randomVector(numDims, true);
    }

    private List<Number> randomVector(int numDims, boolean allowNull) {
        assert numDims != 0 : "numDims must be set before calling randomVector()";
        if (allowNull && rarely()) {
            return null;
        }
        int dimensions = numDims;
        if (elementType == ElementType.BIT) {
            assert dimensions % 8 == 0 : "dimensions must be multiple of 8 for BIT element type but was " + dimensions;
            dimensions = dimensions / 8;
        }
        List<Number> vector = new ArrayList<>(dimensions);
        for (int j = 0; j < dimensions; j++) {
            switch (elementType) {
                case FLOAT -> {
                    if (dimensions == 1) {
                        vector.add(randomValueOtherThan(0f, () -> randomFloat()));
                    } else {
                        vector.add(randomFloat());
                    }
                }
                case BYTE, BIT -> {
                    if (dimensions == 1) {
                        vector.add(randomValueOtherThan((byte) 0, () -> (byte) randomIntBetween(-128, 127)));
                    } else {
                        vector.add((byte) randomIntBetween(-128, 127));
                    }
                }
                default -> throw new IllegalArgumentException("Unexpected element type: " + elementType);
            }
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
        createDenseVectorField(mapping, "left_vector", elementType, numDims);
        createDenseVectorField(mapping, "right_vector", elementType, numDims);
        createDenseVectorField(mapping, "one_dim_vector", elementType, elementType == ElementType.BIT ? Byte.SIZE : 1);
        mapping.endObject().endObject();
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5));

        var CreateRequest = client.prepareCreate(indexName).setMapping(mapping).setSettings(settingsBuilder.build());
        assertAcked(CreateRequest);
    }

    private static void createDenseVectorField(XContentBuilder mapping, String fieldName, ElementType elementType, int dims)
        throws IOException {
        mapping.startObject(fieldName)
            .field("type", "dense_vector")
            .field("dims", dims)
            .field("similarity", "l2_norm")
            .field("element_type", elementType.toString().toLowerCase(Locale.ROOT))
            .startObject("index_options")
            .field("type", "hnsw")
            .endObject();
        mapping.endObject();
    }
}
