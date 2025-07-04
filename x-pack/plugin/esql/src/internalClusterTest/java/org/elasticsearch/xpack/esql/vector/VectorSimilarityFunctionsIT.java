/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.vector;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class VectorSimilarityFunctionsIT extends AbstractEsqlIntegTestCase {

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();

        params.add(new Object[] { "v_cosine_similarity", VectorSimilarityFunction.COSINE });

        return params;
    }

    private final String functionName;
    private final VectorSimilarityFunction similarityFunction;
    private int numDims;

    public VectorSimilarityFunctionsIT(
        @Name("functionName") String functionName,
        @Name("similarityFunction") VectorSimilarityFunction similarityFunction
    ) {
        this.functionName = functionName;
        this.similarityFunction = similarityFunction;
    }

    @SuppressWarnings("unchecked")
    public void testCosineSimilarityBetweenVectors() {
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL similarity = %s(left_vector, right_vector)
                | KEEP left_vector, right_vector, similarity
            """, functionName);

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            valuesList.forEach(values -> {
                float[] left = readVector((List<Float>) values.get(0));
                float[] right = readVector((List<Float>) values.get(1));
                Double similarity = (Double) values.get(2);

                assertNotNull(similarity);
                float expectedSimilarity = similarityFunction.compare(left, right);
                assertEquals(expectedSimilarity, similarity, 0.0001);
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testCosineSimilarityBetweenConstantVectorAndField() {
        var randomVector = randomVectorArray();
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL similarity = %s(left_vector, %s)
                | KEEP left_vector, similarity
            """, functionName, Arrays.toString(randomVector));

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            valuesList.forEach(values -> {
                float[] left = readVector((List<Float>) values.get(0));
                Double similarity = (Double) values.get(1);

                assertNotNull(similarity);
                float expectedSimilarity = similarityFunction.compare(left, randomVector);
                assertEquals(expectedSimilarity, similarity, 0.0001);
            });
        }
    }

    private static float[] readVector(List<Float> leftVector) {
        float[] leftScratch = new float[leftVector.size()];
        for (int i = 0; i < leftVector.size(); i++) {
            leftScratch[i] = leftVector.get(i);
        }
        return leftScratch;
    }

    @Before
    public void setup() throws IOException {
        assumeTrue("Dense vector type is disabled", EsqlCapabilities.Cap.DENSE_VECTOR_FIELD_TYPE.isEnabled());

        createIndexWithDenseVector("test");

        numDims = randomIntBetween(32, 64) * 2; // min 64, even number
        int numDocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            List<Float> leftVector = randomVector();
            List<Float> rightVector = randomVector();
            docs[i] = prepareIndex("test").setId("" + i)
                .setSource("id", String.valueOf(i), "left_vector", leftVector, "right_vector", rightVector);
        }

        indexRandom(true, docs);
    }

    private List<Float> randomVector() {
        assert numDims != 0 : "numDims must be set before calling randomVector()";
        List<Float> vector = new ArrayList<>(numDims);
        for (int j = 0; j < numDims; j++) {
            vector.add(randomFloat());
        }
        return vector;
    }

    private float[] randomVectorArray() {
        assert numDims != 0 : "numDims must be set before calling randomVectorArray()";
        float[] vector = new float[numDims];
        for (int j = 0; j < numDims; j++) {
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
        mapping.startObject(fieldName).field("type", "dense_vector").field("similarity", "cosine");
        mapping.endObject();
    }
}
