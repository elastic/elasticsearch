/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.vector;

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
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class VectorSimilarityFunctionsIT extends AbstractEsqlIntegTestCase {

    private static final Set<String> DENSE_VECTOR_INDEX_TYPES = Set.of(
        /*        "int8_hnsw",
            "hnsw",
            "int4_hnsw",
            "bbq_hnsw",
            "int8_flat",
            "int4_flat",
            "bbq_flat",*/
        "flat"
    );

    @SuppressWarnings("unchecked")
    public void testCosineSimilarity() {
        var query = """
                FROM test
                | EVAL similarity = v_cosine_similarity(left_vector, right_vector)
                | KEEP id, left_vector, right_vector, similarity
            """;

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            valuesList.forEach(values -> {
                List<Float> leftVector = (List<Float>) values.get(1);
                float[] leftScratch = new float[leftVector.size()];
                for (int i = 0; i < leftVector.size(); i++) {
                    leftScratch[i] = leftVector.get(i);
                }
                List<Float> rightVector = (List<Float>) values.get(2);
                float[] rightScratch = new float[rightVector.size()];
                for (int i = 0; i < rightVector.size(); i++) {
                    rightScratch[i] = rightVector.get(i);
                }
                Double similarity = (Double) values.get(3);
                assertNotNull(similarity);

                float expectedSimilarity = VectorSimilarityFunction.COSINE.compare(leftScratch, rightScratch);
                assertEquals(expectedSimilarity, similarity, 0.0001);
            });
        }
    }

    @Before
    public void setup() throws IOException {
        assumeTrue("Dense vector type is disabled", EsqlCapabilities.Cap.DENSE_VECTOR_FIELD_TYPE.isEnabled());

        createIndexWithDenseVector("test");

        int numDims = randomIntBetween(32, 64) * 2; // min 64, even number
        int numDocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            List<Float> leftVector = new ArrayList<>(numDims);
            for (int j = 0; j < numDims; j++) {
                leftVector.add(randomFloat());
            }
            List<Float> rightVector = new ArrayList<>(numDims);
            for (int j = 0; j < numDims; j++) {
                rightVector.add(randomFloat());
            }
            docs[i] = prepareIndex("test").setId("" + i)
                .setSource("id", String.valueOf(i), "left_vector", leftVector, "right_vector", rightVector);
        }

        indexRandom(true, docs);
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
