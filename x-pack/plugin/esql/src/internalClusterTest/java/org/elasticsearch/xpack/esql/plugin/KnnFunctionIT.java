/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class KnnFunctionIT extends AbstractEsqlIntegTestCase {

    private final Map<Integer, List<Float>> indexedVectors = new HashMap<>();

    public void testKnn() {
        var query = """
            FROM test METADATA _score
            | WHERE knn(vector, [1.0, 1.0, 1.0])
            | KEEP id, floats, _score, vector
            | SORT _score DESC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "floats", "_score", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double", "dense_vector"));

            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(indexedVectors.size(), valuesList.size());
            for (int i = 0; i < valuesList.size(); i++) {
                List<Object> row = valuesList.get(i);
                // Vectors should be in order of ID, as they're less similar than the query vector as the ID increases
                assertEquals(i, row.getFirst());
                @SuppressWarnings("unchecked")
                // Vectors should be the same
                List<Double> floats = (List<Double>) row.get(1);
                for (int j = 0; j < floats.size(); j++) {
                    assertEquals(floats.get(j).floatValue(), indexedVectors.get(i).get(j), 0f);
                }
                var score = (Double) row.get(2);
                assertNotNull(score);
                assertTrue(score > 0.0);
                // dense_vector is null for now
                assertNull(row.get(3));
            }
        }
    }

    @Before
    public void setup() throws IOException {
        var indexName = "test";
        var client = client().admin().indices();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject()
            .startObject("vector")
            .field("type", "dense_vector")
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("floats")
            .field("type", "float")
            .endObject()
            .endObject()
            .endObject();

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);

        var CreateRequest = client.prepareCreate(indexName).setMapping(mapping).setSettings(settingsBuilder.build());
        assertAcked(CreateRequest);

        int numDocs = 10;
        int numDims = 3;
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        float value = 0.0f;
        for (int i = 0; i < numDocs; i++) {
            List<Float> vector = new ArrayList<>(numDims);
            for (int j = 0; j < numDims; j++) {
                vector.add(value++);
            }
            docs[i] = prepareIndex("test").setId("" + i).setSource("id", String.valueOf(i), "floats", vector, "vector", vector);
            indexedVectors.put(i, vector);
        }

        indexRandom(true, docs);
    }
}
