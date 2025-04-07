/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class DenseVectorFieldTypeIT extends AbstractEsqlIntegTestCase {

    private static final Set<String> DENSE_VECTOR_INDEX_TYPES = Set.of(
        "int8_hnsw",
        "hnsw",
        "int4_hnsw",
        "bbq_hnsw",
        "int8_flat",
        "int4_flat",
        "bbq_flat",
        "flat"
    );

    private final String indexType;
    private int numDims;
    private int numDocs;

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return DENSE_VECTOR_INDEX_TYPES.stream().map(type -> new Object[] { type }).toList();
    }

    public DenseVectorFieldTypeIT(@Name("indexType") String indexType) {
        this.indexType = indexType;
    }

    private Map<Integer, List<Float>> indexedDocs = new HashMap<>();

    public void testRetrieveFieldType() {
        var query = """
            FROM test
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "vector"));
            assertColumnTypes(resp.columns(), List.of("integer", "dense_vector"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testRetrieveOrderedDenseVectorFieldData() {
        var query = """
            FROM test
            | SORT id ASC
            """;

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            indexedDocs.forEach((id, vector) -> {
                var values = valuesList.get(id);
                assertEquals(id, values.get(0));
                List<Double> vectors = (List<Double>) values.get(1);
                assertEquals(vector.size(), vectors.size());
                for (int i = 0; i < vector.size(); i++) {
                    assertEquals(vector.get(i), vectors.get(i).floatValue(), 0F);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testRetrieveUnOrderedDenseVectorFieldData() {
        var query = "FROM test";

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(valuesList.size(), indexedDocs.size());
            valuesList.forEach(value -> {;
                assertEquals(2, value.size());
                Integer id = (Integer) value.get(0);
                List<Double> vector = (List<Double>) value.get(1);

                List<Float> expectedVector = indexedDocs.get(id);
                for (int i = 0; i < vector.size(); i++) {
                    assertEquals(expectedVector.get(i), vector.get(i).floatValue(), 0F);
                }
            });
        }
    }

    @Before
    public void setup() {
        numDims = randomIntBetween(64, 256);
        numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            List<Float> vector = new ArrayList<>(numDims);
            for (int j = 0; j < numDims; j++) {
//                vector.add(randomFloat());
                vector.add(1.0f);
            }
            indexedDocs.put(i, vector);
        }

        var indexName = "test";
        var client = client().admin().indices();
        var mapping = String.format(Locale.ROOT, """
            {
              "properties": {
                "id": {
                  "type": "integer"
                },
                "vector": {
                  "type": "dense_vector",
                  "similarity": "l2_norm",
                  "index_options": {
                    "type": "%s"
                  }
                }
              }
            }
            """, indexType);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5))
            .build();
        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping(mapping)
            .setSettings(settings);
        assertAcked(CreateRequest);

        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId("" + i).setSource("id", i, "vector", indexedDocs.get(i));
        }
        indexRandom(true, docs);
    }
}
