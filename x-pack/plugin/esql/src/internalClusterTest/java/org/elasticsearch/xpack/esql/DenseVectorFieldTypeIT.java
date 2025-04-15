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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING;
import static org.elasticsearch.index.mapper.SourceFieldMapper.Mode.SYNTHETIC;
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
    private final boolean index;
    private final boolean synthetic;

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        // Indexed field types
        for (String indexType : DENSE_VECTOR_INDEX_TYPES) {
            params.add(new Object[] { indexType, true, false });
        }
        // No indexing
        params.add(new Object[] { null, false, false });
        // No indexing, synthetic source
        params.add(new Object[] { null, false, true });
        return params;
    }

    public DenseVectorFieldTypeIT(@Name("indexType") String indexType, @Name("index") boolean index, @Name("synthetic") boolean synthetic) {
        this.indexType = indexType;
        this.index = index;
        this.synthetic = synthetic;
    }

    private final Map<Integer, List<Float>> indexedVectors = new HashMap<>();

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
    public void testRetrieveTopNDenseVectorFieldData() {
        var query = """
                FROM test
                | KEEP id, vector
                | SORT id ASC
            """;

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            indexedVectors.forEach((id, vector) -> {
                var values = valuesList.get(id);
                assertEquals(id, values.get(0));
                List<Float> vectors = (List<Float>) values.get(1);
                assertNotNull(vectors);
                assertEquals(vector.size(), vectors.size());
                for (int i = 0; i < vector.size(); i++) {
                    assertEquals(vector.get(i), vectors.get(i), 0F);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testRetrieveDenseVectorFieldData() {
        var query = """
            FROM test
            | KEEP id, vector
            """;

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            assertEquals(valuesList.size(), indexedVectors.size());
            valuesList.forEach(value -> {
                ;
                assertEquals(2, value.size());
                Integer id = (Integer) value.get(0);
                List<Float> vector = (List<Float>) value.get(1);
                assertNotNull(vector);
                List<Float> expectedVector = indexedVectors.get(id);
                assertNotNull(expectedVector);
                for (int i = 0; i < vector.size(); i++) {
                    assertEquals(expectedVector.get(i), vector.get(i), 0F);
                }
            });
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
            .field("index", index);
        if (index) {
            mapping.field("similarity", "l2_norm");
        }
        if (indexType != null) {
            mapping.startObject("index_options").field("type", indexType).endObject();
        }
        mapping.endObject().endObject().endObject();
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5));
        if (synthetic) {
            settingsBuilder.put(INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SYNTHETIC);
        }

        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping(mapping)
            .setSettings(settingsBuilder.build());
        assertAcked(CreateRequest);

        int numDims = randomIntBetween(32, 64) * 2; // min 64, even number
        int numDocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            List<Float> vector = new ArrayList<>(numDims);
            for (int j = 0; j < numDims; j++) {
                vector.add(randomFloat());
            }
            docs[i] = prepareIndex("test").setId("" + i).setSource("id", String.valueOf(i), "vector", vector);
            indexedVectors.put(i, vector);
        }

        indexRandom(true, docs);
    }
}
