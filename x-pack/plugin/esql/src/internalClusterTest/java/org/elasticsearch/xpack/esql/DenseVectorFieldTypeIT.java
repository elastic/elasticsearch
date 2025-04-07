/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

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

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return DENSE_VECTOR_INDEX_TYPES.stream().map(type -> new Object[] { type }).toList();
    }

    public DenseVectorFieldTypeIT(@Name("indexType") String indexType) {
        this.indexType = indexType;
    }

    private static Map<Integer, List<Float>> DOC_VALUES = new HashMap<>();
    static {
        DOC_VALUES.put(1, List.of(1.0f, 2.0f, 3.0f));
        DOC_VALUES.put(2, List.of(4.0f, 5.0f, 6.0f));
        DOC_VALUES.put(3, List.of(7.0f, 8.0f, 9.0f));
        DOC_VALUES.put(4, List.of(10.0f, 11.0f, 12.0f));
        DOC_VALUES.put(5, List.of(13.0f, 14.0f, 15.0f));
        DOC_VALUES.put(6, List.of(16.0f, 17.0f, 18.0f));
    }

    public void testRetrieveFieldType() {
        var query = """
            FROM test
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "vector"));
            assertColumnTypes(resp.columns(), List.of("long", "double"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testRetrieveDenseVectorFieldData() {
        var query = """
            FROM test
            | SORT id ASC
            """;

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            DOC_VALUES.forEach((id, vector) -> {
                var values = valuesList.get(id - 1);
                assertEquals(id.intValue(), ((Long) values.get(0)).intValue());
                List<Double> vectors = (List<Double>) values.get(1);
                assertEquals(vector.size(), vectors.size());
                for (int i = 0; i < vector.size(); i++) {
                    assertEquals((float) vector.get(i), vectors.get(i).floatValue(), 0F);
                }
            });
        }
    }

    @Before
    public void setup() {
        var indexName = "test";
        var client = client().admin().indices();
        var mapping = String.format(Locale.ROOT, """
                "id": integer,
                "vector": {
                    "type": "dense_vector",
                    "index_options": {
                        "type": "%s"
                    }
                }
            """, indexType);
        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping(mapping);
        assertAcked(CreateRequest);

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (var entry : DOC_VALUES.entrySet()) {
            bulkRequestBuilder.add(
                new IndexRequest(indexName).id(entry.getKey().toString()).source("id", entry.getKey(), "vector", entry.getValue())
            );
        }

        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow(indexName);
    }
}
