/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING;
import static org.elasticsearch.index.mapper.SourceFieldMapper.Mode.SYNTHETIC;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

public class DenseVectorFieldIndexTypeUpdateIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "update_index";
    public static final String VECTOR_FIELD = "vector";
    private final String initialType;
    private final String updateType;
    private int dimensions;

    public DenseVectorFieldIndexTypeUpdateIT(@Name("initialType") String initialType, @Name("updateType") String updateType) {
        this.initialType = initialType;
        this.updateType = updateType;
    }

    @ParametersFactory
    public static Collection<Object[]> params() {
        List<String> types = List.of(
            "flat",
            "int8_flat",
            "int4_flat",
            "bbq_flat",
            "hnsw",
            "int8_hnsw",
            "int4_hnsw",
            "bbq_hnsw",
            "bbq_disk"
        );

        // A type can be upgraded to types that follow in the list...
        List<Object[]> params = new java.util.ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            for (int j = i + 1; j < types.size(); j++) {
                params.add(new Object[] { types.get(i), types.get(j) });
            }
        }

        // ... except BBQ, that can only be upgraded to another BBQ type
        params = params.stream().filter(p -> {
            String from = (String) p[0];
            String to = (String) p[1];
            return from.contains("bbq") == false || to.contains("bbq");
        }).toList();

        return params;
    }

    @SuppressWarnings("unchecked")
    public void testDenseVectorMappingUpdate() throws Exception {
        dimensions = randomIntBetween(1, 10) * 64;
        var indicesClient = client().admin().indices();

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5));
        if (randomBoolean()) {
            settingsBuilder.put(INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SYNTHETIC);
        }

        // Create index with initial mapping
        var createRequest = indicesClient.prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)))
            .setMapping(updateMapping(dimensions, initialType))
            .setSettings(settingsBuilder.build());
        assertAcked(createRequest);

        // Index a variable number of docs before mapping update
        int docsBefore = randomIntBetween(1, 5);
        for (int i = 1; i <= docsBefore; i++) {
            indexDoc(i);
        }

        indicesClient.prepareFlush(INDEX_NAME).get();
        indicesClient.prepareRefresh(INDEX_NAME).get();

        // Update mapping to new type
        var putMappingRequest = indicesClient.preparePutMapping(INDEX_NAME).setSource(updateMapping(dimensions, updateType)).request();
        assertAcked(indicesClient.putMapping(putMappingRequest));

        // Validate mapping
        GetFieldMappingsResponse fieldMapping = indicesClient.getFieldMappings(
            indicesClient.prepareGetFieldMappings(INDEX_NAME).setFields(VECTOR_FIELD).request()
        ).get();
        var fieldMappingMetadata = fieldMapping.fieldMappings(INDEX_NAME, VECTOR_FIELD);
        var fieldMap = (Map<String, Object>) fieldMappingMetadata.sourceAsMap().get(VECTOR_FIELD);
        var indexOptions = (Map<String, Object>) fieldMap.get("index_options");
        assertThat(indexOptions.get("type"), equalTo(updateType));

        // Index a variable number of docs after mapping update
        int docsAfter = randomIntBetween(1, 5);
        for (int i = docsBefore + 1; i <= docsBefore + docsAfter; i++) {
            indexDoc(i);
        }

        indicesClient.prepareFlush(INDEX_NAME).get();
        indicesClient.prepareRefresh(INDEX_NAME).get();

        // Search to ensure all documents are present
        int expectedDocs = docsBefore + docsAfter;

        // Count query
        assertNoFailuresAndResponse(
            client().prepareSearch(INDEX_NAME).setSize(0).setTrackTotalHits(true).setSize(expectedDocs),
            response -> {
                assertHitCount(response, expectedDocs);
            }
        );

        // KNN query
        float[] queryVector = new float[dimensions];
        for (int i = 0; i < queryVector.length; i++) {
            queryVector[i] = randomFloatBetween(-1, 1, true);
        }
        KnnVectorQueryBuilder queryBuilder = new KnnVectorQueryBuilder(VECTOR_FIELD, queryVector, null, null, null, null, null);
        assertNoFailuresAndResponse(
            client().prepareSearch(INDEX_NAME).setQuery(queryBuilder).setTrackTotalHits(true).setSize(expectedDocs),
            response -> {
                assertHitCount(response, expectedDocs);
            }
        );
    }

    private XContentBuilder updateMapping(int dimensions, String type) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            createFieldMapping(dimensions, type, builder);
        }
        builder.endObject();
        return builder;
    }

    private static void createFieldMapping(int dimensions, String type, XContentBuilder builder) throws IOException {
        builder.startObject("properties");
        {
            builder.startObject(VECTOR_FIELD);
            builder.field("type", "dense_vector");
            builder.field("dims", dimensions);
            builder.field("index", true);
            builder.startObject("index_options");
            builder.field("type", type);
            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
    }

    private void indexDoc(int id) throws ExecutionException, InterruptedException {
        Float[] vector = randomArray(dimensions, dimensions, Float[]::new, () -> randomFloatBetween(-1, 1, true));
        IndexRequest indexRequest = prepareIndex(INDEX_NAME).setSource(VECTOR_FIELD, vector).setId(Integer.toString(id)).request();
        DocWriteResponse indexResponse = client().index(indexRequest).get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
    }
}
