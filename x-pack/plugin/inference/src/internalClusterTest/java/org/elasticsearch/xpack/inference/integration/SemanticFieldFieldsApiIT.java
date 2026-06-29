/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

public class SemanticFieldFieldsApiIT extends ESIntegTestCase {

    private static final String INFERENCE_ID = "embedding-inference-id";
    private static final Map<String, Object> EMBEDDING_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        4,
        "similarity",
        "cosine",
        "api_key",
        "abc64"
    );
    private static final Map<String, String> IMAGE_1 = Map.of(
        "type",
        "image",
        "value",
        "data:image/jpeg;base64,Y2F0IG9uIGEgd2luZG93c2lsbA=="
    );
    private static final Map<String, String> IMAGE_2 = Map.of(
        "type",
        "image",
        "value",
        "data:image/jpeg;base64,ZG9nIHJ1bm5pbmcgaW4gYSBwYXJr"
    );

    private final String indexName = randomIdentifier();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class, FakeMlPlugin.class);
    }

    @Before
    public void setup() throws Exception {
        IntegrationTestUtils.createInferenceEndpoint(client(), TaskType.EMBEDDING, INFERENCE_ID, EMBEDDING_SERVICE_SETTINGS);
        createIndex();
    }

    @After
    public void cleanUp() {
        IntegrationTestUtils.deleteIndex(client(), indexName);
        IntegrationTestUtils.deleteInferenceEndpoint(client(), TaskType.EMBEDDING, INFERENCE_ID);
    }

    public void testFetchingMixedTextAndImageValues() throws Exception {
        indexDoc(
            "doc_fields_mixed",
            Map.of("source_field", "a b c d e f g h i j k l m n o p q r s t u v w x y z", "semantic_field", List.of(IMAGE_1, IMAGE_2))
        );

        SearchRequest request = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(idsQuery().addIds("doc_fields_mixed")).fetchField("semantic_field")
        );

        assertResponse(client().search(request), response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            List<Object> values = response.getHits().getAt(0).field("semantic_field").getValues();
            assertThat(values, containsInAnyOrder("a b c d e f g h i j k l m n o p q r s t u v w x y z", IMAGE_1, IMAGE_2));
        });
    }

    public void testFetchingMixedTextAndImageChunks() throws Exception {
        indexDoc(
            "doc_chunks_mixed",
            Map.of("source_field", "a b c d e f g h i j k l m n o p q r s t u v w x y z", "semantic_field", List.of(IMAGE_1, IMAGE_2))
        );

        SearchRequest request = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(idsQuery().addIds("doc_chunks_mixed"))
                .fetchField(new FieldAndFormat("semantic_field", "chunks"))
        );

        assertResponse(client().search(request), response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            List<Object> chunks = response.getHits().getAt(0).field("semantic_field").getValues();
            assertThat(chunks, hasItems(IMAGE_1, IMAGE_2));
            assertThat(chunks, hasItems("a b c d e f g h i j", " j k l m n o p q r s", " s t u v w x y z"));
        });
    }

    private void createIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping.startObject("source_field").field("type", "text").field("copy_to", "semantic_field").endObject();
        mapping.startObject("semantic_field").field("type", SemanticFieldMapper.CONTENT_TYPE).field("inference_id", INFERENCE_ID);
        mapping.startObject("chunking_settings").field("strategy", "word").field("max_chunk_size", 10).field("overlap", 1).endObject();
        mapping.endObject();
        mapping.endObject().endObject();

        assertAcked(prepareCreate(indexName).setMapping(mapping));
    }

    private void indexDoc(String id, Map<String, Object> source) {
        client().prepareIndex(indexName).setId(id).setSource(source).setRefreshPolicy(IMMEDIATE).get();
    }
}
