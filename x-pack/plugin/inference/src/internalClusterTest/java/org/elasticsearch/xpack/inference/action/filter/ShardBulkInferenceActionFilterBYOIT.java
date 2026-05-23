/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.INDICES_INFERENCE_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticTextInput;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for BYO (bring-your-own) chunks and vectors on semantic_text fields.
 * Verifies single-shot BYO indexing and backward compatibility with plain string input.
 * Multi-part staging lifecycle is covered by unit tests in
 * {@link ShardBulkInferenceActionFilterBYOTests} because it requires read-modify-write
 * semantics that are better suited to controlled unit test environments.
 */
@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class ShardBulkInferenceActionFilterBYOIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "byo-test-index";
    private static final String DENSE_INFERENCE_ID = "dense-byo-endpoint";
    private static final int DIMS = 3;

    private ModelRegistry modelRegistry;

    @Before
    public void setup() throws Exception {
        modelRegistry = internalCluster().getCurrentMasterNodeInstance(ModelRegistry.class);
        Utils.storeDenseModel(DENSE_INFERENCE_ID, modelRegistry, DIMS, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(otherSettings)
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .put(INDICES_INFERENCE_BATCH_SIZE.getKey(), ByteSizeValue.ofKb(1))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
    }

    /**
     * Indexes a document with precomputed chunks and vectors in a single request,
     * then verifies the document is indexed and searchable.
     */
    public void testSingleShotBYO() throws Exception {
        String indexName = INDEX_NAME + "-singleshot";
        createSemanticTextIndex(indexName);

        String text = "hello world";
        Map<String, Object> byoValue = new LinkedHashMap<>();
        byoValue.put("text", text);
        byoValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3)), chunkMap(5, 11, List.of(0.4, 0.5, 0.6))));

        BulkResponse response = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequestBuilder(client()).setIndex(indexName).setId("1").setSource(Map.of("dense_field", byoValue)))
            .get();
        assertNoFailures(response);
        assertThat(numHits(indexName), equalTo(1));
    }

    /**
     * Indexes multiple BYO documents in a single bulk request to verify
     * batch processing works correctly.
     */
    public void testSingleShotBYOMultipleDocs() throws Exception {
        String indexName = INDEX_NAME + "-multi";
        createSemanticTextIndex(indexName);

        var bulkBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 5; i++) {
            String text = "document number " + i;
            Map<String, Object> byoValue = new LinkedHashMap<>();
            byoValue.put("text", text);
            byoValue.put("chunks", List.of(chunkMap(0, text.length(), List.of(0.1 + i * 0.01, 0.2 + i * 0.01, 0.3 + i * 0.01))));
            bulkBuilder.add(
                new IndexRequestBuilder(client()).setIndex(indexName).setId(Integer.toString(i)).setSource(Map.of("dense_field", byoValue))
            );
        }

        BulkResponse response = bulkBuilder.get();
        assertNoFailures(response);
        assertThat(numHits(indexName), equalTo(5));
    }

    /**
     * Verifies that a plain string value still triggers inference (backward compatibility).
     */
    public void testPlainStringTriggersInference() throws Exception {
        String indexName = INDEX_NAME + "-plainstring";
        createSemanticTextIndex(indexName);

        BulkResponse response = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(
                new IndexRequestBuilder(client()).setIndex(indexName).setId("1").setSource(Map.of("dense_field", randomSemanticTextInput()))
            )
            .get();
        assertNoFailures(response);
        assertThat(numHits(indexName), equalTo(1));
    }

    /**
     * Verifies that BYO with incomplete chunk coverage fails validation.
     */
    public void testSingleShotBYOIncompleteChunksCausesFailure() {
        String indexName = INDEX_NAME + "-incomplete";
        createSemanticTextIndex(indexName);

        // Text is 11 chars ("hello world") but only one chunk covering [0,5)
        Map<String, Object> byoValue = new LinkedHashMap<>();
        byoValue.put("text", "hello world");
        byoValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3))));

        BulkResponse response = client().prepareBulk()
            .add(new IndexRequestBuilder(client()).setIndex(indexName).setId("1").setSource(Map.of("dense_field", byoValue)))
            .get();
        assertTrue(response.hasFailures());
        for (BulkItemResponse item : response.getItems()) {
            assertTrue(item.isFailed());
            assertThat(item.getFailureMessage(), containsString("Gap"));
        }
    }

    // ---- helpers ----

    private void createSemanticTextIndex(String indexName) {
        prepareCreate(indexName).setMapping(String.format(Locale.ROOT, """
            {
                "properties": {
                    "dense_field": {
                        "type": "semantic_text",
                        "inference_id": "%s"
                    }
                }
            }
            """, DENSE_INFERENCE_ID)).get();
    }

    private static Map<String, Object> chunkMap(int startOffset, int endOffset, List<Double> values) {
        Map<String, Object> chunk = new LinkedHashMap<>();
        chunk.put("start_offset", startOffset);
        chunk.put("end_offset", endOffset);
        chunk.put("embeddings", values);
        return chunk;
    }

    private int numHits(String indexName) throws Exception {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
        SearchResponse searchResponse = client().search(new SearchRequest(indexName).source(sourceBuilder)).get();
        try {
            return (int) searchResponse.getHits().getTotalHits().value();
        } finally {
            searchResponse.decRef();
        }
    }

    private void assertNoFailures(BulkResponse bulkResponse) {
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse item : bulkResponse.getItems()) {
                if (item.isFailed()) {
                    fail(item.getFailure().getCause(), "Failed to index document %s: %s", item.getId(), item.getFailureMessage());
                }
            }
        }
        assertFalse(bulkResponse.hasFailures());
    }
}
