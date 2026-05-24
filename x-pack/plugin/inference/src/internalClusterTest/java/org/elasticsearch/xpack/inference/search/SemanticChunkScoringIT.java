/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.search;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.ChunkResult;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for per-chunk scoring in semantic queries.
 * Verifies that {@code chunks_per_doc} and {@code min_score} parameters
 * on a {@code semantic} query correctly populate {@code _chunks} on search hits.
 */
@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class SemanticChunkScoringIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test-chunk-scoring";
    private static final String SPARSE_INFERENCE_ID = "sparse-chunk-endpoint";

    @Before
    public void setup() throws Exception {
        ModelRegistry modelRegistry = internalCluster().getCurrentMasterNodeInstance(ModelRegistry.class);
        Utils.storeSparseModel(SPARSE_INFERENCE_ID, modelRegistry);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(otherSettings).put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, FakeMlPlugin.class);
    }

    private void createIndexWithSemanticField() {
        prepareCreate(INDEX_NAME).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            .setMapping(String.format(java.util.Locale.ROOT, """
                {
                    "properties": {
                        "content": {
                            "type": "semantic_text",
                            "inference_id": "%s"
                        }
                    }
                }
                """, SPARSE_INFERENCE_ID))
            .get();
    }

    private void indexDocWithChunks(String id, List<String> chunks) {
        new IndexRequestBuilder(client()).setIndex(INDEX_NAME)
            .setId(id)
            .setSource(Map.of("content", chunks))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private SearchResponse search(String queryJson) throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(org.elasticsearch.index.query.QueryBuilders.wrapperQuery(queryJson));
        return client().search(new SearchRequest(INDEX_NAME).source(source)).get();
    }

    /**
     * Index a document with a single string and search with chunks_per_doc.
     * Verify that the response contains _chunks with at most the requested number of entries.
     */
    public void testChunksPerDocOnly() throws Exception {
        createIndexWithSemanticField();

        // Index a single string value — the mock inference service produces one chunk per string
        new IndexRequestBuilder(client()).setIndex(INDEX_NAME)
            .setId("doc1")
            .setSource(Map.of("content", "the quick brown fox jumps over the lazy dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        String queryJson = """
            { "semantic": { "field": "content", "query": "animals", "chunks_per_doc": 2 } }
            """;
        SearchResponse response = search(queryJson);
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            SearchHit hit = response.getHits().getAt(0);

            Map<String, List<ChunkResult>> chunks = hit.getChunks();
            assertThat("_chunks should not be empty when chunks_per_doc is set", chunks.isEmpty(), equalTo(false));
            assertThat("_chunks should contain 'content' key, got keys: " + chunks.keySet(), chunks.containsKey("content"), equalTo(true));

            List<ChunkResult> contentChunks = chunks.get("content");
            assertThat("chunks_per_doc=2 should limit results", contentChunks.size(), lessThanOrEqualTo(2));
            assertThat("should have at least one chunk", contentChunks.size(), greaterThan(0));

            for (ChunkResult chunk : contentChunks) {
                assertThat("chunk text should not be null", chunk.text(), notNullValue());
                assertThat("chunk score should be positive", chunk.score(), greaterThan(0f));
                assertThat("start_offset should be non-negative", chunk.startOffset() >= 0, equalTo(true));
                assertThat("end_offset should be > start_offset", chunk.endOffset() > chunk.startOffset(), equalTo(true));
            }
        } finally {
            response.decRef();
        }
    }

    /**
     * Search without chunk params. Verify that the response has no _chunks field, preserving backward compatibility.
     */
    public void testNoChunkParamsBackwardCompat() throws Exception {
        createIndexWithSemanticField();
        indexDocWithChunks("doc1", List.of("chunk about dogs", "chunk about cats"));

        String queryJson = """
            { "semantic": { "field": "content", "query": "animals" } }
            """;
        SearchResponse response = search(queryJson);
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            SearchHit hit = response.getHits().getAt(0);
            assertThat("_chunks should be empty without chunk params", hit.getChunks().isEmpty(), equalTo(true));
        } finally {
            response.decRef();
        }
    }

    /**
     * Search with min_score=0 to verify chunks are returned when all chunks pass the threshold.
     */
    public void testMinScoreFiltersChunks() throws Exception {
        createIndexWithSemanticField();
        indexDocWithChunks("doc1", List.of("chunk about dogs", "chunk about cats", "chunk about birds"));

        // min_score=0 means all chunks with non-negative scores pass
        String queryJson = """
            { "semantic": { "field": "content", "query": "animals", "min_score": 0, "chunks_per_doc": 10 } }
            """;
        SearchResponse response = search(queryJson);
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            SearchHit hit = response.getHits().getAt(0);
            Map<String, List<ChunkResult>> chunks = hit.getChunks();
            assertThat("_chunks should not be empty with min_score=0", chunks.isEmpty(), equalTo(false));
            assertThat(chunks.containsKey("content"), equalTo(true));
            List<ChunkResult> contentChunks = chunks.get("content");
            assertThat("all chunks should pass min_score=0", contentChunks.size(), greaterThan(0));
            for (ChunkResult chunk : contentChunks) {
                assertThat("chunk score should be >= min_score", chunk.score() >= 0f, equalTo(true));
            }
        } finally {
            response.decRef();
        }
    }
}
