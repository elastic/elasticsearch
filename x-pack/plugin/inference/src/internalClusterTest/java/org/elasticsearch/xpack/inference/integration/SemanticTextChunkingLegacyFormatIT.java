/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests ported from {@code 25_semantic_text_field_mapping_chunking_bwc.yml},
 * covering chunking configurations for legacy-format semantic_text indices: mapping verification,
 * search highlights with different chunking strategies, bulk indexing, and error handling.
 */
public class SemanticTextChunkingLegacyFormatIT extends SemanticTextLegacyFormatTestCase {

    private static final String INFERENCE_FIELD = "inference_field";
    private static final String KEYWORD_FIELD = "keyword_field";
    private static final String DEFAULT_CHUNKED_FIELD = "default_chunked_inference_field";
    private static final String CUSTOMIZED_CHUNKED_FIELD = "customized_chunked_inference_field";

    /**
     * Long text that produces a single chunk with sentence/default chunking, and three chunks with
     * word chunking (max_chunk_size=10, overlap=1).
     */
    private static final String LONG_TEXT =
        "Elasticsearch is an open source, distributed, RESTful, search engine which is built on top of Lucene internally"
            + " and enjoys all the features it provides.";

    /**
     * Shorter text that produces two chunks with word chunking (max_chunk_size=10, overlap=1).
     */
    private static final String SHORT_TEXT =
        "Elasticsearch is a free, open-source search engine and analytics tool that stores and indexes data.";

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        for (String suffix : List.of("-ds", "-dd", "-cs", "-cd", "-nd", "-i1", "-i2")) {
            IntegrationTestUtils.deleteIndex(client(), indexName + suffix);
        }
    }

    /**
     * Ported from "We return chunking configurations with mappings" in {@code 25_semantic_text_field_mapping_chunking_bwc.yml}.
     * Creates legacy indices with default, custom (word), and none chunking strategies and
     * verifies that the chunking_settings are reflected correctly in the index mapping.
     */
    @SuppressWarnings("unchecked")
    public void testMappingReturnsChunkingConfigurations() throws Exception {
        String defaultSparse = indexName + "-ds";
        String defaultDense = indexName + "-dd";
        String customSparse = indexName + "-cs";
        String customDense = indexName + "-cd";
        String noneDense = indexName + "-nd";

        // default chunking (no chunking_settings specified)
        assertAcked(
            prepareCreate(defaultSparse).setSettings(legacyIndexSettings()).setMapping(singleFieldMapping(SPARSE_INFERENCE_ID, null)).get()
        );
        assertAcked(
            prepareCreate(defaultDense).setSettings(legacyIndexSettings()).setMapping(singleFieldMapping(DENSE_INFERENCE_ID, null)).get()
        );

        // custom word chunking
        assertAcked(
            prepareCreate(customSparse).setSettings(legacyIndexSettings()).setMapping(singleFieldMapping(SPARSE_INFERENCE_ID, "word")).get()
        );
        assertAcked(
            prepareCreate(customDense).setSettings(legacyIndexSettings()).setMapping(singleFieldMapping(DENSE_INFERENCE_ID, "word")).get()
        );

        // none chunking
        assertAcked(
            prepareCreate(noneDense).setSettings(legacyIndexSettings()).setMapping(noneChunkingFieldMapping(DENSE_INFERENCE_ID)).get()
        );

        // default indices must not expose chunking_settings
        assertMappingHasNoChunkingSettings(defaultSparse, INFERENCE_FIELD);
        assertMappingHasNoChunkingSettings(defaultDense, INFERENCE_FIELD);

        // custom indices must expose word chunking settings
        Map<String, Object> csCustomSparse = getChunkingSettings(customSparse, INFERENCE_FIELD);
        assertThat(csCustomSparse, notNullValue());
        assertThat("strategy", csCustomSparse.get("strategy"), equalTo("word"));
        assertThat("max_chunk_size", csCustomSparse.get("max_chunk_size"), equalTo(10));
        assertThat("overlap", csCustomSparse.get("overlap"), equalTo(1));

        Map<String, Object> csCustomDense = getChunkingSettings(customDense, INFERENCE_FIELD);
        assertThat(csCustomDense, notNullValue());
        assertThat("strategy", csCustomDense.get("strategy"), equalTo("word"));
        assertThat("max_chunk_size", csCustomDense.get("max_chunk_size"), equalTo(10));
        assertThat("overlap", csCustomDense.get("overlap"), equalTo(1));

        // none-chunking index must expose only the strategy
        Map<String, Object> csNoneDense = getChunkingSettings(noneDense, INFERENCE_FIELD);
        assertThat(csNoneDense, notNullValue());
        assertThat("strategy", csNoneDense.get("strategy"), equalTo("none"));
    }

    /**
     * Ported from "We do not set custom chunking settings for null chunking settings" in
     * {@code 25_semantic_text_field_mapping_chunking_bwc.yml}.
     * Creates a legacy index with {@code chunking_settings: null} and verifies that
     * no chunking_settings appear in the mapping.
     */
    public void testNullChunkingSettingsNotStored() throws Exception {
        assertAcked(
            prepareCreate(indexName).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(INFERENCE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .nullField("chunking_settings")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        assertMappingHasNoChunkingSettings(indexName, INFERENCE_FIELD);
    }

    /**
     * Ported from "We do not set custom chunking settings for empty specified chunking settings" in
     * {@code 25_semantic_text_field_mapping_chunking_bwc.yml}.
     * Creates a legacy index with an empty {@code chunking_settings: {}} object and verifies that
     * no chunking_settings appear in the mapping.
     */
    public void testEmptyChunkingSettingsNotStored() throws Exception {
        assertAcked(
            prepareCreate(indexName).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(INFERENCE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .startObject("chunking_settings")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        assertMappingHasNoChunkingSettings(indexName, INFERENCE_FIELD);
    }

    /**
     * Ported from "We return different chunks based on configured chunking overrides or model defaults for sparse embeddings" in
     * {@code 25_semantic_text_field_mapping_chunking_bwc.yml}.
     * Indexes a document into both a default-chunking (sparse) index and a custom word-chunking
     * (sparse) index, then verifies that highlights reflect the different chunking configurations:
     * the default index produces a single fragment while the custom index produces three.
     */
    public void testSparseEmbeddingsChunkingVariantsReturnDifferentHighlights() throws Exception {
        String defaultSparse = indexName + "-ds";
        String customSparse = indexName + "-cs";

        assertAcked(
            prepareCreate(defaultSparse).setSettings(legacyIndexSettings()).setMapping(singleFieldMapping(SPARSE_INFERENCE_ID, null)).get()
        );
        assertAcked(
            prepareCreate(customSparse).setSettings(legacyIndexSettings()).setMapping(singleFieldMapping(SPARSE_INFERENCE_ID, "word")).get()
        );

        client().prepareIndex(defaultSparse).setId("doc_1").setSource(Map.of(INFERENCE_FIELD, LONG_TEXT)).get();
        client().admin().indices().prepareRefresh(defaultSparse).get();

        client().prepareIndex(customSparse).setId("doc_2").setSource(Map.of(INFERENCE_FIELD, LONG_TEXT)).get();
        client().admin().indices().prepareRefresh(customSparse).get();

        // default chunking: single sentence → 1 fragment
        assertResponse(
            client().search(
                new SearchRequest(defaultSparse).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(INFERENCE_FIELD, "What is Elasticsearch?"))
                        .highlighter(
                            new HighlightBuilder().field(
                                new HighlightBuilder.Field(INFERENCE_FIELD).highlighterType("semantic").numOfFragments(3)
                            )
                        )
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, INFERENCE_FIELD, 0, 1, equalTo(LONG_TEXT));
            }
        );

        // custom word chunking: max_chunk_size=10, overlap=1 → 3 fragments
        assertResponse(
            client().search(
                new SearchRequest(customSparse).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(INFERENCE_FIELD, "What is Elasticsearch?"))
                        .highlighter(
                            new HighlightBuilder().field(
                                new HighlightBuilder.Field(INFERENCE_FIELD).highlighterType("semantic").numOfFragments(3)
                            )
                        )
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(
                    response,
                    0,
                    INFERENCE_FIELD,
                    0,
                    3,
                    equalTo("Elasticsearch is an open source, distributed, RESTful, search engine which")
                );
                assertHighlight(response, 0, INFERENCE_FIELD, 1, 3, equalTo(" which is built on top of Lucene internally and enjoys"));
                assertHighlight(response, 0, INFERENCE_FIELD, 2, 3, equalTo(" enjoys all the features it provides."));
            }
        );
    }

    /**
     * Ported from "We return different chunks based on configured chunking overrides or model defaults for dense embeddings" in
     * {@code 25_semantic_text_field_mapping_chunking_bwc.yml}.
     * Indexes a document into a default-chunking (dense), a custom word-chunking (dense), and a
     * none-chunking (dense) index, then verifies that highlights reflect the different chunking
     * configurations.
     */
    public void testDenseEmbeddingsChunkingVariantsReturnDifferentHighlights() throws Exception {
        String defaultDense = indexName + "-dd";
        String customDense = indexName + "-cd";
        String noneDense = indexName + "-nd";

        assertAcked(
            prepareCreate(defaultDense).setSettings(legacyIndexSettings()).setMapping(singleFieldMapping(DENSE_INFERENCE_ID, null)).get()
        );
        assertAcked(
            prepareCreate(customDense).setSettings(legacyIndexSettings()).setMapping(singleFieldMapping(DENSE_INFERENCE_ID, "word")).get()
        );
        assertAcked(
            prepareCreate(noneDense).setSettings(legacyIndexSettings()).setMapping(noneChunkingFieldMapping(DENSE_INFERENCE_ID)).get()
        );

        client().prepareIndex(defaultDense).setId("doc_3").setSource(Map.of(INFERENCE_FIELD, LONG_TEXT)).get();
        client().admin().indices().prepareRefresh(defaultDense).get();

        client().prepareIndex(customDense).setId("doc_4").setSource(Map.of(INFERENCE_FIELD, LONG_TEXT)).get();
        client().admin().indices().prepareRefresh(customDense).get();

        client().prepareIndex(noneDense).setId("doc_5").setSource(Map.of(INFERENCE_FIELD, LONG_TEXT)).get();
        client().admin().indices().prepareRefresh(noneDense).get();

        // default chunking → 1 fragment
        assertResponse(
            client().search(
                new SearchRequest(defaultDense).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(INFERENCE_FIELD, "What is Elasticsearch?"))
                        .highlighter(
                            new HighlightBuilder().field(
                                new HighlightBuilder.Field(INFERENCE_FIELD).highlighterType("semantic").numOfFragments(2)
                            )
                        )
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, INFERENCE_FIELD, 0, 1, equalTo(LONG_TEXT));
            }
        );

        // custom word chunking → 3 fragments
        assertResponse(
            client().search(
                new SearchRequest(customDense).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(INFERENCE_FIELD, "What is Elasticsearch?"))
                        .highlighter(
                            new HighlightBuilder().field(
                                new HighlightBuilder.Field(INFERENCE_FIELD).highlighterType("semantic").numOfFragments(3)
                            )
                        )
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(
                    response,
                    0,
                    INFERENCE_FIELD,
                    0,
                    3,
                    equalTo("Elasticsearch is an open source, distributed, RESTful, search engine which")
                );
                assertHighlight(response, 0, INFERENCE_FIELD, 1, 3, equalTo(" which is built on top of Lucene internally and enjoys"));
                assertHighlight(response, 0, INFERENCE_FIELD, 2, 3, equalTo(" enjoys all the features it provides."));
            }
        );

        // none chunking → 1 fragment (whole text, no splitting)
        assertResponse(
            client().search(
                new SearchRequest(noneDense).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(INFERENCE_FIELD, "What is Elasticsearch?"))
                        .highlighter(
                            new HighlightBuilder().field(
                                new HighlightBuilder.Field(INFERENCE_FIELD).highlighterType("semantic").numOfFragments(2)
                            )
                        )
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, INFERENCE_FIELD, 0, 1, equalTo(LONG_TEXT));
            }
        );
    }

    /**
     * Ported from "We respect multiple semantic_text fields with different chunking configurations"
     * in {@code 25_semantic_text_field_mapping_chunking_bwc.yml}.
     * Creates a single legacy index with two semantic_text fields — one with default chunking and
     * one with custom word chunking — indexes a document, and verifies that highlights for each
     * field reflect the respective chunking configuration.
     */
    public void testMixedChunkingFieldsInSameIndex() throws Exception {
        assertAcked(
            prepareCreate(indexName).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(KEYWORD_FIELD)
                        .field("type", "keyword")
                        .endObject()
                        .startObject(DEFAULT_CHUNKED_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .endObject()
                        .startObject(CUSTOMIZED_CHUNKED_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .startObject("chunking_settings")
                        .field("strategy", "word")
                        .field("max_chunk_size", 10)
                        .field("overlap", 1)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        client().prepareIndex(indexName)
            .setId("doc_1")
            .setSource(Map.of(DEFAULT_CHUNKED_FIELD, LONG_TEXT, CUSTOMIZED_CHUNKED_FIELD, LONG_TEXT))
            .get();
        client().admin().indices().prepareRefresh(indexName).get();

        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(
                        new BoolQueryBuilder().should(new SemanticQueryBuilder(DEFAULT_CHUNKED_FIELD, "What is Elasticsearch?"))
                            .should(new SemanticQueryBuilder(CUSTOMIZED_CHUNKED_FIELD, "What is Elasticsearch?"))
                    )
                        .highlighter(
                            new HighlightBuilder().field(
                                new HighlightBuilder.Field(DEFAULT_CHUNKED_FIELD).highlighterType("semantic").numOfFragments(3)
                            ).field(new HighlightBuilder.Field(CUSTOMIZED_CHUNKED_FIELD).highlighterType("semantic").numOfFragments(3))
                        )
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                // default chunking → 1 fragment
                assertHighlight(response, 0, DEFAULT_CHUNKED_FIELD, 0, 1, equalTo(LONG_TEXT));
                // word chunking → 3 fragments
                assertHighlight(
                    response,
                    0,
                    CUSTOMIZED_CHUNKED_FIELD,
                    0,
                    3,
                    equalTo("Elasticsearch is an open source, distributed, RESTful, search engine which")
                );
                assertHighlight(
                    response,
                    0,
                    CUSTOMIZED_CHUNKED_FIELD,
                    1,
                    3,
                    equalTo(" which is built on top of Lucene internally and enjoys")
                );
                assertHighlight(response, 0, CUSTOMIZED_CHUNKED_FIELD, 2, 3, equalTo(" enjoys all the features it provides."));
            }
        );
    }

    /**
     * Ported from "Bulk requests are handled appropriately" in {@code 25_semantic_text_field_mapping_chunking_bwc.yml}.
     * Bulk-indexes three documents across two indices with different chunking configurations and
     * verifies that highlights for each document match its index's chunking strategy.
     */
    public void testBulkRequestsWithDifferentChunkingConfigurations() throws Exception {
        String index1 = indexName + "-i1";  // custom word chunking
        String index2 = indexName + "-i2";  // default chunking

        assertAcked(
            prepareCreate(index1).setSettings(legacyIndexSettings()).setMapping(singleFieldMapping(SPARSE_INFERENCE_ID, "word")).get()
        );
        assertAcked(
            prepareCreate(index2).setSettings(legacyIndexSettings()).setMapping(singleFieldMapping(SPARSE_INFERENCE_ID, null)).get()
        );

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy("true");
        bulk.add(new IndexRequestBuilder(client()).setIndex(index1).setId("doc_1").setSource(Map.of(INFERENCE_FIELD, LONG_TEXT)));
        bulk.add(new IndexRequestBuilder(client()).setIndex(index2).setId("doc_2").setSource(Map.of(INFERENCE_FIELD, LONG_TEXT)));
        bulk.add(new IndexRequestBuilder(client()).setIndex(index1).setId("doc_3").setSource(Map.of(INFERENCE_FIELD, SHORT_TEXT)));
        BulkResponse bulkResponse = bulk.get();
        assertFalse(bulkResponse.hasFailures());

        assertResponse(
            client().search(
                new SearchRequest(index1, index2).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(INFERENCE_FIELD, "What is Elasticsearch?"))
                        .highlighter(
                            new HighlightBuilder().field(
                                new HighlightBuilder.Field(INFERENCE_FIELD).highlighterType("semantic").numOfFragments(3)
                            )
                        )
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 3L);
                for (var hit : response.getHits().getHits()) {
                    var highlights = hit.getHighlightFields().get(INFERENCE_FIELD);
                    assertThat("highlight for " + hit.getId(), highlights, notNullValue());
                    switch (hit.getId()) {
                        case "doc_1" -> {
                            // index1 custom word chunking on LONG_TEXT → 3 fragments
                            assertThat("doc_1 fragment count", highlights.fragments().length, equalTo(3));
                            assertThat(
                                highlights.fragments()[0].string(),
                                equalTo("Elasticsearch is an open source, distributed, RESTful, search engine which")
                            );
                            assertThat(
                                highlights.fragments()[1].string(),
                                equalTo(" which is built on top of Lucene internally and enjoys")
                            );
                            assertThat(highlights.fragments()[2].string(), equalTo(" enjoys all the features it provides."));
                        }
                        case "doc_2" -> {
                            // index2 default chunking on LONG_TEXT → 1 fragment
                            assertThat("doc_2 fragment count", highlights.fragments().length, equalTo(1));
                            assertThat(highlights.fragments()[0].string(), equalTo(LONG_TEXT));
                        }
                        case "doc_3" -> {
                            // index1 custom word chunking on SHORT_TEXT → 2 fragments
                            assertThat("doc_3 fragment count", highlights.fragments().length, equalTo(2));
                            assertThat(
                                highlights.fragments()[0].string(),
                                equalTo("Elasticsearch is a free, open-source search engine and analytics")
                            );
                            assertThat(highlights.fragments()[1].string(), equalTo(" analytics tool that stores and indexes data."));
                        }
                        default -> fail("unexpected hit id: " + hit.getId());
                    }
                }
            }
        );
    }

    /**
     * Ported from "Invalid chunking settings will result in an error" in {@code 25_semantic_text_field_mapping_chunking_bwc.yml}.
     * Verifies that creating a legacy index with invalid chunking_settings (extra unknown fields,
     * missing required fields, or an invalid strategy) produces a descriptive error.
     */
    public void testInvalidChunkingSettingsFailsWithError() throws Exception {
        // extra unknown field
        Exception eExtra = assertThrows(
            Exception.class,
            () -> prepareCreate(indexName + "-fail-1").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(KEYWORD_FIELD)
                        .field("type", "keyword")
                        .endObject()
                        .startObject(INFERENCE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .startObject("chunking_settings")
                        .field("strategy", "word")
                        .field("max_chunk_size", 10)
                        .field("overlap", 1)
                        .field("extra", "stuff")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
        assertThat(eExtra.getMessage(), containsString("chunking settings can not have the following settings"));

        // missing required field (max_chunk_size for word strategy)
        Exception eMissing = assertThrows(
            Exception.class,
            () -> prepareCreate(indexName + "-fail-2").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(KEYWORD_FIELD)
                        .field("type", "keyword")
                        .endObject()
                        .startObject(INFERENCE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .startObject("chunking_settings")
                        .field("strategy", "word")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
        assertThat(eMissing.getMessage(), containsString("[chunking_settings] does not contain the required setting [max_chunk_size]"));

        // invalid strategy value
        Exception eStrategy = assertThrows(
            Exception.class,
            () -> prepareCreate(indexName + "-fail-3").setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(INFERENCE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .startObject("chunking_settings")
                        .field("strategy", "invalid")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
        assertThat(eStrategy.getMessage(), containsString("Invalid chunkingStrategy"));
    }

    /**
     * Ported from "We can update chunking settings" in {@code 25_semantic_text_field_mapping_chunking_bwc.yml}.
     * Creates a legacy index without chunking settings, verifies they are absent, updates the
     * mapping to add word chunking settings, verifies they are present, then removes them by
     * updating the mapping again without chunking_settings, and verifies they are absent once more.
     */
    public void testUpdateChunkingSettings() throws Exception {
        assertAcked(
            prepareCreate(indexName).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(INFERENCE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        assertMappingHasNoChunkingSettings(indexName, INFERENCE_FIELD);

        // add chunking settings via put_mapping
        indicesAdmin().preparePutMapping(indexName)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject(INFERENCE_FIELD)
                    .field("type", "semantic_text")
                    .field("inference_id", SPARSE_INFERENCE_ID)
                    .startObject("chunking_settings")
                    .field("strategy", "word")
                    .field("max_chunk_size", 10)
                    .field("overlap", 1)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        Map<String, Object> cs = getChunkingSettings(indexName, INFERENCE_FIELD);
        assertThat(cs, notNullValue());
        assertThat("strategy", cs.get("strategy"), equalTo("word"));
        assertThat("max_chunk_size", cs.get("max_chunk_size"), equalTo(10));
        assertThat("overlap", cs.get("overlap"), equalTo(1));

        // remove chunking settings by updating without chunking_settings
        indicesAdmin().preparePutMapping(indexName)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject(INFERENCE_FIELD)
                    .field("type", "semantic_text")
                    .field("inference_id", SPARSE_INFERENCE_ID)
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        assertMappingHasNoChunkingSettings(indexName, INFERENCE_FIELD);
    }

    // ----------------------------- helpers -------------------------------------------------------

    /**
     * Returns an XContentBuilder for a mapping with a {@code keyword_field} and a single
     * {@code inference_field} semantic_text field. If {@code chunkingStrategy} is {@code "word"},
     * word chunking settings (max_chunk_size=10, overlap=1) are added; if {@code null}, no
     * chunking_settings are emitted.
     */
    private static XContentBuilder singleFieldMapping(String inferenceId, String chunkingStrategy) throws IOException {
        var b = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(KEYWORD_FIELD)
            .field("type", "keyword")
            .endObject()
            .startObject(INFERENCE_FIELD)
            .field("type", "semantic_text")
            .field("inference_id", inferenceId);
        if ("word".equals(chunkingStrategy)) {
            b.startObject("chunking_settings").field("strategy", "word").field("max_chunk_size", 10).field("overlap", 1).endObject();
        }
        return b.endObject().endObject().endObject();
    }

    /**
     * Returns an XContentBuilder for a mapping with a {@code keyword_field} and an
     * {@code inference_field} semantic_text field configured with {@code strategy: none}.
     */
    private static XContentBuilder noneChunkingFieldMapping(String inferenceId) throws Exception {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(KEYWORD_FIELD)
            .field("type", "keyword")
            .endObject()
            .startObject(INFERENCE_FIELD)
            .field("type", "semantic_text")
            .field("inference_id", inferenceId)
            .startObject("chunking_settings")
            .field("strategy", "none")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

    /**
     * Asserts that the mapping for {@code fieldName} in {@code index} does not contain a
     * {@code chunking_settings} entry.
     */
    @SuppressWarnings("unchecked")
    private void assertMappingHasNoChunkingSettings(String index, String fieldName) {
        GetMappingsResponse resp = client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, index).get();
        MappingMetadata meta = resp.getMappings().get(index);
        assertThat("mapping metadata for [" + index + "]", meta, notNullValue());
        Map<String, Object> properties = (Map<String, Object>) meta.getSourceAsMap().get("properties");
        assertThat("properties for [" + index + "]", properties, notNullValue());
        Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
        assertThat("field mapping for [" + fieldName + "]", fieldMapping, notNullValue());
        assertFalse(
            "chunking_settings should be absent from [" + fieldName + "] in [" + index + "]",
            fieldMapping.containsKey("chunking_settings")
        );
    }

    /**
     * Returns the {@code chunking_settings} map from the mapping of {@code fieldName} in
     * {@code index}, or {@code null} if absent.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> getChunkingSettings(String index, String fieldName) {
        GetMappingsResponse resp = client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, index).get();
        MappingMetadata meta = resp.getMappings().get(index);
        assertThat("mapping metadata for [" + index + "]", meta, notNullValue());
        Map<String, Object> properties = (Map<String, Object>) meta.getSourceAsMap().get("properties");
        assertThat("properties for [" + index + "]", properties, notNullValue());
        Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
        assertThat("field mapping for [" + fieldName + "]", fieldMapping, notNullValue());
        return (Map<String, Object>) fieldMapping.get("chunking_settings");
    }
}
