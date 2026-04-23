/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNotHighlighted;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.inference.Utils.storeDenseModel;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests ported from {@code 90_semantic_text_highlighter_bwc.yml}, covering
 * highlighting for sparse and dense semantic_text fields in legacy-format indices.
 */
public class SemanticTextHighlighterLegacyFormatIT extends SemanticTextLegacyFormatTestCase {

    private static final String LONG_CHUNK =
        "ElasticSearch is an open source, distributed, RESTful, search engine which is built on top of Lucene"
            + " internally and enjoys all the features it provides.";
    private static final String SHORT_CHUNK = "You Know, for Search!";
    private static final String LONG_CHUNK_2 = "For a moment, nothing happened. Then, after a second or so, nothing continued to happen.";
    private static final String DOC_2_TEXT =
        "Nothing travels faster than the speed of light with the possible exception of bad news, which obeys its own special laws.";

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        storeDenseModel(DENSE_BBQ_INFERENCE_ID, modelRegistry, 64, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        for (String suffix : List.of("-sparse", "-dense", "-mce", "-mas", "-mad", "-mams", "-mamd", "-flat", "-hnsw", "-bbqdisk")) {
            IntegrationTestUtils.deleteIndex(client(), indexName + suffix);
        }
    }

    private void createSparseHighlightIndex(String idx) throws Exception {
        assertAcked(
            prepareCreate(idx).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("body")
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    private void createDenseHighlightIndex(String idx) throws Exception {
        assertAcked(
            prepareCreate(idx).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("body")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("element_type", "float")
                        .endObject()
                        .endObject()
                        .endObject()
                        .startObject("another_body")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("element_type", "float")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    private HighlightBuilder.Field highlightField(String fieldName, int numFragments) {
        return new HighlightBuilder.Field(fieldName).highlighterType("semantic").numOfFragments(numFragments);
    }

    /**
     * Ported from "Highlighting using a sparse embedding model" in 90_semantic_text_highlighter_bwc.yml.
     * Verifies fragment count limiting and score-ordered highlighting for sparse fields.
     */
    public void testLegacyFormatHighlightingSparse() throws Exception {
        String idx = indexName + "-sparse";
        createSparseHighlightIndex(idx);

        Map<String, Object> source = new HashMap<>();
        source.put("body", new String[] { LONG_CHUNK, SHORT_CHUNK });
        client().prepareIndex(idx).setId("doc_1").setSource(source).get();
        client().admin().indices().prepareRefresh(idx).get();

        // number_of_fragments: 1
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder("body", "What is Elasticsearch?"))
                        .highlighter(new HighlightBuilder().field(highlightField("body", 1)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 1, equalTo(LONG_CHUNK));
            }
        );

        // number_of_fragments: 2
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder("body", "What is Elasticsearch?"))
                        .highlighter(new HighlightBuilder().field(highlightField("body", 2)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 2, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "body", 1, 2, equalTo(SHORT_CHUNK));
            }
        );

        // order: score, number_of_fragments: 1
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder("body", "What is Elasticsearch?"))
                        .highlighter(new HighlightBuilder().field(highlightField("body", 1)).order("score"))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 1, equalTo(LONG_CHUNK));
            }
        );

        // order: score, number_of_fragments: 2
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder("body", "What is Elasticsearch?"))
                        .highlighter(new HighlightBuilder().field(highlightField("body", 2)).order("score"))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 2, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "body", 1, 2, equalTo(SHORT_CHUNK));
            }
        );
    }

    /**
     * Ported from "Highlighting using a dense embedding model" in 90_semantic_text_highlighter_bwc.yml.
     * Verifies fragment count limiting and score-ordered highlighting for dense fields.
     */
    public void testLegacyFormatHighlightingDense() throws Exception {
        String idx = indexName + "-dense";
        createDenseHighlightIndex(idx);

        Map<String, Object> source = new HashMap<>();
        source.put("body", new String[] { LONG_CHUNK, SHORT_CHUNK });
        client().prepareIndex(idx).setId("doc_1").setSource(source).get();
        client().admin().indices().prepareRefresh(idx).get();

        // number_of_fragments: 1 — dense cosine similarity ranks SHORT_CHUNK highest
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder("body", "What is Elasticsearch?"))
                        .highlighter(new HighlightBuilder().field(highlightField("body", 1)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 1, equalTo(SHORT_CHUNK));
            }
        );

        // number_of_fragments: 2 — document order
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder("body", "What is Elasticsearch?"))
                        .highlighter(new HighlightBuilder().field(highlightField("body", 2)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 2, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "body", 1, 2, equalTo(SHORT_CHUNK));
            }
        );

        // order: score, number_of_fragments: 1
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder("body", "What is Elasticsearch?"))
                        .highlighter(new HighlightBuilder().field(highlightField("body", 1)).order("score"))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 1, equalTo(SHORT_CHUNK));
            }
        );

        // order: score, number_of_fragments: 2 — score order puts SHORT_CHUNK first
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder("body", "What is Elasticsearch?"))
                        .highlighter(new HighlightBuilder().field(highlightField("body", 2)).order("score"))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 2, equalTo(SHORT_CHUNK));
                assertHighlight(response, 0, "body", 1, 2, equalTo(LONG_CHUNK));
            }
        );
    }

    /**
     * Ported from "Highlighting empty field" in 90_semantic_text_highlighter_bwc.yml.
     * Verifies that a field with no indexed data produces no highlights.
     */
    public void testLegacyFormatHighlightingEmptyField() throws Exception {
        String idx = indexName + "-dense";
        createDenseHighlightIndex(idx);

        Map<String, Object> source = new HashMap<>();
        source.put("body", new String[] { LONG_CHUNK, SHORT_CHUNK });
        client().prepareIndex(idx).setId("doc_1").setSource(source).get();
        client().admin().indices().prepareRefresh(idx).get();

        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                        .highlighter(new HighlightBuilder().field(new HighlightBuilder.Field("another_body")))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertNotHighlighted(response, 0, "another_body");
            }
        );
    }

    /**
     * Ported from "Highlighting and multi chunks with empty input" in 90_semantic_text_highlighter_bwc.yml.
     * Verifies that whitespace-only values produce no chunks and are excluded from highlights.
     */
    public void testLegacyFormatHighlightingMultiChunksWithEmptyInput() throws Exception {
        String idx = indexName + "-mce";
        assertAcked(
            prepareCreate(idx).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_text_field")
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        Map<String, Object> source = new HashMap<>();
        source.put("semantic_text_field", new String[] { "some test data", "    ", "now with chunks" });
        client().prepareIndex(idx).setId("doc_1").setSource(source).get();
        client().admin().indices().prepareRefresh(idx).get();

        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder("semantic_text_field", "test"))
                        .highlighter(new HighlightBuilder().field(highlightField("semantic_text_field", 3)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "semantic_text_field", 0, 2, equalTo("some test data"));
                assertHighlight(response, 0, "semantic_text_field", 1, 2, equalTo("now with chunks"));
            }
        );
    }

    /**
     * Ported from "Highlighting with match_all query" in 90_semantic_text_highlighter_bwc.yml.
     * Verifies match_all highlighting for both sparse and dense legacy indices.
     */
    public void testLegacyFormatHighlightingWithMatchAll() throws Exception {
        String sparseIdx = indexName + "-mas";
        String denseIdx = indexName + "-mad";
        createSparseHighlightIndex(sparseIdx);
        createDenseHighlightIndex(denseIdx);

        Map<String, Object> source = new HashMap<>();
        source.put("body", new String[] { LONG_CHUNK, SHORT_CHUNK });
        client().prepareIndex(sparseIdx).setId("doc_1").setSource(source).get();
        client().admin().indices().prepareRefresh(sparseIdx).get();
        client().prepareIndex(denseIdx).setId("doc_1").setSource(source).get();
        client().admin().indices().prepareRefresh(denseIdx).get();

        // sparse match_all
        assertResponse(
            client().search(
                new SearchRequest(sparseIdx).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                        .highlighter(new HighlightBuilder().field(highlightField("body", 2)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 2, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "body", 1, 2, equalTo(SHORT_CHUNK));
            }
        );

        // dense match_all
        assertResponse(
            client().search(
                new SearchRequest(denseIdx).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                        .highlighter(new HighlightBuilder().field(highlightField("body", 2)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 2, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "body", 1, 2, equalTo(SHORT_CHUNK));
            }
        );
    }

    /**
     * Ported from "Highlighting with match_all and multi chunks with empty input" in 90_semantic_text_highlighter_bwc.yml.
     * Verifies that match_all highlighting skips whitespace-only chunks for sparse and dense indices.
     */
    public void testLegacyFormatHighlightingMatchAllMultiChunksWithEmptyInput() throws Exception {
        String sparseIdx = indexName + "-mams";
        String denseIdx = indexName + "-mamd";

        assertAcked(
            prepareCreate(sparseIdx).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_text_field")
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .endObject()
                        .startObject("text_field")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        assertAcked(
            prepareCreate(denseIdx).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_text_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .endObject()
                        .startObject("text_field")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        Map<String, Object> source = new HashMap<>();
        source.put("semantic_text_field", new String[] { "some test data", "    ", "now with chunks" });
        source.put("text_field", "some test data");

        client().prepareIndex(sparseIdx).setId("doc_1").setSource(source).get();
        client().admin().indices().prepareRefresh(sparseIdx).get();
        client().prepareIndex(denseIdx).setId("doc_1").setSource(source).get();
        client().admin().indices().prepareRefresh(denseIdx).get();

        // sparse match_all with empty input
        assertResponse(
            client().search(
                new SearchRequest(sparseIdx).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                        .highlighter(new HighlightBuilder().field(highlightField("semantic_text_field", 2)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "semantic_text_field", 0, 2, equalTo("some test data"));
                assertHighlight(response, 0, "semantic_text_field", 1, 2, equalTo("now with chunks"));
            }
        );

        // dense match_all with empty input
        assertResponse(
            client().search(
                new SearchRequest(denseIdx).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                        .highlighter(new HighlightBuilder().field(highlightField("semantic_text_field", 2)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "semantic_text_field", 0, 2, equalTo("some test data"));
                assertHighlight(response, 0, "semantic_text_field", 1, 2, equalTo("now with chunks"));
            }
        );
    }

    /**
     * Ported from "Highlighting with flat quantization index options" in 90_semantic_text_highlighter_bwc.yml.
     * Verifies that the semantic highlighter works correctly for flat, int4_flat, int8_flat, and bbq_flat index types.
     */
    public void testLegacyFormatHighlightingFlatIndexOptions() throws Exception {
        String idx = indexName + "-flat";
        assertAcked(
            prepareCreate(idx).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("flat_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "flat")
                        .endObject()
                        .endObject()
                        .endObject()
                        .startObject("int4_flat_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int4_flat")
                        .endObject()
                        .endObject()
                        .endObject()
                        .startObject("int8_flat_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int8_flat")
                        .endObject()
                        .endObject()
                        .endObject()
                        .startObject("bbq_flat_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_BBQ_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "bbq_flat")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        Map<String, Object> source = new HashMap<>();
        source.put("flat_field", new String[] { LONG_CHUNK, SHORT_CHUNK });
        source.put("int4_flat_field", new String[] { LONG_CHUNK, SHORT_CHUNK });
        source.put("int8_flat_field", new String[] { LONG_CHUNK, SHORT_CHUNK });
        source.put("bbq_flat_field", new String[] { LONG_CHUNK, SHORT_CHUNK });
        client().prepareIndex(idx).setId("doc_1").setSource(source).get();
        client().admin().indices().prepareRefresh(idx).get();

        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                        .highlighter(
                            new HighlightBuilder().field(highlightField("flat_field", 1))
                                .field(highlightField("int4_flat_field", 1))
                                .field(highlightField("int8_flat_field", 1))
                                .field(highlightField("bbq_flat_field", 1))
                        )
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertThat(response.getHits().getHits()[0].getHighlightFields().size(), equalTo(4));
                assertHighlight(response, 0, "flat_field", 0, 1, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "int4_flat_field", 0, 1, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "int8_flat_field", 0, 1, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "bbq_flat_field", 0, 1, equalTo(LONG_CHUNK));
            }
        );
    }

    /**
     * Ported from "Highlighting with HNSW quantization index options" in 90_semantic_text_highlighter_bwc.yml.
     * Verifies that the semantic highlighter works correctly for hnsw, int4_hnsw, int8_hnsw, and bbq_hnsw index types.
     */
    public void testLegacyFormatHighlightingHnswIndexOptions() throws Exception {
        String idx = indexName + "-hnsw";
        assertAcked(
            prepareCreate(idx).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("hnsw_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "hnsw")
                        .endObject()
                        .endObject()
                        .endObject()
                        .startObject("int4_hnsw_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int4_hnsw")
                        .endObject()
                        .endObject()
                        .endObject()
                        .startObject("int8_hnsw_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int8_hnsw")
                        .endObject()
                        .endObject()
                        .endObject()
                        .startObject("bbq_hnsw_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_BBQ_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "bbq_hnsw")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        Map<String, Object> source = new HashMap<>();
        source.put("hnsw_field", new String[] { LONG_CHUNK, SHORT_CHUNK });
        source.put("int4_hnsw_field", new String[] { LONG_CHUNK, SHORT_CHUNK });
        source.put("int8_hnsw_field", new String[] { LONG_CHUNK, SHORT_CHUNK });
        source.put("bbq_hnsw_field", new String[] { LONG_CHUNK, SHORT_CHUNK });
        client().prepareIndex(idx).setId("doc_1").setSource(source).get();
        client().admin().indices().prepareRefresh(idx).get();

        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                        .highlighter(
                            new HighlightBuilder().field(highlightField("hnsw_field", 1))
                                .field(highlightField("int4_hnsw_field", 1))
                                .field(highlightField("int8_hnsw_field", 1))
                                .field(highlightField("bbq_hnsw_field", 1))
                        )
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertThat(response.getHits().getHits()[0].getHighlightFields().size(), equalTo(4));
                assertHighlight(response, 0, "hnsw_field", 0, 1, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "int4_hnsw_field", 0, 1, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "int8_hnsw_field", 0, 1, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "bbq_hnsw_field", 0, 1, equalTo(LONG_CHUNK));
            }
        );
    }

    /**
     * Ported from "Highlighting with knn with similarity" in 90_semantic_text_highlighter_bwc.yml.
     * Verifies that knn similarity filtering and highlight fragment ordering work correctly.
     */
    public void testLegacyFormatHighlightingKnnWithSimilarity() throws Exception {
        String idx = indexName + "-dense";
        createDenseHighlightIndex(idx);

        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("body", new String[] { LONG_CHUNK, SHORT_CHUNK, LONG_CHUNK_2 });
        client().prepareIndex(idx).setId("doc_1").setSource(doc1).get();

        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("body", DOC_2_TEXT);
        client().prepareIndex(idx).setId("doc_2").setSource(doc2).get();
        client().admin().indices().prepareRefresh(idx).get();

        // match_all, numFragments=1, sorted by _id for deterministic ordering
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                        .highlighter(new HighlightBuilder().field(highlightField("body", 1)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 2L);
                assertHighlight(response, 0, "body", 0, 1, equalTo(LONG_CHUNK));
                assertHighlight(response, 1, "body", 0, 1, equalTo(DOC_2_TEXT));
            }
        );

        // knn with similarity threshold, numFragments=3
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(
                        new KnnVectorQueryBuilder(
                            "body",
                            new TextEmbeddingQueryVectorBuilder(null, "What is Elasticsearch?"),
                            10,
                            10,
                            null,
                            0.9977f
                        )
                    ).highlighter(new HighlightBuilder().field(highlightField("body", 3))).trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "body", 0, 3, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "body", 1, 3, equalTo(SHORT_CHUNK));
                assertHighlight(response, 0, "body", 2, 3, equalTo(LONG_CHUNK_2));
            }
        );
    }

    /**
     * Ported from "Highlighting with type:bbq_disk index options" in 90_semantic_text_highlighter_bwc.yml.
     * Verifies highlighting for the bbq_disk index type with and without similarity thresholds.
     */
    public void testLegacyFormatHighlightingBbqDiskIndexOptions() throws Exception {
        String idx = indexName + "-bbqdisk";
        assertAcked(
            prepareCreate(idx).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("bbq_disk_body")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_BBQ_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "bbq_disk")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("bbq_disk_body", new String[] { LONG_CHUNK, SHORT_CHUNK, LONG_CHUNK_2 });
        client().prepareIndex(idx).setId("doc_1").setSource(doc1).get();

        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("bbq_disk_body", DOC_2_TEXT);
        client().prepareIndex(idx).setId("doc_2").setSource(doc2).get();
        client().admin().indices().prepareRefresh(idx).get();

        // match_all, numFragments=1
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                        .highlighter(new HighlightBuilder().field(highlightField("bbq_disk_body", 1)))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 2L);
                assertHighlight(response, 0, "bbq_disk_body", 0, 1, equalTo(LONG_CHUNK));
                assertHighlight(response, 1, "bbq_disk_body", 0, 1, equalTo(DOC_2_TEXT));
            }
        );

        // knn with similarity threshold, numFragments=3
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(
                        new KnnVectorQueryBuilder(
                            "bbq_disk_body",
                            new TextEmbeddingQueryVectorBuilder(null, "What is Elasticsearch?"),
                            10,
                            10,
                            null,
                            0.9975f
                        )
                    ).highlighter(new HighlightBuilder().field(highlightField("bbq_disk_body", 3))).trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, "bbq_disk_body", 0, 3, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "bbq_disk_body", 1, 3, equalTo(SHORT_CHUNK));
                assertHighlight(response, 0, "bbq_disk_body", 2, 3, equalTo(LONG_CHUNK_2));
            }
        );

        // knn without similarity, numFragments=3
        assertResponse(
            client().search(
                new SearchRequest(idx).source(
                    new SearchSourceBuilder().query(
                        new KnnVectorQueryBuilder(
                            "bbq_disk_body",
                            new TextEmbeddingQueryVectorBuilder(null, "What is Elasticsearch?"),
                            10,
                            10,
                            null,
                            null
                        )
                    ).highlighter(new HighlightBuilder().field(highlightField("bbq_disk_body", 3))).trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 2L);
                assertHighlight(response, 0, "bbq_disk_body", 0, 3, equalTo(LONG_CHUNK));
                assertHighlight(response, 0, "bbq_disk_body", 1, 3, equalTo(SHORT_CHUNK));
                assertHighlight(response, 0, "bbq_disk_body", 2, 3, equalTo(LONG_CHUNK_2));
                assertHighlight(response, 1, "bbq_disk_body", 0, 1, equalTo(DOC_2_TEXT));
            }
        );
    }
}
