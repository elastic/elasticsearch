/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests ported from {@code 90_semantic_text_highlighter_bwc.yml}, covering
 * highlighting for sparse and dense semantic_text fields in legacy-format indices.
 */
public class SemanticTextHighlighterLegacyFormatIT extends SemanticTextLegacyFormatTestCase {

    /**
     * Indexes a document with an array of two strings in the sparse field, queries with
     * {@code SemanticQueryBuilder} and a {@code HighlightBuilder}, and asserts that both strings
     * appear in the highlights.
     */
    public void testLegacyFormatHighlightingSparse() throws Exception {
        createLegacyIndex();

        String[] texts = new String[] { "highlight sparse first", "highlight sparse second" };
        Map<String, Object> source = new HashMap<>();
        source.put(SPARSE_FIELD, texts);
        client().prepareIndex(indexName).setSource(source).get();
        client().admin().indices().prepareRefresh(indexName).get();

        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(SPARSE_FIELD, "highlight"))
                        .highlighter(new HighlightBuilder().field(SPARSE_FIELD))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, SPARSE_FIELD, 0, 2, equalTo(texts[0]));
                assertHighlight(response, 0, SPARSE_FIELD, 1, 2, equalTo(texts[1]));
            }
        );
    }

    /**
     * Indexes a document with an array of two strings in the dense field, queries with
     * {@code SemanticQueryBuilder} and a {@code HighlightBuilder}, and asserts that both strings
     * appear in the highlights.
     */
    public void testLegacyFormatHighlightingDense() throws Exception {
        createLegacyIndex();

        String[] texts = new String[] { "highlight dense first", "highlight dense second" };
        Map<String, Object> source = new HashMap<>();
        source.put(DENSE_FIELD, texts);
        client().prepareIndex(indexName).setSource(source).get();
        client().admin().indices().prepareRefresh(indexName).get();

        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(DENSE_FIELD, "highlight"))
                        .highlighter(new HighlightBuilder().field(DENSE_FIELD))
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                assertHighlight(response, 0, DENSE_FIELD, 0, 2, equalTo(texts[0]));
                assertHighlight(response, 0, DENSE_FIELD, 1, 2, equalTo(texts[1]));
            }
        );
    }
}
