/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests ported from {@code 30_semantic_text_inference_bwc.yml}, covering document
 * structure, input coercion, search, and bulk indexing for legacy-format semantic_text indices.
 */
public class SemanticTextInferenceLegacyFormatIT extends SemanticTextLegacyFormatTestCase {

    /**
     * Ported from "Calculates sparse embedding and text embedding results for new documents" in {@code 30_semantic_text_inference_bwc.yml}.
     * Indexes a single document and verifies that the legacy source structure is populated:
     * {@code field.text} holds the original string, and {@code field.inference.chunks[0].text}
     * plus {@code field.inference.chunks[0].embeddings} are present.
     */
    public void testLegacyFormatDocumentStructure() throws Exception {
        createLegacyIndex();

        final String inputText = "legacy format test";
        Map<String, Object> source = Map.of(SPARSE_FIELD, inputText, DENSE_FIELD, inputText);
        String docId = client().prepareIndex(indexName).setSource(source).get().getId();

        GetResponse getResponse = client().prepareGet(indexName, docId).get();
        assertTrue(getResponse.isExists());
        Map<String, Object> sourceMap = getResponse.getSourceAsMap();

        assertLegacyFieldStructure(sourceMap, SPARSE_FIELD, inputText);
        assertLegacyFieldStructure(sourceMap, DENSE_FIELD, inputText);
    }

    /**
     * Ported from "Calculates sparse embedding and text embedding results for new documents with integer value" and "...with boolean value" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that integer and boolean source values are coerced to strings in the legacy format
     * (matching the YAML test {@code 30_semantic_text_inference_bwc.yml}).
     */
    public void testLegacyFormatNumericAndBooleanInputsCoercedToString() throws Exception {
        createLegacyIndex();

        Map<String, Object> source = new HashMap<>();
        source.put(SPARSE_FIELD, 75);
        source.put(DENSE_FIELD, true);
        String docId = client().prepareIndex(indexName).setSource(source).get().getId();

        GetResponse getResponse = client().prepareGet(indexName, docId).get();
        Map<String, Object> sourceMap = getResponse.getSourceAsMap();

        @SuppressWarnings("unchecked")
        Map<String, Object> sparseField = (Map<String, Object>) sourceMap.get(SPARSE_FIELD);
        assertThat(sparseField.get("text"), equalTo("75"));

        @SuppressWarnings("unchecked")
        Map<String, Object> denseField = (Map<String, Object>) sourceMap.get(DENSE_FIELD);
        assertThat(denseField.get("text"), equalTo("true"));
    }

    /**
     * Ported from "Sparse vector results are indexed as nested chunks and searchable" in {@code 30_semantic_text_inference_bwc.yml}.
     * Bulk-indexes two documents and verifies that a {@code SemanticQueryBuilder} search on the
     * sparse field returns both hits.
     */
    public void testLegacyFormatSparseSearch() throws Exception {
        createLegacyIndex();

        BulkRequestBuilder bulkBuilder = client().prepareBulk().setRefreshPolicy("true");
        bulkBuilder.add(new IndexRequestBuilder(client()).setIndex(indexName).setSource(Map.of(SPARSE_FIELD, "first document")));
        bulkBuilder.add(new IndexRequestBuilder(client()).setIndex(indexName).setSource(Map.of(SPARSE_FIELD, "second document")));
        BulkResponse bulkResponse = bulkBuilder.get();
        assertFalse(bulkResponse.hasFailures());

        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(SPARSE_FIELD, "document")).trackTotalHits(true)
                )
            ),
            response -> assertHitCount(response, 2L)
        );
    }

    /**
     * Ported from "Dense vector results are indexed as nested chunks and searchable" in {@code 30_semantic_text_inference_bwc.yml}.
     * Bulk-indexes two documents and verifies that a {@code SemanticQueryBuilder} search on the
     * dense field returns hits.
     */
    public void testLegacyFormatDenseSearch() throws Exception {
        createLegacyIndex();

        BulkRequestBuilder bulkBuilder = client().prepareBulk().setRefreshPolicy("true");
        bulkBuilder.add(new IndexRequestBuilder(client()).setIndex(indexName).setSource(Map.of(DENSE_FIELD, "first dense document")));
        bulkBuilder.add(new IndexRequestBuilder(client()).setIndex(indexName).setSource(Map.of(DENSE_FIELD, "second dense document")));
        BulkResponse bulkResponse = bulkBuilder.get();
        assertFalse(bulkResponse.hasFailures());

        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(DENSE_FIELD, "document")).trackTotalHits(true)
                )
            ),
            response -> assertHitCount(response, 2L)
        );
    }

    /**
     * Ported from "Calculates embeddings for bulk operations - index" in {@code 30_semantic_text_inference_bwc.yml}.
     * Bulk-indexes several documents and asserts that there are no failures and the expected hit
     * count is correct.
     */
    public void testLegacyFormatBulkIndex() throws Exception {
        createLegacyIndex();

        int docCount = randomIntBetween(3, 10);
        BulkRequestBuilder bulkBuilder = client().prepareBulk().setRefreshPolicy("true");
        for (int i = 0; i < docCount; i++) {
            bulkBuilder.add(
                new IndexRequestBuilder(client()).setIndex(indexName).setSource(Map.of(SPARSE_FIELD, "doc " + i, DENSE_FIELD, "doc " + i))
            );
        }
        BulkResponse bulkResponse = bulkBuilder.get();
        assertFalse(bulkResponse.hasFailures());

        assertResponse(
            client().search(
                new SearchRequest(indexName).source(new SearchSourceBuilder().query(new SemanticQueryBuilder(SPARSE_FIELD, "doc")))
            ),
            response -> assertHitCount(response, (long) docCount)
        );
    }
}
