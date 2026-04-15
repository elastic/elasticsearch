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
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests ported from {@code 60_semantic_text_inference_update_bwc.yml}, covering
 * document updates (non-semantic field, semantic field re-inference, and explicit null) for
 * legacy-format semantic_text indices.
 */
public class SemanticTextInferenceUpdateLegacyFormatIT extends SemanticTextLegacyFormatTestCase {

    /**
     * Indexes a document, captures the embeddings, updates an unrelated text field, and asserts
     * that the embeddings for the semantic field are unchanged.
     */
    public void testLegacyFormatNonSemanticFieldUpdatePreservesEmbeddings() throws Exception {
        assertAcked(
            prepareCreate(indexName).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(SPARSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .endObject()
                        .startObject("non_inference_field")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        Map<String, Object> source = Map.of(SPARSE_FIELD, "original text", "non_inference_field", "original non-semantic");
        String docId = client().prepareIndex(indexName).setSource(source).get().getId();

        Object originalEmbeddings = getEmbeddingsFromFirstChunk(indexName, docId, SPARSE_FIELD);
        assertThat(originalEmbeddings, notNullValue());

        // Update only the non-inference field
        client().prepareUpdate(indexName, docId).setDoc(Map.of("non_inference_field", "updated non-semantic")).get();

        Object updatedEmbeddings = getEmbeddingsFromFirstChunk(indexName, docId, SPARSE_FIELD);
        assertThat(updatedEmbeddings, equalTo(originalEmbeddings));
    }

    /**
     * Indexes a document, then updates the semantic field with new text. Verifies that the stored
     * text value reflects the new input.
     */
    public void testLegacyFormatSemanticFieldUpdateRecalculatesEmbeddings() throws Exception {
        createLegacyIndex();

        String originalText = "original semantic text";
        String docId = client().prepareIndex(indexName)
            .setSource(Map.of(SPARSE_FIELD, originalText, DENSE_FIELD, originalText))
            .get()
            .getId();

        // Verify original text is stored
        GetResponse before = client().prepareGet(indexName, docId).get();
        @SuppressWarnings("unchecked")
        Map<String, Object> sparseBefore = (Map<String, Object>) before.getSourceAsMap().get(SPARSE_FIELD);
        assertThat(sparseBefore.get("text"), equalTo(originalText));

        // Bulk-update the sparse field with new text
        String newText = "updated semantic text";
        BulkRequestBuilder bulkBuilder = client().prepareBulk().setRefreshPolicy("true");
        bulkBuilder.add(new UpdateRequestBuilder(client()).setIndex(indexName).setId(docId).setDoc(Map.of(SPARSE_FIELD, newText)));
        BulkResponse bulkResponse = bulkBuilder.get();
        assertFalse(bulkResponse.hasFailures());

        // Verify updated text is stored
        GetResponse after = client().prepareGet(indexName, docId).get();
        @SuppressWarnings("unchecked")
        Map<String, Object> sparseAfter = (Map<String, Object>) after.getSourceAsMap().get(SPARSE_FIELD);
        assertThat(sparseAfter.get("text"), equalTo(newText));

        // Verify chunks reflect the new text
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> chunks = (List<Map<String, Object>>) ((Map<String, Object>) sparseAfter.get("inference")).get("chunks");
        assertFalse(chunks.isEmpty());
        assertThat(chunks.get(0).get("text"), equalTo(newText));
    }

    /**
     * Indexes a document, then updates the semantic field with an explicit null. Asserts that the
     * field value in source is null.
     */
    public void testLegacyFormatExplicitNullClearsInferenceResults() throws Exception {
        createLegacyIndex();

        String docId = client().prepareIndex(indexName)
            .setSource(Map.of(SPARSE_FIELD, "some text", DENSE_FIELD, "some dense text"))
            .get()
            .getId();

        // Update with null value
        Map<String, Object> nullUpdate = new HashMap<>();
        nullUpdate.put(SPARSE_FIELD, null);
        client().prepareUpdate(indexName, docId).setDoc(nullUpdate).get();

        GetResponse getResponse = client().prepareGet(indexName, docId).get();
        Map<String, Object> sourceMap = getResponse.getSourceAsMap();
        assertNull(sourceMap.get(SPARSE_FIELD));
    }
}
