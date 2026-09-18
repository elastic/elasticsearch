/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests ported from {@code 60_semantic_text_inference_update_bwc.yml}, covering
 * document updates (non-semantic field, semantic field re-inference, object source fields,
 * partial updates, script rejection, bulk operations, and explicit null) for legacy-format
 * semantic_text indices.
 */
public class SemanticTextInferenceUpdateLegacyFormatIT extends SemanticTextLegacyFormatTestCase {

    private static final String UPDATE_SCRIPT_LANG = "update_test_lang";
    private static final String ADD_NEW_FIELD_SCRIPT = "add_new_field";

    /**
     * Registers a no-op update script under the {@code update_test_lang} language that succeeds
     * and adds a {@code new_field} to the source. This allows testing the semantic_text script
     * rejection path in the regular update API without requiring the Painless engine.
     */
    public static class NoOpUpdateScriptPlugin extends MockScriptPlugin {
        @Override
        public String pluginScriptLang() {
            return UPDATE_SCRIPT_LANG;
        }

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of(ADD_NEW_FIELD_SCRIPT, vars -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                @SuppressWarnings("unchecked")
                Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                source.put("new_field", "hello");
                return ctx;
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(NoOpUpdateScriptPlugin.class);
        return plugins;
    }

    /**
     * Creates a legacy index on {@link #indexName} with {@code sparse_field}, {@code dense_field},
     * and a plain {@code non_inference_field} text field.
     */
    private void createLegacyIndexWithNonInferenceField() throws Exception {
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
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .endObject()
                        .startObject("non_inference_field")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    /**
     * Ported from "Updating non semantic_text fields does not recalculate embeddings" in {@code 60_semantic_text_inference_update_bwc.yml}.
     * Indexes a document with sparse, dense, and non-inference fields, captures the embeddings for
     * both semantic fields, updates only the non-inference field, and asserts that the text and
     * embeddings for both semantic fields are unchanged while the non-inference field is updated.
     */
    public void testLegacyFormatNonSemanticFieldUpdatePreservesEmbeddings() throws Exception {
        createLegacyIndexWithNonInferenceField();

        client().prepareIndex(indexName)
            .setId("doc_1")
            .setSource(
                Map.of(SPARSE_FIELD, "inference test", DENSE_FIELD, "another inference test", "non_inference_field", "non inference test")
            )
            .get();

        Object sparseEmbedding = getEmbeddingsFromFirstChunk(indexName, "doc_1", SPARSE_FIELD);
        assertThat(sparseEmbedding, notNullValue());
        Object denseEmbedding = getEmbeddingsFromFirstChunk(indexName, "doc_1", DENSE_FIELD);
        assertThat(denseEmbedding, notNullValue());

        client().prepareUpdate(indexName, "doc_1").setDoc(Map.of("non_inference_field", "another non inference test")).get();

        GetResponse after = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> afterMap = after.getSourceAsMap();

        @SuppressWarnings("unchecked")
        Map<String, Object> sparseField = (Map<String, Object>) afterMap.get(SPARSE_FIELD);
        assertThat(sparseField.get("text"), equalTo("inference test"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sparseChunks = (List<Map<String, Object>>) ((Map<String, Object>) sparseField.get("inference")).get(
            "chunks"
        );
        assertThat(sparseChunks.get(0).get("text"), equalTo("inference test"));
        assertThat(sparseChunks.get(0).get("embeddings"), equalTo(sparseEmbedding));

        @SuppressWarnings("unchecked")
        Map<String, Object> denseField = (Map<String, Object>) afterMap.get(DENSE_FIELD);
        assertThat(denseField.get("text"), equalTo("another inference test"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> denseChunks = (List<Map<String, Object>>) ((Map<String, Object>) denseField.get("inference")).get(
            "chunks"
        );
        assertThat(denseChunks.get(0).get("text"), equalTo("another inference test"));
        assertThat(denseChunks.get(0).get("embeddings"), equalTo(denseEmbedding));

        assertThat(afterMap.get("non_inference_field"), equalTo("another non inference test"));
    }

    /**
     * Ported from "Updating semantic_text fields recalculates embeddings" in {@code 60_semantic_text_inference_update_bwc.yml}.
     * Indexes a document, then verifies that bulk and regular updates to semantic fields produce
     * updated text and chunk values for both sparse and dense fields across multiple update rounds.
     */
    public void testLegacyFormatSemanticFieldUpdateRecalculatesEmbeddings() throws Exception {
        createLegacyIndexWithNonInferenceField();

        client().prepareIndex(indexName)
            .setId("doc_1")
            .setSource(
                Map.of(SPARSE_FIELD, "inference test", DENSE_FIELD, "another inference test", "non_inference_field", "non inference test")
            )
            .get();

        GetResponse initial = client().prepareGet(indexName, "doc_1").get();
        assertLegacyFieldStructure(initial.getSourceAsMap(), SPARSE_FIELD, "inference test");
        assertLegacyFieldStructure(initial.getSourceAsMap(), DENSE_FIELD, "another inference test");
        assertThat(initial.getSourceAsMap().get("non_inference_field"), equalTo("non inference test"));

        // First bulk update
        BulkResponse bulkResponse1 = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(
                new UpdateRequestBuilder(client()).setIndex(indexName)
                    .setId("doc_1")
                    .setDoc(Map.of(SPARSE_FIELD, "I am a test", DENSE_FIELD, "I am a teapot"))
            )
            .get();
        assertFalse(bulkResponse1.hasFailures());

        GetResponse afterBulk1 = client().prepareGet(indexName, "doc_1").get();
        assertLegacyFieldStructure(afterBulk1.getSourceAsMap(), SPARSE_FIELD, "I am a test");
        assertLegacyFieldStructure(afterBulk1.getSourceAsMap(), DENSE_FIELD, "I am a teapot");
        assertThat(afterBulk1.getSourceAsMap().get("non_inference_field"), equalTo("non inference test"));

        // Regular update
        client().prepareUpdate(indexName, "doc_1")
            .setDoc(Map.of(SPARSE_FIELD, "updated inference test", DENSE_FIELD, "another updated inference test"))
            .get();

        GetResponse afterUpdate = client().prepareGet(indexName, "doc_1").get();
        assertLegacyFieldStructure(afterUpdate.getSourceAsMap(), SPARSE_FIELD, "updated inference test");
        assertLegacyFieldStructure(afterUpdate.getSourceAsMap(), DENSE_FIELD, "another updated inference test");
        assertThat(afterUpdate.getSourceAsMap().get("non_inference_field"), equalTo("non inference test"));

        // Second bulk update
        BulkResponse bulkResponse2 = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(
                new UpdateRequestBuilder(client()).setIndex(indexName)
                    .setId("doc_1")
                    .setDoc(Map.of(SPARSE_FIELD, "bulk inference test", DENSE_FIELD, "bulk updated inference test"))
            )
            .get();
        assertFalse(bulkResponse2.hasFailures());

        GetResponse afterBulk2 = client().prepareGet(indexName, "doc_1").get();
        assertLegacyFieldStructure(afterBulk2.getSourceAsMap(), SPARSE_FIELD, "bulk inference test");
        assertLegacyFieldStructure(afterBulk2.getSourceAsMap(), DENSE_FIELD, "bulk updated inference test");
        assertThat(afterBulk2.getSourceAsMap().get("non_inference_field"), equalTo("non inference test"));
    }

    /**
     * Ported from "Update logic handles source fields in object fields" in {@code 60_semantic_text_inference_update_bwc.yml}.
     * Creates an index with {@code object_source} containing sub-fields that copy to the top-level
     * semantic_text fields. Verifies that bulk and regular updates including object-path keys
     * produce multiple inference chunks for each field.
     */
    public void testLegacyFormatUpdateLogicHandlesSourceFieldsInObjectFields() throws Exception {
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
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .endObject()
                        .startObject("object_source")
                        .startObject("properties")
                        .startObject(SPARSE_FIELD)
                        .field("type", "text")
                        .field("copy_to", SPARSE_FIELD)
                        .endObject()
                        .startObject(DENSE_FIELD)
                        .field("type", "text")
                        .field("copy_to", DENSE_FIELD)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        client().prepareIndex(indexName).setId("doc_1").setSource(Map.of(SPARSE_FIELD, "sparse data 1", DENSE_FIELD, "dense data 1")).get();

        GetResponse initial = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> initialMap = initial.getSourceAsMap();
        assertThat(getChunks(initialMap, SPARSE_FIELD).size(), equalTo(1));
        assertThat(getChunks(initialMap, SPARSE_FIELD).get(0).get("text"), equalTo("sparse data 1"));
        assertThat(getChunks(initialMap, DENSE_FIELD).size(), equalTo(1));
        assertThat(getChunks(initialMap, DENSE_FIELD).get(0).get("text"), equalTo("dense data 1"));

        // Bulk update using dot-notation keys for object sub-fields
        Map<String, Object> bulkDoc1 = new LinkedHashMap<>();
        bulkDoc1.put(SPARSE_FIELD, "sparse data 1");
        bulkDoc1.put("object_source." + SPARSE_FIELD, "sparse data 2");
        bulkDoc1.put(DENSE_FIELD, "dense data 1");
        bulkDoc1.put("object_source." + DENSE_FIELD, "dense data 2");

        BulkResponse bulkResponse = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(new UpdateRequestBuilder(client()).setIndex(indexName).setId("doc_1").setDoc(bulkDoc1))
            .get();
        assertFalse(bulkResponse.hasFailures());

        GetResponse afterBulk = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> afterBulkMap = afterBulk.getSourceAsMap();

        @SuppressWarnings("unchecked")
        Map<String, Object> sparseBulk = (Map<String, Object>) afterBulkMap.get(SPARSE_FIELD);
        assertThat(sparseBulk.get("text"), equalTo("sparse data 1"));
        List<Map<String, Object>> sparseChunksBulk = getChunks(afterBulkMap, SPARSE_FIELD);
        assertThat(sparseChunksBulk.size(), equalTo(2));
        assertThat(sparseChunksBulk.get(0).get("text"), equalTo("sparse data 2"));
        assertThat(sparseChunksBulk.get(1).get("text"), equalTo("sparse data 1"));

        @SuppressWarnings("unchecked")
        Map<String, Object> denseBulk = (Map<String, Object>) afterBulkMap.get(DENSE_FIELD);
        assertThat(denseBulk.get("text"), equalTo("dense data 1"));
        List<Map<String, Object>> denseChunksBulk = getChunks(afterBulkMap, DENSE_FIELD);
        assertThat(denseChunksBulk.size(), equalTo(2));
        assertThat(denseChunksBulk.get(0).get("text"), equalTo("dense data 1"));
        assertThat(denseChunksBulk.get(1).get("text"), equalTo("dense data 2"));

        // Regular update using dot-notation keys for object sub-fields
        Map<String, Object> updateDoc = new LinkedHashMap<>();
        updateDoc.put(SPARSE_FIELD, "sparse data 1");
        updateDoc.put("object_source." + SPARSE_FIELD, "sparse data 3");
        updateDoc.put(DENSE_FIELD, "dense data 1");
        updateDoc.put("object_source." + DENSE_FIELD, "dense data 3");

        client().prepareUpdate(indexName, "doc_1").setDoc(updateDoc).get();

        GetResponse afterUpdate = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> afterUpdateMap = afterUpdate.getSourceAsMap();

        @SuppressWarnings("unchecked")
        Map<String, Object> sparseUpdate = (Map<String, Object>) afterUpdateMap.get(SPARSE_FIELD);
        assertThat(sparseUpdate.get("text"), equalTo("sparse data 1"));
        List<Map<String, Object>> sparseChunksUpdate = getChunks(afterUpdateMap, SPARSE_FIELD);
        assertThat(sparseChunksUpdate.size(), equalTo(2));
        assertThat(sparseChunksUpdate.get(0).get("text"), equalTo("sparse data 3"));
        assertThat(sparseChunksUpdate.get(1).get("text"), equalTo("sparse data 1"));

        @SuppressWarnings("unchecked")
        Map<String, Object> denseUpdate = (Map<String, Object>) afterUpdateMap.get(DENSE_FIELD);
        assertThat(denseUpdate.get("text"), equalTo("dense data 1"));
        List<Map<String, Object>> denseChunksUpdate = getChunks(afterUpdateMap, DENSE_FIELD);
        assertThat(denseChunksUpdate.size(), equalTo(2));
        assertThat(denseChunksUpdate.get(0).get("text"), equalTo("dense data 1"));
        assertThat(denseChunksUpdate.get(1).get("text"), equalTo("dense data 3"));
    }

    /**
     * Ported from "Partial updates work when using the update API" in {@code 60_semantic_text_inference_update_bwc.yml}.
     * Creates an index where plain text fields copy their values into semantic_text fields. Verifies
     * that a partial update using only the source fields recalculates the copy-to chunks while
     * preserving the direct field values.
     */
    public void testLegacyFormatPartialUpdatesWorkWithUpdateApi() throws Exception {
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
                        .startObject("sparse_source_field")
                        .field("type", "text")
                        .field("copy_to", SPARSE_FIELD)
                        .endObject()
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .endObject()
                        .startObject("dense_source_field")
                        .field("type", "text")
                        .field("copy_to", DENSE_FIELD)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        client().prepareIndex(indexName)
            .setId("doc_1")
            .setSource(
                Map.of(
                    SPARSE_FIELD,
                    "sparse data 1",
                    "sparse_source_field",
                    "sparse data 2",
                    DENSE_FIELD,
                    "dense data 1",
                    "dense_source_field",
                    "dense data 2"
                )
            )
            .get();

        GetResponse initial = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> initialMap = initial.getSourceAsMap();
        List<Map<String, Object>> sparseInitialChunks = getChunks(initialMap, SPARSE_FIELD);
        assertThat(sparseInitialChunks.size(), equalTo(2));
        assertThat(sparseInitialChunks.get(1).get("text"), equalTo("sparse data 2"));
        assertThat(sparseInitialChunks.get(1).get("embeddings"), notNullValue());
        List<Map<String, Object>> denseInitialChunks = getChunks(initialMap, DENSE_FIELD);
        assertThat(denseInitialChunks.size(), equalTo(2));
        assertThat(denseInitialChunks.get(1).get("text"), equalTo("dense data 2"));
        assertThat(denseInitialChunks.get(1).get("embeddings"), notNullValue());

        // Partial update: update only source fields
        client().prepareUpdate(indexName, "doc_1")
            .setDoc(Map.of("sparse_source_field", "sparse data 3", "dense_source_field", "dense data 3"))
            .get();

        GetResponse afterUpdate = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> afterMap = afterUpdate.getSourceAsMap();

        @SuppressWarnings("unchecked")
        Map<String, Object> sparseField = (Map<String, Object>) afterMap.get(SPARSE_FIELD);
        assertThat(sparseField.get("text"), equalTo("sparse data 1"));
        List<Map<String, Object>> sparseChunks = getChunks(afterMap, SPARSE_FIELD);
        assertThat(sparseChunks.size(), equalTo(2));
        assertThat(sparseChunks.get(1).get("text"), equalTo("sparse data 3"));
        assertThat(sparseChunks.get(1).get("embeddings"), notNullValue());

        @SuppressWarnings("unchecked")
        Map<String, Object> denseField = (Map<String, Object>) afterMap.get(DENSE_FIELD);
        assertThat(denseField.get("text"), equalTo("dense data 1"));
        List<Map<String, Object>> denseChunks = getChunks(afterMap, DENSE_FIELD);
        assertThat(denseChunks.size(), equalTo(2));
        assertThat(denseChunks.get(1).get("text"), equalTo("dense data 3"));
        assertThat(denseChunks.get(1).get("embeddings"), notNullValue());
    }

    /**
     * Ported from "Partial updates work when using the update API and the semantic_text field's original value is null" in
     * {@code 60_semantic_text_inference_update_bwc.yml}.
     * Indexes a document without setting the direct semantic_text field values (so their
     * {@code text} is null), then verifies that a partial update via source fields still
     * recalculates the chunks while keeping {@code text} null.
     */
    public void testLegacyFormatPartialUpdatesWorkWhenSemanticFieldOriginalValueIsNull() throws Exception {
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
                        .startObject("sparse_source_field")
                        .field("type", "text")
                        .field("copy_to", SPARSE_FIELD)
                        .endObject()
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .endObject()
                        .startObject("dense_source_field")
                        .field("type", "text")
                        .field("copy_to", DENSE_FIELD)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        // Only set source fields; sparse_field and dense_field direct values are not provided
        client().prepareIndex(indexName)
            .setId("doc_1")
            .setSource(Map.of("sparse_source_field", "sparse data 2", "dense_source_field", "dense data 2"))
            .get();

        GetResponse initial = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> initialMap = initial.getSourceAsMap();

        @SuppressWarnings("unchecked")
        Map<String, Object> sparseInitial = (Map<String, Object>) initialMap.get(SPARSE_FIELD);
        assertNull(sparseInitial.get("text"));
        List<Map<String, Object>> sparseInitialChunks = getChunks(initialMap, SPARSE_FIELD);
        assertThat(sparseInitialChunks.size(), equalTo(1));
        assertThat(sparseInitialChunks.get(0).get("text"), equalTo("sparse data 2"));
        assertThat(sparseInitialChunks.get(0).get("embeddings"), notNullValue());

        @SuppressWarnings("unchecked")
        Map<String, Object> denseInitial = (Map<String, Object>) initialMap.get(DENSE_FIELD);
        assertNull(denseInitial.get("text"));
        List<Map<String, Object>> denseInitialChunks = getChunks(initialMap, DENSE_FIELD);
        assertThat(denseInitialChunks.size(), equalTo(1));
        assertThat(denseInitialChunks.get(0).get("text"), equalTo("dense data 2"));
        assertThat(denseInitialChunks.get(0).get("embeddings"), notNullValue());

        client().prepareUpdate(indexName, "doc_1")
            .setDoc(Map.of("sparse_source_field", "sparse data 3", "dense_source_field", "dense data 3"))
            .get();

        GetResponse afterUpdate = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> afterMap = afterUpdate.getSourceAsMap();

        @SuppressWarnings("unchecked")
        Map<String, Object> sparseAfter = (Map<String, Object>) afterMap.get(SPARSE_FIELD);
        assertNull(sparseAfter.get("text"));
        List<Map<String, Object>> sparseAfterChunks = getChunks(afterMap, SPARSE_FIELD);
        assertThat(sparseAfterChunks.size(), equalTo(1));
        assertThat(sparseAfterChunks.get(0).get("text"), equalTo("sparse data 3"));
        assertThat(sparseAfterChunks.get(0).get("embeddings"), notNullValue());

        @SuppressWarnings("unchecked")
        Map<String, Object> denseAfter = (Map<String, Object>) afterMap.get(DENSE_FIELD);
        assertNull(denseAfter.get("text"));
        List<Map<String, Object>> denseAfterChunks = getChunks(afterMap, DENSE_FIELD);
        assertThat(denseAfterChunks.size(), equalTo(1));
        assertThat(denseAfterChunks.get(0).get("text"), equalTo("dense data 3"));
        assertThat(denseAfterChunks.get(0).get("embeddings"), notNullValue());
    }

    /**
     * Ported from "Updates with script are not allowed" in {@code 60_semantic_text_inference_update_bwc.yml}.
     * Verifies that both bulk and regular script updates are rejected with a 400 error on an
     * index that contains semantic_text fields.
     */
    public void testLegacyFormatUpdatesWithScriptAreNotAllowed() throws Exception {
        createLegacyIndexWithNonInferenceField();

        // Bulk index a doc
        BulkResponse bulkIndex = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(
                new UpdateRequestBuilder(client()).setIndex(indexName)
                    .setId("doc_1")
                    .setDoc(Map.of(SPARSE_FIELD, "I am a test", DENSE_FIELD, "I am a teapot", "non_inference_field", "non inference test"))
                    .setDocAsUpsert(true)
            )
            .get();
        assertFalse(bulkIndex.hasFailures());

        // Bulk script update must fail — ShardBulkInferenceActionFilter rejects before execution
        Script bulkScript = new Script(ScriptType.INLINE, UPDATE_SCRIPT_LANG, ADD_NEW_FIELD_SCRIPT, Map.of());
        BulkResponse bulkScriptResponse = client().prepareBulk()
            .add(new UpdateRequestBuilder(client()).setIndex(indexName).setId("doc_1").setScript(bulkScript).setScriptedUpsert(true))
            .get();
        assertTrue(bulkScriptResponse.hasFailures());
        BulkItemResponse.Failure failure = bulkScriptResponse.getItems()[0].getFailure();
        assertThat(failure.getStatus(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            failure.getCause().getMessage(),
            containsString("Cannot apply update with a script on indices that contain [semantic_text] field(s)")
        );

        // Regular script update must fail — script succeeds via mock engine, then inference check throws
        Script regularScript = new Script(ScriptType.INLINE, UPDATE_SCRIPT_LANG, ADD_NEW_FIELD_SCRIPT, Map.of());
        ElasticsearchStatusException regularException = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().prepareUpdate(indexName, "doc_1").setScript(regularScript).get()
        );
        assertThat(
            regularException.getMessage(),
            containsString("Cannot apply update with a script on indices that contain inference field(s)")
        );
    }

    /**
     * Ported from "semantic_text copy_to needs values for every source field for bulk updates" in
     * {@code 60_semantic_text_inference_update_bwc.yml}.
     * Verifies that when a semantic_text field has multiple copy_to source fields, a bulk update
     * that omits one of the source fields is rejected with a 400 error.
     */
    public void testLegacyFormatCopyToNeedsValuesForEverySourceFieldInBulkUpdates() throws Exception {
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
                        .startObject("source_field")
                        .field("type", "text")
                        .field("copy_to", SPARSE_FIELD)
                        .endObject()
                        .startObject("another_source_field")
                        .field("type", "text")
                        .field("copy_to", SPARSE_FIELD)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        // Creation with only source_field + sparse_field (no another_source_field) succeeds
        client().prepareIndex(indexName)
            .setId("doc_1")
            .setSource(Map.of("source_field", "a single source field provided", SPARSE_FIELD, "inference test"))
            .get();

        // Bulk update omitting another_source_field must fail
        BulkResponse bulkResponse = client().prepareBulk()
            .add(
                new UpdateRequestBuilder(client()).setIndex(indexName)
                    .setId("doc_1")
                    .setDoc(
                        Map.of("source_field", "a single source field is kept as provided via bulk", SPARSE_FIELD, "updated inference test")
                    )
            )
            .get();
        assertTrue(bulkResponse.hasFailures());
        BulkItemResponse.Failure failure = bulkResponse.getItems()[0].getFailure();
        assertThat(failure.getStatus(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            failure.getCause().getMessage(),
            containsString(
                "Field [another_source_field] must be specified on an update request to calculate inference for field ["
                    + SPARSE_FIELD
                    + "]"
            )
        );
    }

    /**
     * Ported from "Calculates embeddings for bulk operations - update" in {@code 60_semantic_text_inference_update_bwc.yml}.
     * Verifies that bulk index and update operations calculate embeddings for both semantic fields
     * and that a subsequent bulk script update is rejected.
     */
    public void testLegacyFormatCalculatesEmbeddingsForBulkUpdateOperation() throws Exception {
        createLegacyIndexWithNonInferenceField();

        // Bulk index
        BulkResponse bulkIndex = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(
                new UpdateRequestBuilder(client()).setIndex(indexName)
                    .setId("doc_1")
                    .setDoc(
                        Map.of(
                            SPARSE_FIELD,
                            "inference test",
                            DENSE_FIELD,
                            "another inference test",
                            "non_inference_field",
                            "non inference test"
                        )
                    )
                    .setDocAsUpsert(true)
            )
            .get();
        assertFalse(bulkIndex.hasFailures());
        assertThat(bulkIndex.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));

        // Bulk update
        BulkResponse bulkUpdate = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(
                new UpdateRequestBuilder(client()).setIndex(indexName)
                    .setId("doc_1")
                    .setDoc(
                        Map.of(
                            SPARSE_FIELD,
                            "updated inference test",
                            DENSE_FIELD,
                            "another updated inference test",
                            "non_inference_field",
                            "updated non inference test"
                        )
                    )
            )
            .get();
        assertFalse(bulkUpdate.hasFailures());
        assertThat(bulkUpdate.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));

        GetResponse getResponse = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> sourceMap = getResponse.getSourceAsMap();
        assertLegacyFieldStructure(sourceMap, SPARSE_FIELD, "updated inference test");
        assertLegacyFieldStructure(sourceMap, DENSE_FIELD, "another updated inference test");
        assertThat(sourceMap.get("non_inference_field"), equalTo("updated non inference test"));

        // Bulk script update must fail — ShardBulkInferenceActionFilter rejects before execution
        BulkResponse bulkScript = client().prepareBulk()
            .add(
                new UpdateRequestBuilder(client()).setIndex(indexName)
                    .setId("doc_1")
                    .setScript(new Script(ScriptType.INLINE, UPDATE_SCRIPT_LANG, ADD_NEW_FIELD_SCRIPT, Map.of()))
            )
            .get();
        assertTrue(bulkScript.hasFailures());
        assertThat(bulkScript.getItems()[0].getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            bulkScript.getItems()[0].getFailure().getCause().getMessage(),
            containsString("Cannot apply update with a script on indices that contain [semantic_text] field(s)")
        );
    }

    /**
     * Ported from "Calculates embeddings for bulk operations - upsert" in {@code 60_semantic_text_inference_update_bwc.yml}.
     * Verifies that a bulk update on a non-existent document fails with 404, that doc_as_upsert
     * creates the document on the second attempt, and that a subsequent upsert updates it.
     */
    public void testLegacyFormatCalculatesEmbeddingsForBulkUpsertOperation() throws Exception {
        createLegacyIndexWithNonInferenceField();

        Map<String, Object> docFields = Map.of(
            SPARSE_FIELD,
            "inference test",
            DENSE_FIELD,
            "another inference test",
            "non_inference_field",
            "non inference test"
        );

        // Initial update (no upsert) fails because document does not exist
        BulkResponse initialUpdate = client().prepareBulk()
            .add(new UpdateRequestBuilder(client()).setIndex(indexName).setId("doc_1").setDoc(docFields))
            .get();
        assertTrue(initialUpdate.hasFailures());
        assertThat(initialUpdate.getItems()[0].getFailure().getStatus(), equalTo(RestStatus.NOT_FOUND));

        // Upsert creates the document
        BulkResponse upsertCreate = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(new UpdateRequestBuilder(client()).setIndex(indexName).setId("doc_1").setDoc(docFields).setDocAsUpsert(true))
            .get();
        assertFalse(upsertCreate.hasFailures());
        assertThat(upsertCreate.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));

        GetResponse afterCreate = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> afterCreateMap = afterCreate.getSourceAsMap();
        assertLegacyFieldStructure(afterCreateMap, SPARSE_FIELD, "inference test");
        assertLegacyFieldStructure(afterCreateMap, DENSE_FIELD, "another inference test");
        assertThat(afterCreateMap.get("non_inference_field"), equalTo("non inference test"));

        // Upsert on existing document updates it
        Map<String, Object> updatedFields = Map.of(
            SPARSE_FIELD,
            "updated inference test",
            DENSE_FIELD,
            "another updated inference test",
            "non_inference_field",
            "updated non inference test"
        );
        BulkResponse upsertUpdate = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(new UpdateRequestBuilder(client()).setIndex(indexName).setId("doc_1").setDoc(updatedFields).setDocAsUpsert(true))
            .get();
        assertFalse(upsertUpdate.hasFailures());
        assertThat(upsertUpdate.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));

        GetResponse afterUpdate = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> afterUpdateMap = afterUpdate.getSourceAsMap();
        assertLegacyFieldStructure(afterUpdateMap, SPARSE_FIELD, "updated inference test");
        assertLegacyFieldStructure(afterUpdateMap, DENSE_FIELD, "another updated inference test");
        assertThat(afterUpdateMap.get("non_inference_field"), equalTo("updated non inference test"));
    }

    /**
     * Ported from "Bypass inference on bulk update operation" in {@code 60_semantic_text_inference_update_bwc.yml}.
     * Verifies that a bulk update containing only a non-semantic field does not recalculate or
     * overwrite the existing semantic field embeddings.
     */
    public void testLegacyFormatBypassInferenceOnBulkUpdateOperation() throws Exception {
        createLegacyIndexWithNonInferenceField();

        // Create document via upsert
        Map<String, Object> docFields = Map.of(
            SPARSE_FIELD,
            "inference test",
            DENSE_FIELD,
            "another inference test",
            "non_inference_field",
            "non inference test"
        );
        BulkResponse upsertCreate = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(new UpdateRequestBuilder(client()).setIndex(indexName).setId("doc_1").setDoc(docFields).setDocAsUpsert(true))
            .get();
        assertFalse(upsertCreate.hasFailures());
        assertThat(upsertCreate.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));

        Object sparseEmbedding = getEmbeddingsFromFirstChunk(indexName, "doc_1", SPARSE_FIELD);
        assertThat(sparseEmbedding, notNullValue());
        Object denseEmbedding = getEmbeddingsFromFirstChunk(indexName, "doc_1", DENSE_FIELD);
        assertThat(denseEmbedding, notNullValue());

        // Update only the non-inference field
        BulkResponse bypassUpdate = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(
                new UpdateRequestBuilder(client()).setIndex(indexName)
                    .setId("doc_1")
                    .setDoc(Map.of("non_inference_field", "another value"))
                    .setDocAsUpsert(true)
            )
            .get();
        assertFalse(bypassUpdate.hasFailures());
        assertThat(bypassUpdate.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));

        GetResponse afterUpdate = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> afterMap = afterUpdate.getSourceAsMap();

        @SuppressWarnings("unchecked")
        Map<String, Object> sparseField = (Map<String, Object>) afterMap.get(SPARSE_FIELD);
        assertThat(sparseField.get("text"), equalTo("inference test"));
        List<Map<String, Object>> sparseChunks = getChunks(afterMap, SPARSE_FIELD);
        assertThat(sparseChunks.get(0).get("text"), equalTo("inference test"));
        assertThat(sparseChunks.get(0).get("embeddings"), equalTo(sparseEmbedding));

        @SuppressWarnings("unchecked")
        Map<String, Object> denseField = (Map<String, Object>) afterMap.get(DENSE_FIELD);
        assertThat(denseField.get("text"), equalTo("another inference test"));
        List<Map<String, Object>> denseChunks = getChunks(afterMap, DENSE_FIELD);
        assertThat(denseChunks.get(0).get("text"), equalTo("another inference test"));
        assertThat(denseChunks.get(0).get("embeddings"), equalTo(denseEmbedding));

        assertThat(afterMap.get("non_inference_field"), equalTo("another value"));
    }

    /**
     * Ported from "Explicit nulls clear inference results on bulk update operation" in {@code 60_semantic_text_inference_update_bwc.yml}.
     * Verifies that a bulk update with explicit null values for both semantic fields clears the
     * inference results stored in the document.
     */
    public void testLegacyFormatExplicitNullClearsInferenceResults() throws Exception {
        createLegacyIndexWithNonInferenceField();

        // Create document via upsert
        Map<String, Object> docFields = Map.of(
            SPARSE_FIELD,
            "inference test",
            DENSE_FIELD,
            "another inference test",
            "non_inference_field",
            "non inference test"
        );
        BulkResponse upsertCreate = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(new UpdateRequestBuilder(client()).setIndex(indexName).setId("doc_1").setDoc(docFields).setDocAsUpsert(true))
            .get();
        assertFalse(upsertCreate.hasFailures());
        assertThat(upsertCreate.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));

        GetResponse afterCreate = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> afterCreateMap = afterCreate.getSourceAsMap();
        assertLegacyFieldStructure(afterCreateMap, SPARSE_FIELD, "inference test");
        assertLegacyFieldStructure(afterCreateMap, DENSE_FIELD, "another inference test");
        assertThat(afterCreateMap.get("non_inference_field"), equalTo("non inference test"));

        // Update with explicit null values
        Map<String, Object> nullUpdate = new HashMap<>();
        nullUpdate.put(SPARSE_FIELD, null);
        nullUpdate.put(DENSE_FIELD, null);
        nullUpdate.put("non_inference_field", "updated value");

        BulkResponse nullBulk = client().prepareBulk()
            .setRefreshPolicy("true")
            .add(new UpdateRequestBuilder(client()).setIndex(indexName).setId("doc_1").setDoc(nullUpdate).setDocAsUpsert(true))
            .get();
        assertFalse(nullBulk.hasFailures());
        assertThat(nullBulk.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));

        GetResponse afterNull = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> afterNullMap = afterNull.getSourceAsMap();
        assertNull(afterNullMap.get(SPARSE_FIELD));
        assertNull(afterNullMap.get(DENSE_FIELD));
        assertThat(afterNullMap.get("non_inference_field"), equalTo("updated value"));
    }

    /**
     * Extracts the inference chunks list for the given semantic_text field from the source map.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getChunks(Map<String, Object> sourceMap, String fieldName) {
        Map<String, Object> fieldMap = (Map<String, Object>) sourceMap.get(fieldName);
        Map<String, Object> inference = (Map<String, Object>) fieldMap.get("inference");
        return (List<Map<String, Object>>) inference.get("chunks");
    }
}
