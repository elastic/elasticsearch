/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.inference.Utils.storeSparseModel;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests ported from {@code 30_semantic_text_inference_bwc.yml}, covering document
 * structure, input coercion, search, and bulk indexing for legacy-format semantic_text indices.
 */
public class SemanticTextInferenceLegacyFormatIT extends SemanticTextLegacyFormatTestCase {

    private static final String UBQ_SCRIPT_LANG = "ubq_test_lang";
    private static final String UPDATE_SEMANTIC_FIELDS_SCRIPT = "update_semantic_fields";

    /**
     * Registers a mock script that sets {@code sparse_field} and {@code dense_field} to new
     * string values, simulating the Painless script used in the YAML version of this test
     * without requiring the Painless engine.
     */
    public static class UpdateSemanticFieldsScriptPlugin extends MockScriptPlugin {
        @Override
        public String pluginScriptLang() {
            return UBQ_SCRIPT_LANG;
        }

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of(UPDATE_SEMANTIC_FIELDS_SCRIPT, vars -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                @SuppressWarnings("unchecked")
                Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                source.put("sparse_field", "updated inference test");
                source.put("dense_field", "another updated inference test");
                return ctx;
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ReindexPlugin.class);
        plugins.add(UpdateSemanticFieldsScriptPlugin.class);
        return plugins;
    }

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
     * Ported from "Calculates sparse embedding and text embedding results for new documents with integer value" and "...with boolean value"
     * in {@code 30_semantic_text_inference_bwc.yml}.
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

    /**
     * Ported from "Calculates sparse embedding and text embedding results for new documents with a collection of mixed data types" in
     * {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that an array of mixed-type values (boolean, integer, string, float) is coerced to
     * an array of strings in the legacy format.
     */
    @SuppressWarnings("unchecked")
    public void testLegacyFormatMixedDataTypesCoercedToString() throws Exception {
        createLegacyIndex();

        Map<String, Object> source = new HashMap<>();
        source.put(SPARSE_FIELD, List.of(false, 75, "inference test", 13.49));
        source.put(DENSE_FIELD, List.of(true, 49.99, "another inference test", 5654));
        String docId = client().prepareIndex(indexName).setSource(source).get().getId();

        GetResponse getResponse = client().prepareGet(indexName, docId).get();
        Map<String, Object> sourceMap = getResponse.getSourceAsMap();

        Map<String, Object> sparseFieldMap = (Map<String, Object>) sourceMap.get(SPARSE_FIELD);
        List<String> sparseTexts = (List<String>) sparseFieldMap.get("text");
        assertThat(sparseTexts, hasSize(4));
        assertThat(sparseTexts.get(0), equalTo("false"));
        assertThat(sparseTexts.get(1), equalTo("75"));
        assertThat(sparseTexts.get(2), equalTo("inference test"));
        assertThat(sparseTexts.get(3), equalTo("13.49"));
        Map<String, Object> sparseInference = (Map<String, Object>) sparseFieldMap.get("inference");
        List<Map<String, Object>> sparseChunks = (List<Map<String, Object>>) sparseInference.get("chunks");
        assertThat(sparseChunks, hasSize(4));

        Map<String, Object> denseFieldMap = (Map<String, Object>) sourceMap.get(DENSE_FIELD);
        List<String> denseTexts = (List<String>) denseFieldMap.get("text");
        assertThat(denseTexts, hasSize(4));
        assertThat(denseTexts.get(0), equalTo("true"));
        assertThat(denseTexts.get(1), equalTo("49.99"));
        assertThat(denseTexts.get(2), equalTo("another inference test"));
        assertThat(denseTexts.get(3), equalTo("5654"));
        Map<String, Object> denseInference = (Map<String, Object>) denseFieldMap.get("inference");
        List<Map<String, Object>> denseChunks = (List<Map<String, Object>>) denseInference.get("chunks");
        assertThat(denseChunks, hasSize(4));
    }

    /**
     * Ported from "Reindex works for semantic_text fields" in {@code 30_semantic_text_inference_bwc.yml}.
     * Reindexes a legacy-format document into a second legacy index and verifies that the
     * embeddings are preserved (not re-computed).
     */
    @SuppressWarnings("unchecked")
    public void testLegacyFormatReindex() throws Exception {
        createLegacyIndex();

        final String inputText = "inference test";
        final String denseText = "another inference test";
        client().prepareIndex(indexName)
            .setId("doc_1")
            .setSource(Map.of(SPARSE_FIELD, inputText, DENSE_FIELD, denseText, "non_inference_field", "non inference test"))
            .get();

        // Capture embeddings from source before reindex
        GetResponse srcGet = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> srcSourceMap = srcGet.getSourceAsMap();
        Map<String, Object> srcSparse = (Map<String, Object>) srcSourceMap.get(SPARSE_FIELD);
        Map<String, Object> srcSparseInference = (Map<String, Object>) srcSparse.get("inference");
        List<Map<String, Object>> srcSparseChunks = (List<Map<String, Object>>) srcSparseInference.get("chunks");
        Object sparseEmbeddings = srcSparseChunks.get(0).get("embeddings");

        Map<String, Object> srcDense = (Map<String, Object>) srcSourceMap.get(DENSE_FIELD);
        Map<String, Object> srcDenseInference = (Map<String, Object>) srcDense.get("inference");
        List<Map<String, Object>> srcDenseChunks = (List<Map<String, Object>>) srcDenseInference.get("chunks");
        Object denseEmbeddings = srcDenseChunks.get(0).get("embeddings");

        client().admin().indices().prepareRefresh(indexName).get();

        // Create destination legacy index
        String destIndex = indexName + "_dest";
        assertAcked(
            prepareCreate(destIndex).setSettings(legacyIndexSettings())
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

        try {
            new ReindexRequestBuilder(client()).source(indexName).destination(destIndex).get();

            GetResponse destGet = client().prepareGet(destIndex, "doc_1").get();
            assertTrue(destGet.isExists());
            Map<String, Object> destSourceMap = destGet.getSourceAsMap();

            Map<String, Object> destSparse = (Map<String, Object>) destSourceMap.get(SPARSE_FIELD);
            assertThat(destSparse.get("text"), equalTo(inputText));
            Map<String, Object> destSparseInf = (Map<String, Object>) destSparse.get("inference");
            List<Map<String, Object>> destSparseChunks = (List<Map<String, Object>>) destSparseInf.get("chunks");
            assertThat(destSparseChunks.get(0).get("text"), equalTo(inputText));
            assertThat(destSparseChunks.get(0).get("embeddings"), equalTo(sparseEmbeddings));

            Map<String, Object> destDense = (Map<String, Object>) destSourceMap.get(DENSE_FIELD);
            assertThat(destDense.get("text"), equalTo(denseText));
            Map<String, Object> destDenseInf = (Map<String, Object>) destDense.get("inference");
            List<Map<String, Object>> destDenseChunks = (List<Map<String, Object>>) destDenseInf.get("chunks");
            assertThat(destDenseChunks.get(0).get("text"), equalTo(denseText));
            assertThat(destDenseChunks.get(0).get("embeddings"), equalTo(denseEmbeddings));

            assertThat(destSourceMap.get("non_inference_field"), equalTo("non inference test"));
        } finally {
            IntegrationTestUtils.deleteIndex(client(), destIndex);
        }
    }

    /**
     * Ported from "semantic_text copy_to calculates embeddings for source fields" in
     * {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that when multiple text fields have {@code copy_to} pointing at a {@code semantic_text}
     * field, the inference layer generates chunks for all copied values in addition to the field's
     * own value.
     */
    @SuppressWarnings("unchecked")
    public void testLegacyFormatCopyTo() throws Exception {
        String copyToIndex = indexName + "_copy_to";
        assertAcked(
            prepareCreate(copyToIndex).setSettings(legacyIndexSettings())
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

        try {
            client().prepareIndex(copyToIndex)
                .setId("doc_1")
                .setSource(
                    Map.of(
                        "source_field",
                        "copy_to inference test",
                        SPARSE_FIELD,
                        "inference test",
                        "another_source_field",
                        "another copy_to inference test"
                    )
                )
                .get();

            GetResponse getResponse = client().prepareGet(copyToIndex, "doc_1").get();
            Map<String, Object> sourceMap = getResponse.getSourceAsMap();

            Map<String, Object> sparseFieldMap = (Map<String, Object>) sourceMap.get(SPARSE_FIELD);
            assertThat(sparseFieldMap.get("text"), equalTo("inference test"));
            Map<String, Object> inference = (Map<String, Object>) sparseFieldMap.get("inference");
            List<Map<String, Object>> chunks = (List<Map<String, Object>>) inference.get("chunks");
            assertThat("should have 3 chunks (direct + 2 copy_to sources)", chunks, hasSize(3));
            // All three source texts must appear somewhere in the chunks
            List<String> chunkTexts = chunks.stream().map(c -> (String) c.get("text")).toList();
            assertTrue(chunkTexts.contains("inference test"));
            assertTrue(chunkTexts.contains("copy_to inference test"));
            assertTrue(chunkTexts.contains("another copy_to inference test"));
            for (Map<String, Object> chunk : chunks) {
                assertThat(chunk.get("embeddings"), notNullValue());
            }
        } finally {
            IntegrationTestUtils.deleteIndex(client(), copyToIndex);
        }
    }

    /**
     * Ported from "Update by query picks up new semantic_text fields" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that running an update-by-query after adding a new {@code semantic_text} mapping
     * generates inference for the previously-unindexed field values.
     */
    @SuppressWarnings("unchecked")
    public void testLegacyFormatUpdateByQueryPicksUpNewSemanticTextFields() throws Exception {
        String mappingUpdateIndex = indexName + "_mapping_update";
        assertAcked(
            prepareCreate(mappingUpdateIndex).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("dynamic", false)
                        .startObject("properties")
                        .startObject("non_inference_field")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        try {
            client().prepareIndex(mappingUpdateIndex)
                .setId("doc_1")
                .setRefreshPolicy("true")
                .setSource(
                    Map.of(
                        SPARSE_FIELD,
                        "inference test",
                        DENSE_FIELD,
                        "another inference test",
                        "non_inference_field",
                        "non inference test"
                    )
                )
                .get();

            // Add semantic_text fields to the mapping
            indicesAdmin().preparePutMapping(mappingUpdateIndex)
                .setSource(
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
                .get();

            BulkByScrollResponse ubqResponse = new UpdateByQueryRequestBuilder(client()).source(mappingUpdateIndex).get();
            assertThat(ubqResponse.getUpdated(), equalTo(1L));

            GetResponse getResponse = client().prepareGet(mappingUpdateIndex, "doc_1").get();
            Map<String, Object> sourceMap = getResponse.getSourceAsMap();
            assertLegacyFieldStructure(sourceMap, SPARSE_FIELD, "inference test");
            assertLegacyFieldStructure(sourceMap, DENSE_FIELD, "another inference test");
            assertThat(sourceMap.get("non_inference_field"), equalTo("non inference test"));
        } finally {
            IntegrationTestUtils.deleteIndex(client(), mappingUpdateIndex);
        }
    }

    /**
     * Ported from "Update by query works for scripts" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that an update-by-query with a Painless script that modifies {@code semantic_text}
     * fields triggers re-inference for the new values.
     */
    public void testLegacyFormatUpdateByQueryWorksForScripts() throws Exception {
        createLegacyIndex();

        client().prepareIndex(indexName)
            .setId("doc_1")
            .setRefreshPolicy("true")
            .setSource(
                Map.of(SPARSE_FIELD, "inference test", DENSE_FIELD, "another inference test", "non_inference_field", "non inference test")
            )
            .get();

        BulkByScrollResponse ubqResponse = new UpdateByQueryRequestBuilder(client()).source(indexName)
            .script(new Script(ScriptType.INLINE, UBQ_SCRIPT_LANG, UPDATE_SEMANTIC_FIELDS_SCRIPT, Map.of()))
            .get();
        assertThat(ubqResponse.getUpdated(), equalTo(1L));

        GetResponse getResponse = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> sourceMap = getResponse.getSourceAsMap();
        assertLegacyFieldStructure(sourceMap, SPARSE_FIELD, "updated inference test");
        assertLegacyFieldStructure(sourceMap, DENSE_FIELD, "another updated inference test");
    }

    /**
     * Ported from "Can be used inside an object field" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that {@code semantic_text} fields nested inside a plain object field generate
     * inference data with the correct legacy structure.
     */
    @SuppressWarnings("unchecked")
    public void testLegacyFormatCanBeUsedInsideObjectField() throws Exception {
        String objectIndex = indexName + "_object";
        assertAcked(
            prepareCreate(objectIndex).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("level_1")
                        .startObject("properties")
                        .startObject(SPARSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", SPARSE_INFERENCE_ID)
                        .endObject()
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        try {
            client().prepareIndex(objectIndex)
                .setId("doc_1")
                .setSource(Map.of("level_1", Map.of(SPARSE_FIELD, "inference test", DENSE_FIELD, "another inference test")))
                .get();

            GetResponse getResponse = client().prepareGet(objectIndex, "doc_1").get();
            Map<String, Object> sourceMap = getResponse.getSourceAsMap();
            Map<String, Object> level1 = (Map<String, Object>) sourceMap.get("level_1");
            assertThat(level1, notNullValue());
            assertLegacyFieldStructure(level1, SPARSE_FIELD, "inference test");
            assertLegacyFieldStructure(level1, DENSE_FIELD, "another inference test");
        } finally {
            IntegrationTestUtils.deleteIndex(client(), objectIndex);
        }
    }

    /**
     * Ported from "Deletes on bulk operation" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that a bulk request containing a delete and an update on the same index applies
     * both operations correctly for legacy semantic_text documents.
     */
    @SuppressWarnings("unchecked")
    public void testLegacyFormatDeletesOnBulkOperation() throws Exception {
        createLegacyIndex();

        // Index two documents with array values
        BulkRequestBuilder firstBulk = client().prepareBulk().setRefreshPolicy("true");
        firstBulk.add(
            new IndexRequestBuilder(client()).setIndex(indexName)
                .setId("1")
                .setSource(Map.of(DENSE_FIELD, List.of("you know, for testing", "now with chunks")))
        );
        firstBulk.add(
            new IndexRequestBuilder(client()).setIndex(indexName)
                .setId("2")
                .setSource(Map.of(DENSE_FIELD, List.of("some more tests", "that include chunks")))
        );
        assertFalse(firstBulk.get().hasFailures());

        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(DENSE_FIELD, "you know, for testing")).trackTotalHits(true)
                )
            ),
            response -> assertHitCount(response, 2L)
        );

        // Delete doc 2 and update doc 1 in a single bulk
        BulkRequestBuilder secondBulk = client().prepareBulk().setRefreshPolicy("true");
        secondBulk.add(new DeleteRequest(indexName, "2"));
        secondBulk.add(new UpdateRequest(indexName, "1").doc(Map.of(DENSE_FIELD, "updated text")));
        assertFalse(secondBulk.get().hasFailures());

        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(DENSE_FIELD, "you know, for testing")).trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                Map<String, Object> hit0Source = response.getHits().getAt(0).getSourceAsMap();
                Map<String, Object> denseFieldMap = (Map<String, Object>) hit0Source.get(DENSE_FIELD);
                assertThat(denseFieldMap.get("text"), equalTo("updated text"));
            }
        );
    }

    /**
     * Ported from "Skip fetching _inference_fields" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that requesting {@code _inference_fields} as a fetch field does not result in
     * a spurious {@code _inference_fields} key in the returned {@code _source} for legacy indices.
     */
    public void testLegacyFormatSkipFetchingInferenceFields() throws Exception {
        createLegacyIndex();

        String docId = client().prepareIndex(indexName)
            .setSource(Map.of(SPARSE_FIELD, "test value"))
            .setRefreshPolicy("true")
            .get()
            .getId();

        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).fetchField("_inference_fields").trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                Map<String, Object> sourceMap = response.getHits().getAt(0).getSourceAsMap();
                assertFalse("_inference_fields must not appear in legacy format _source", sourceMap.containsKey("_inference_fields"));
            }
        );
    }

    /**
     * Ported from "Skip fetching _inference_fields with exclude_vectors" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that setting {@code _source.exclude_vectors: false} does not add a spurious
     * {@code _inference_fields} key to the {@code _source} of legacy-format documents.
     */
    public void testLegacyFormatSkipFetchingInferenceFieldsWithExcludeVectors() throws Exception {
        createLegacyIndex();

        client().prepareIndex(indexName).setSource(Map.of(SPARSE_FIELD, "test value")).setRefreshPolicy("true").get();

        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).excludeVectors(false).trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 1L);
                Map<String, Object> sourceMap = response.getHits().getAt(0).getSourceAsMap();
                assertFalse("_inference_fields must not appear in legacy format _source", sourceMap.containsKey("_inference_fields"));
            }
        );
    }

    /**
     * Ported from "Empty semantic_text field skips embedding generation" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that indexing an empty string or a whitespace-only string produces a document with
     * zero inference chunks in the legacy format.
     */
    @SuppressWarnings("unchecked")
    public void testLegacyFormatEmptySemanticTextFieldSkipsEmbeddingGeneration() throws Exception {
        createLegacyIndex();

        client().prepareIndex(indexName).setId("doc_1").setSource(Map.of(SPARSE_FIELD, "")).setRefreshPolicy("true").get();
        client().prepareIndex(indexName).setId("doc_2").setSource(Map.of(SPARSE_FIELD, "  ")).setRefreshPolicy("true").get();

        // Verify via GET so we are not dependent on search order
        GetResponse getDoc1 = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> source1 = getDoc1.getSourceAsMap();
        Map<String, Object> sparseField1 = (Map<String, Object>) source1.get(SPARSE_FIELD);
        assertThat(sparseField1.get("text"), equalTo(""));
        Map<String, Object> inference1 = (Map<String, Object>) sparseField1.get("inference");
        List<Map<String, Object>> chunks1 = (List<Map<String, Object>>) inference1.get("chunks");
        assertThat(chunks1, hasSize(0));

        GetResponse getDoc2 = client().prepareGet(indexName, "doc_2").get();
        Map<String, Object> source2 = getDoc2.getSourceAsMap();
        Map<String, Object> sparseField2 = (Map<String, Object>) source2.get(SPARSE_FIELD);
        assertThat(sparseField2.get("text"), equalTo("  "));
        Map<String, Object> inference2 = (Map<String, Object>) sparseField2.get("inference");
        List<Map<String, Object>> chunks2 = (List<Map<String, Object>>) inference2.get("chunks");
        assertThat(chunks2, hasSize(0));
    }

    /**
     * Ported from "Multi chunks skips empty input embedding generation" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that when an array input contains whitespace-only elements, those elements are
     * excluded from inference and only non-empty elements produce chunks.
     */
    @SuppressWarnings("unchecked")
    public void testLegacyFormatMultiChunksSkipsEmptyInputEmbeddingGeneration() throws Exception {
        createLegacyIndex();

        client().prepareIndex(indexName)
            .setId("doc_1")
            .setSource(Map.of(SPARSE_FIELD, List.of("some test data", "    ", "now with chunks")))
            .setRefreshPolicy("true")
            .get();

        GetResponse getResponse = client().prepareGet(indexName, "doc_1").get();
        Map<String, Object> sourceMap = getResponse.getSourceAsMap();
        Map<String, Object> sparseFieldMap = (Map<String, Object>) sourceMap.get(SPARSE_FIELD);
        Map<String, Object> inference = (Map<String, Object>) sparseFieldMap.get("inference");
        List<Map<String, Object>> chunks = (List<Map<String, Object>>) inference.get("chunks");
        assertThat(chunks, hasSize(2));
        assertThat(chunks.get(0).get("text"), equalTo("some test data"));
        assertThat(chunks.get(0).get("embeddings"), notNullValue());
        assertThat(chunks.get(1).get("text"), equalTo("now with chunks"));
        assertThat(chunks.get(1).get("embeddings"), notNullValue());
    }

    /**
     * Ported from "inference endpoint late creation" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that an index can be created referencing an inference endpoint that does not yet
     * exist, and that indexing documents works correctly once the endpoint is registered.
     */
    public void testLegacyFormatInferenceEndpointLateCreation() throws Exception {
        final String lateInferenceId = "late-inference-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        String lateIndex = indexName + "_late";

        assertAcked(
            prepareCreate(lateIndex).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("inference_field")
                        .field("type", "semantic_text")
                        .field("inference_id", lateInferenceId)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        try {
            // Register the endpoint after the index has been created
            storeSparseModel(lateInferenceId, modelRegistry);

            client().prepareIndex(lateIndex)
                .setId("doc_1")
                .setSource(Map.of("inference_field", "inference test"))
                .setRefreshPolicy("true")
                .get();

            assertResponse(
                client().search(
                    new SearchRequest(lateIndex).source(
                        new SearchSourceBuilder().query(QueryBuilders.existsQuery("inference_field")).trackTotalHits(true)
                    )
                ),
                response -> assertHitCount(response, 1L)
            );
        } finally {
            IntegrationTestUtils.deleteIndex(client(), lateIndex);
        }
    }

    /**
     * Ported from "Skip fetching _inference_fields with _source_includes" in {@code 30_semantic_text_inference_bwc.yml}.
     * Verifies that a GET request with {@code _source_includes=_inference_fields} does not return
     * a spurious {@code _inference_fields} key in the {@code _source} of legacy-format documents.
     */
    public void testLegacyFormatSkipFetchingInferenceFieldsWithSourceIncludes() throws Exception {
        createLegacyIndex();

        client().prepareIndex(indexName).setId("doc_1").setSource(Map.of(SPARSE_FIELD, "test value")).setRefreshPolicy("true").get();

        GetResponse getResponse = client().prepareGet(indexName, "doc_1").setFetchSource(new String[] { "_inference_fields" }, null).get();
        assertThat(getResponse.getId(), equalTo("doc_1"));
        assertFalse(
            "_inference_fields must not appear in legacy format _source",
            getResponse.getSourceAsMap().containsKey("_inference_fields")
        );
    }
}
