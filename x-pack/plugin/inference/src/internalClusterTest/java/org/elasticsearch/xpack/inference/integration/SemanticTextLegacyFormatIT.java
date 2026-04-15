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
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapperTests;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.inference.Utils.storeDenseModel;
import static org.elasticsearch.xpack.inference.Utils.storeSparseModel;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the legacy semantic_text format (indices created before
 * {@link org.elasticsearch.index.IndexVersions#SEMANTIC_TEXT_LEGACY_FORMAT_FORBIDDEN}).
 *
 * <p>These tests replace the deleted YAML BWC test files that set
 * {@code index.mapping.semantic_text.use_legacy_format: true} through the REST API.
 * Private index settings are allowed here so that we can set {@code index.version.created}
 * to a version in the valid legacy range.
 */
public class SemanticTextLegacyFormatIT extends ESIntegTestCase {

    private static final String SPARSE_FIELD = "sparse_field";
    private static final String DENSE_FIELD = "dense_field";
    private static final String SPARSE_INFERENCE_ID = "sparse-inference-id";
    private static final String SPARSE_INFERENCE_ID_2 = "sparse-inference-id-2";
    private static final String DENSE_INFERENCE_ID = "dense-inference-id";
    private static final String DENSE_BBQ_INFERENCE_ID = "dense-bbq-inference-id";

    private String indexName;
    private ModelRegistry modelRegistry;

    @Before
    public void setup() throws Exception {
        indexName = "test_legacy_" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        modelRegistry = internalCluster().getCurrentMasterNodeInstance(ModelRegistry.class);
        storeSparseModel(SPARSE_INFERENCE_ID, modelRegistry);
        storeDenseModel(DENSE_INFERENCE_ID, modelRegistry, 10, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT);
    }

    @After
    public void cleanUp() {
        IntegrationTestUtils.deleteIndex(client(), indexName);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(otherSettings).put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, FakeMlPlugin.class);
    }

    /**
     * Builds settings for a legacy-format index, using a random index version that is compatible
     * with {@code index.mapping.semantic_text.use_legacy_format: true}.
     */
    private Settings legacyIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(true))
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }

    /** Creates a legacy index with both sparse and dense semantic_text fields. */
    private void createLegacyIndex() throws Exception {
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
                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    // -----------------------------------------------------------------------
    // Tests ported from 30_semantic_text_inference_bwc.yml
    // -----------------------------------------------------------------------

    /**
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

    // -----------------------------------------------------------------------
    // Tests ported from 60_semantic_text_inference_update_bwc.yml
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Tests ported from 90_semantic_text_highlighter_bwc.yml
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Tests ported from 10_semantic_text_field_mapping_bwc.yml
    // -----------------------------------------------------------------------

    /**
     * Creates a legacy index with a semantic_text field that has no initial model_settings,
     * confirms the settings are absent before indexing, then indexes a document and confirms
     * that model_settings are subsequently populated in the mapping.
     */
    public void testLegacyFormatMappingPopulatedAfterFirstSparseDocument() throws Exception {
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
                        .endObject()
                        .endObject()
                )
                .get()
        );

        // Before indexing: model_settings should be absent
        assertMappingModelSettings(indexName, SPARSE_FIELD, false);

        // Index a document to trigger inference and mapping population
        client().prepareIndex(indexName).setSource(Map.of(SPARSE_FIELD, "mapping test")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // After indexing: model_settings should be present
        assertMappingModelSettings(indexName, SPARSE_FIELD, true);
    }

    /**
     * Creates a legacy index with a dense semantic_text field that has no initial model_settings,
     * confirms the settings are absent before indexing, then indexes a document and confirms
     * that model_settings (including dimensions, similarity, and element_type) are subsequently
     * populated in the mapping.
     */
    public void testLegacyFormatMappingPopulatedAfterFirstDenseDocument() throws Exception {
        assertAcked(
            prepareCreate(indexName).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        // Before indexing: model_settings should be absent
        assertMappingModelSettings(indexName, DENSE_FIELD, false);

        // Index a document to trigger inference and mapping population
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "dense mapping test")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // After indexing: model_settings should be present with dense-specific fields
        assertMappingModelSettings(indexName, DENSE_FIELD, true);
        assertDenseMappingModelSettings(indexName, DENSE_FIELD);
    }

    /**
     * Verifies field_caps behaviour for a legacy index containing a sparse semantic_text field.
     * Before indexing, {@code include_empty_fields=false} returns neither field. After indexing a
     * sparse document, {@code include_empty_fields=false} returns the sparse field but not the
     * dense field. The text sub-field of the sparse field should be searchable.
     */
    public void testLegacyFormatFieldCapsWithSparseEmbedding() throws Exception {
        createLegacyIndex();

        // Before indexing: include_empty_fields=true → both fields present
        FieldCapabilitiesResponse withEmpty = fieldCaps(true);
        assertNotNull(withEmpty.getField(SPARSE_FIELD));
        assertNotNull(withEmpty.getField(DENSE_FIELD));

        // Before indexing: include_empty_fields=false → neither field present
        FieldCapabilitiesResponse withoutEmpty = fieldCaps(false);
        assertNull(withoutEmpty.getField(SPARSE_FIELD));
        assertNull(withoutEmpty.getField(DENSE_FIELD));

        // Index one sparse document and refresh
        client().prepareIndex(indexName).setSource(Map.of(SPARSE_FIELD, "these are not the droids you're looking for")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // After indexing: include_empty_fields=true → both fields present, sparse text sub-field searchable
        FieldCapabilitiesResponse withEmptyAfter = fieldCaps(true);
        assertNotNull(withEmptyAfter.getField(SPARSE_FIELD));
        assertNotNull(withEmptyAfter.getField(DENSE_FIELD));
        assertThat(withEmptyAfter.getField(SPARSE_FIELD).get("text").isSearchable(), equalTo(true));
        assertThat(withEmptyAfter.getField(DENSE_FIELD).get("text").isSearchable(), equalTo(true));

        // After indexing: include_empty_fields=false → sparse_field present, dense_field absent
        FieldCapabilitiesResponse withoutEmptyAfter = fieldCaps(false);
        assertNotNull(withoutEmptyAfter.getField(SPARSE_FIELD));
        assertNull(withoutEmptyAfter.getField(DENSE_FIELD));
        assertThat(withoutEmptyAfter.getField(SPARSE_FIELD).get("text").isSearchable(), equalTo(true));
    }

    /**
     * Verifies that indexing a document into a legacy-format index backed by a BBQ-compatible model
     * does NOT auto-populate {@code index_options} in the mapping. In the new (non-legacy) format
     * the mapper would inject default index_options from the inference service; the legacy format
     * must not do so.
     */
    public void testLegacyFormatBbqCompatibleModelHasNoAutoIndexOptions() throws Exception {
        storeDenseModel(DENSE_BBQ_INFERENCE_ID, modelRegistry, 64, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT);

        String bbqIndexName = indexName + "_bbq";
        assertAcked(
            prepareCreate(bbqIndexName).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_BBQ_INFERENCE_ID)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        try {
            // Before indexing: no model_settings and no index_options
            assertMappingModelSettings(bbqIndexName, DENSE_FIELD, false);
            assertMappingHasNoIndexOptions(bbqIndexName, DENSE_FIELD);

            // Index one document
            client().prepareIndex(bbqIndexName).setSource(Map.of(DENSE_FIELD, "these are not the droids you're looking for")).get();
            client().admin().indices().prepareRefresh(bbqIndexName).get();

            // After indexing: model_settings present, but still no index_options (legacy does not auto-populate)
            assertMappingModelSettings(bbqIndexName, DENSE_FIELD, true);
            assertMappingHasNoIndexOptions(bbqIndexName, DENSE_FIELD);
        } finally {
            IntegrationTestUtils.deleteIndex(client(), bbqIndexName);
        }
    }

    /**
     * Verifies field_caps behaviour for a legacy index containing a dense semantic_text field.
     * After indexing a dense document, {@code include_empty_fields=false} returns the dense field
     * but not the sparse field.
     */
    public void testLegacyFormatFieldCapsWithTextEmbedding() throws Exception {
        createLegacyIndex();

        // Before indexing: include_empty_fields=false → neither field present
        FieldCapabilitiesResponse withoutEmpty = fieldCaps(false);
        assertNull(withoutEmpty.getField(SPARSE_FIELD));
        assertNull(withoutEmpty.getField(DENSE_FIELD));

        // Index one dense document and refresh
        client().prepareIndex(indexName).setSource(Map.of(DENSE_FIELD, "these are not the droids you're looking for")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // After indexing: include_empty_fields=true → both fields present
        FieldCapabilitiesResponse withEmptyAfter = fieldCaps(true);
        assertNotNull(withEmptyAfter.getField(SPARSE_FIELD));
        assertNotNull(withEmptyAfter.getField(DENSE_FIELD));
        assertThat(withEmptyAfter.getField(SPARSE_FIELD).get("text").isSearchable(), equalTo(true));
        assertThat(withEmptyAfter.getField(DENSE_FIELD).get("text").isSearchable(), equalTo(true));

        // After indexing: include_empty_fields=false → dense_field present, sparse_field absent
        FieldCapabilitiesResponse withoutEmptyAfter = fieldCaps(false);
        assertNull(withoutEmptyAfter.getField(SPARSE_FIELD));
        assertNotNull(withoutEmptyAfter.getField(DENSE_FIELD));
        assertThat(withoutEmptyAfter.getField(DENSE_FIELD).get("text").isSearchable(), equalTo(true));
    }

    /**
     * Verifies that field_caps does not expose the internal sub-fields of semantic_text
     * ({@code inference}, {@code inference.chunks}, {@code inference.chunks.embeddings}, etc.).
     */
    public void testLegacyFormatFieldCapsExcludesSubFields() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(SPARSE_FIELD, "test text", DENSE_FIELD, "test text")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        FieldCapabilitiesResponse response = fieldCaps(true);
        assertNotNull(response.getField(SPARSE_FIELD));
        assertNotNull(response.getField(DENSE_FIELD));

        // Internal sub-fields must NOT be exposed
        assertNull(response.getField(SPARSE_FIELD + ".inference"));
        assertNull(response.getField(SPARSE_FIELD + ".inference.chunks"));
        assertNull(response.getField(SPARSE_FIELD + ".inference.chunks.embeddings"));
        assertNull(response.getField(DENSE_FIELD + ".inference"));
        assertNull(response.getField(DENSE_FIELD + ".inference.chunks"));
        assertNull(response.getField(DENSE_FIELD + ".inference.chunks.embeddings"));
    }

    /**
     * Verifies that using a {@code SemanticQueryBuilder} as an index filter in a field_caps request
     * does not cause a failure and returns the expected fields.
     */
    public void testLegacyFormatFieldCapsWithSemanticQueryFilter() throws Exception {
        createLegacyIndex();
        client().prepareIndex(indexName).setSource(Map.of(SPARSE_FIELD, "This is a story about a cat and a dog.")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.indices(indexName);
        request.fields("*");
        request.indexFilter(new SemanticQueryBuilder(SPARSE_FIELD, "test"));

        FieldCapabilitiesResponse response = client().execute(TransportFieldCapabilitiesAction.TYPE, request).actionGet();
        assertNotNull(response.getField(SPARSE_FIELD));
        assertThat(response.getField(SPARSE_FIELD).get("text").isSearchable(), equalTo(true));
    }

    /**
     * Creates a legacy index with explicit {@code int8_hnsw} dense index options, verifies the
     * options are persisted in the mapping before and after indexing a document.
     */
    public void testLegacyFormatDenseIndexOptionsPreserved() throws Exception {
        String optionsIndex = indexName + "_options";
        assertAcked(
            prepareCreate(optionsIndex).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(DENSE_FIELD)
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int8_hnsw")
                        .field("m", 20)
                        .field("ef_construction", 100)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        try {
            assertDenseIndexOptions(optionsIndex, DENSE_FIELD, "int8_hnsw", 20, 100);

            client().prepareIndex(optionsIndex).setSource(Map.of(DENSE_FIELD, "mapping test")).get();
            client().admin().indices().prepareRefresh(optionsIndex).get();

            assertDenseIndexOptions(optionsIndex, DENSE_FIELD, "int8_hnsw", 20, 100);
        } finally {
            IntegrationTestUtils.deleteIndex(client(), optionsIndex);
        }
    }

    /**
     * Creates a legacy index with dense index options and verifies they can be updated (m and
     * ef_construction) but not with an incompatible type change.
     */
    public void testLegacyFormatDenseIndexOptionsUpdate() throws Exception {
        String optionsIndex = indexName + "_options_update";
        assertAcked(
            prepareCreate(optionsIndex).setSettings(legacyIndexSettings())
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int8_hnsw")
                        .field("m", 16)
                        .field("ef_construction", 100)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        try {
            assertDenseIndexOptions(optionsIndex, "semantic_field", "int8_hnsw", 16, 100);

            // Update m and ef_construction (compatible change)
            indicesAdmin().preparePutMapping(optionsIndex)
                .setSource(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("semantic_field")
                        .field("type", "semantic_text")
                        .field("inference_id", DENSE_INFERENCE_ID)
                        .startObject("index_options")
                        .startObject("dense_vector")
                        .field("type", "int8_hnsw")
                        .field("m", 20)
                        .field("ef_construction", 90)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get();

            assertDenseIndexOptions(optionsIndex, "semantic_field", "int8_hnsw", 20, 90);

            // Changing the type is incompatible and must fail
            assertThrows(
                Exception.class,
                () -> indicesAdmin().preparePutMapping(optionsIndex)
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("properties")
                            .startObject("semantic_field")
                            .field("type", "semantic_text")
                            .field("inference_id", DENSE_INFERENCE_ID)
                            .startObject("index_options")
                            .startObject("dense_vector")
                            .field("type", "int8_flat")
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                    )
                    .get()
            );
        } finally {
            IntegrationTestUtils.deleteIndex(client(), optionsIndex);
        }
    }

    /**
     * Verifies that the inference_id of a semantic_text field can be updated to a compatible
     * endpoint. After updating, documents indexed before the change retain the old inference_id
     * in their stored source, and documents indexed after the change use the new inference_id.
     */
    @SuppressWarnings("unchecked")
    public void testLegacyFormatInferenceIdUpdate() throws Exception {
        createLegacyIndex();

        // Index first document with the original inference_id
        client().prepareIndex(indexName).setId("doc_1").setSource(Map.of(SPARSE_FIELD, "This is a story about a cat and a dog.")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // Verify mapping shows original inference_id and model_settings
        assertMappingInferenceId(indexName, SPARSE_FIELD, SPARSE_INFERENCE_ID);
        assertMappingModelSettings(indexName, SPARSE_FIELD, true);

        // Register the second sparse model and update the inference_id
        storeSparseModel(SPARSE_INFERENCE_ID_2, modelRegistry);
        indicesAdmin().preparePutMapping(indexName)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject(SPARSE_FIELD)
                    .field("type", "semantic_text")
                    .field("inference_id", SPARSE_INFERENCE_ID_2)
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        // Verify mapping shows updated inference_id
        assertMappingInferenceId(indexName, SPARSE_FIELD, SPARSE_INFERENCE_ID_2);

        // Index second document with the updated inference_id
        client().prepareIndex(indexName).setId("doc_2").setSource(Map.of(SPARSE_FIELD, "One day they started playing the piano.")).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // Search and verify per-doc inference_id in stored source
        assertResponse(
            client().search(
                new SearchRequest(indexName).source(
                    new SearchSourceBuilder().query(new SemanticQueryBuilder(SPARSE_FIELD, "piano"))
                        .excludeVectors(false)
                        .trackTotalHits(true)
                )
            ),
            response -> {
                assertHitCount(response, 2L);
                // Both docs are returned; locate each by _id and verify inference_id
                for (int i = 0; i < 2; i++) {
                    String docId = response.getHits().getAt(i).getId();
                    Map<String, Object> source = response.getHits().getAt(i).getSourceAsMap();
                    Map<String, Object> fieldMap = (Map<String, Object>) source.get(SPARSE_FIELD);
                    assertNotNull("source for " + docId + " should have " + SPARSE_FIELD, fieldMap);
                    Map<String, Object> inferenceMap = (Map<String, Object>) fieldMap.get("inference");
                    assertNotNull("inference should be present for " + docId, inferenceMap);
                    String expectedInferenceId = "doc_1".equals(docId) ? SPARSE_INFERENCE_ID : SPARSE_INFERENCE_ID_2;
                    assertThat("inference_id for " + docId, inferenceMap.get("inference_id"), equalTo(expectedInferenceId));
                }
            }
        );
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Asserts the legacy semantic_text source structure for a given field:
     * <ul>
     *   <li>{@code field.text} equals the expected text</li>
     *   <li>{@code field.inference.chunks[0].text} equals the expected text</li>
     *   <li>{@code field.inference.chunks[0].embeddings} is non-null</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    private void assertLegacyFieldStructure(Map<String, Object> sourceMap, String fieldName, String expectedText) {
        Map<String, Object> fieldMap = (Map<String, Object>) sourceMap.get(fieldName);
        assertThat("field map for [" + fieldName + "] should not be null", fieldMap, notNullValue());
        assertThat(fieldName + ".text", fieldMap.get("text"), equalTo(expectedText));

        Map<String, Object> inference = (Map<String, Object>) fieldMap.get("inference");
        assertThat(fieldName + ".inference", inference, notNullValue());

        List<Map<String, Object>> chunks = (List<Map<String, Object>>) inference.get("chunks");
        assertThat(fieldName + ".inference.chunks", chunks, notNullValue());
        assertFalse(fieldName + ".inference.chunks should not be empty", chunks.isEmpty());

        Map<String, Object> firstChunk = chunks.get(0);
        assertThat(fieldName + ".inference.chunks[0].text", firstChunk.get("text"), equalTo(expectedText));
        assertThat(fieldName + ".inference.chunks[0].embeddings", firstChunk.get("embeddings"), notNullValue());
    }

    /**
     * Returns the embeddings object from the first chunk of the given semantic_text field in the
     * stored document, or null if the field or chunks are absent.
     */
    @SuppressWarnings("unchecked")
    private Object getEmbeddingsFromFirstChunk(String index, String docId, String fieldName) {
        GetResponse response = client().prepareGet(index, docId).get();
        Map<String, Object> sourceMap = response.getSourceAsMap();
        Map<String, Object> fieldMap = (Map<String, Object>) sourceMap.get(fieldName);
        if (fieldMap == null) {
            return null;
        }
        Map<String, Object> inference = (Map<String, Object>) fieldMap.get("inference");
        if (inference == null) {
            return null;
        }
        List<Map<String, Object>> chunks = (List<Map<String, Object>>) inference.get("chunks");
        if (chunks == null || chunks.isEmpty()) {
            return null;
        }
        return chunks.get(0).get("embeddings");
    }

    /**
     * Asserts whether the given semantic_text field's mapping contains {@code model_settings}.
     *
     * @param index      the index to inspect
     * @param fieldName  the field name to inspect
     * @param shouldExist whether model_settings is expected to be present
     */
    @SuppressWarnings("unchecked")
    private void assertMappingModelSettings(String index, String fieldName, boolean shouldExist) {
        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, index).get();
        MappingMetadata mappingMetadata = mappingsResponse.getMappings().get(index);
        assertThat(mappingMetadata, notNullValue());
        Map<String, Object> mappingSource = mappingMetadata.getSourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mappingSource.get("properties");
        assertThat("properties should not be null", properties, notNullValue());
        Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
        assertThat("field mapping for [" + fieldName + "] should not be null", fieldMapping, notNullValue());

        boolean modelSettingsPresent = fieldMapping.containsKey("model_settings");
        if (shouldExist) {
            assertTrue("model_settings should be present in [" + fieldName + "] mapping", modelSettingsPresent);
        } else {
            assertFalse("model_settings should not be present in [" + fieldName + "] mapping before first doc", modelSettingsPresent);
        }
    }

    /**
     * Asserts that the model_settings for a dense semantic_text field contain the dense-specific
     * fields ({@code dimensions}, {@code similarity}, {@code element_type}) in addition to
     * {@code task_type}.
     */
    @SuppressWarnings("unchecked")
    private void assertDenseMappingModelSettings(String index, String fieldName) {
        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, index).get();
        MappingMetadata mappingMetadata = mappingsResponse.getMappings().get(index);
        assertThat(mappingMetadata, notNullValue());
        Map<String, Object> properties = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
        Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
        Map<String, Object> modelSettings = (Map<String, Object>) fieldMapping.get("model_settings");
        assertThat("model_settings.task_type", modelSettings.get("task_type"), notNullValue());
        assertThat("model_settings.dimensions", modelSettings.get("dimensions"), notNullValue());
        assertThat("model_settings.similarity", modelSettings.get("similarity"), notNullValue());
        assertThat("model_settings.element_type", modelSettings.get("element_type"), notNullValue());
    }

    /**
     * Executes a {@code field_caps} request against {@link #indexName} for all fields ({@code *}).
     *
     * @param includeEmptyFields whether to include fields that have no data in the index
     */
    private FieldCapabilitiesResponse fieldCaps(boolean includeEmptyFields) {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.indices(indexName);
        request.fields("*");
        request.includeEmptyFields(includeEmptyFields);
        return client().execute(TransportFieldCapabilitiesAction.TYPE, request).actionGet();
    }

    /**
     * Asserts that the {@code index_options} key is absent from the mapping of the given field.
     */
    @SuppressWarnings("unchecked")
    private void assertMappingHasNoIndexOptions(String index, String fieldName) {
        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, index).get();
        MappingMetadata mappingMetadata = mappingsResponse.getMappings().get(index);
        assertThat(mappingMetadata, notNullValue());
        Map<String, Object> properties = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
        Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
        assertThat("field mapping for [" + fieldName + "] should not be null", fieldMapping, notNullValue());
        assertFalse("index_options should be absent from [" + fieldName + "] mapping", fieldMapping.containsKey("index_options"));
    }

    /**
     * Asserts the {@code dense_vector} index options ({@code type}, {@code m},
     * {@code ef_construction}) stored in the field mapping.
     */
    @SuppressWarnings("unchecked")
    private void assertDenseIndexOptions(String index, String fieldName, String expectedType, int expectedM, int expectedEfConstruction) {
        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, index).get();
        MappingMetadata mappingMetadata = mappingsResponse.getMappings().get(index);
        assertThat(mappingMetadata, notNullValue());
        Map<String, Object> properties = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
        Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
        assertThat("field mapping for [" + fieldName + "] should not be null", fieldMapping, notNullValue());
        Map<String, Object> indexOptions = (Map<String, Object>) fieldMapping.get("index_options");
        assertThat("index_options should be present in [" + fieldName + "] mapping", indexOptions, notNullValue());
        Map<String, Object> denseVector = (Map<String, Object>) indexOptions.get("dense_vector");
        assertThat("index_options.dense_vector should be present", denseVector, notNullValue());
        assertThat("index_options.dense_vector.type", denseVector.get("type"), equalTo(expectedType));
        assertThat("index_options.dense_vector.m", denseVector.get("m"), equalTo(expectedM));
        assertThat("index_options.dense_vector.ef_construction", denseVector.get("ef_construction"), equalTo(expectedEfConstruction));
    }

    /**
     * Asserts that the given field's mapping shows the expected {@code inference_id} value.
     */
    @SuppressWarnings("unchecked")
    private void assertMappingInferenceId(String index, String fieldName, String expectedInferenceId) {
        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, index).get();
        MappingMetadata mappingMetadata = mappingsResponse.getMappings().get(index);
        assertThat(mappingMetadata, notNullValue());
        Map<String, Object> properties = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
        Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
        assertThat("field mapping for [" + fieldName + "] should not be null", fieldMapping, notNullValue());
        assertThat("inference_id for [" + fieldName + "]", fieldMapping.get("inference_id"), equalTo(expectedInferenceId));
    }
}
