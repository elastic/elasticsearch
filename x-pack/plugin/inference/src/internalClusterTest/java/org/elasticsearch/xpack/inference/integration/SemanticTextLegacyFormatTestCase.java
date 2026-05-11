/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapperTests;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.inference.Utils.storeDenseModel;
import static org.elasticsearch.xpack.inference.Utils.storeSparseModel;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Base class for integration tests covering the legacy semantic_text format (indices created before
 * {@link org.elasticsearch.index.IndexVersions#SEMANTIC_TEXT_LEGACY_FORMAT_FORBIDDEN}).
 *
 * <p>Provides shared constants, lifecycle hooks, ESIntegTestCase overrides, and helper methods
 * used across all legacy-format test classes. Private index settings are allowed so that
 * {@code index.version.created} can be set to a version in the valid legacy range.
 */
public abstract class SemanticTextLegacyFormatTestCase extends ESIntegTestCase {

    protected static final String SPARSE_FIELD = "sparse_field";
    protected static final String DENSE_FIELD = "dense_field";
    protected static final String SPARSE_INFERENCE_ID = "sparse-inference-id";
    protected static final String SPARSE_INFERENCE_ID_2 = "sparse-inference-id-2";
    protected static final String DENSE_INFERENCE_ID = "dense-inference-id";
    protected static final String DENSE_BBQ_INFERENCE_ID = "dense-bbq-inference-id";

    protected String indexName;
    protected ModelRegistry modelRegistry;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        indexName = "test_legacy_" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        modelRegistry = internalCluster().getCurrentMasterNodeInstance(ModelRegistry.class);
        storeSparseModel(SPARSE_INFERENCE_ID, modelRegistry);
        storeDenseModel(DENSE_INFERENCE_ID, modelRegistry, 10, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IntegrationTestUtils.deleteIndex(client(), indexName);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        // Allows setting index.version.created to a legacy-compatible version in order to create legacy-format indices
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
    protected Settings legacyIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(true))
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }

    /** Creates a legacy index with both sparse and dense semantic_text fields. */
    protected void createLegacyIndex() throws Exception {
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

    /**
     * Asserts the legacy semantic_text source structure for a given field:
     * <ul>
     *   <li>{@code field.text} equals the expected text</li>
     *   <li>{@code field.inference.chunks[0].text} equals the expected text</li>
     *   <li>{@code field.inference.chunks[0].embeddings} is non-null</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    protected void assertLegacyFieldStructure(Map<String, Object> sourceMap, String fieldName, String expectedText) {
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
    protected Object getEmbeddingsFromFirstChunk(String index, String docId, String fieldName) {
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
    protected void assertMappingModelSettings(String index, String fieldName, boolean shouldExist) {
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
    protected void assertDenseMappingModelSettings(String index, String fieldName) {
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
    protected FieldCapabilitiesResponse fieldCaps(boolean includeEmptyFields) {
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
    protected void assertMappingHasNoIndexOptions(String index, String fieldName) {
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
    protected void assertDenseIndexOptions(String index, String fieldName, String expectedType, int expectedM, int expectedEfConstruction) {
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
    protected void assertMappingInferenceId(String index, String fieldName, String expectedInferenceId) {
        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, index).get();
        MappingMetadata mappingMetadata = mappingsResponse.getMappings().get(index);
        assertThat(mappingMetadata, notNullValue());
        Map<String, Object> properties = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
        Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(fieldName);
        assertThat("field mapping for [" + fieldName + "] should not be null", fieldMapping, notNullValue());
        assertThat("inference_id for [" + fieldName + "]", fieldMapping.get("inference_id"), equalTo(expectedInferenceId));
    }

    /**
     * Builds a document source map with a dense legacy-format semantic_text field containing a
     * single chunk.
     *
     * @param fieldName   the semantic_text field name
     * @param text        the original text value
     * @param inferenceId the inference_id to embed in the inline inference structure
     * @param dims        the dimensions value in model_settings
     * @param similarity  the similarity value in model_settings (e.g. "cosine", "dot_product")
     * @param elementType the element_type value in model_settings (e.g. "float", "byte")
     * @param embeddings  the vector embeddings for the single chunk
     * @return a map suitable for passing to {@code prepareIndex(...).setSource(...)}
     */
    protected Map<String, Object> buildLegacyDenseDoc(
        String fieldName,
        String text,
        String inferenceId,
        int dims,
        String similarity,
        String elementType,
        List<Double> embeddings
    ) {
        Map<String, Object> ms = new HashMap<>();
        ms.put("task_type", "text_embedding");
        ms.put("dimensions", dims);
        ms.put("similarity", similarity);
        ms.put("element_type", elementType);

        Map<String, Object> inference = new HashMap<>();
        inference.put("inference_id", inferenceId);
        inference.put("model_settings", ms);
        inference.put("chunks", List.of(Map.of("text", text, "embeddings", embeddings)));

        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put("text", text);
        fieldMap.put("inference", inference);

        return Map.of(fieldName, fieldMap);
    }

    /**
     * Builds a document source map with a sparse legacy-format semantic_text field containing a
     * single chunk.
     *
     * @param fieldName   the semantic_text field name
     * @param text        the original text value
     * @param inferenceId the inference_id to embed in the inline inference structure
     * @param taskType    the task_type value in model_settings (e.g. "sparse_embedding")
     * @param embeddings  the sparse token-weight map for the single chunk
     * @return a map suitable for passing to {@code prepareIndex(...).setSource(...)}
     */
    protected Map<String, Object> buildLegacySparseDoc(
        String fieldName,
        String text,
        String inferenceId,
        String taskType,
        Map<String, Float> embeddings
    ) {
        Map<String, Object> ms = new HashMap<>();
        ms.put("task_type", taskType);

        Map<String, Object> inference = new HashMap<>();
        inference.put("inference_id", inferenceId);
        inference.put("model_settings", ms);
        inference.put("chunks", List.of(Map.of("text", text, "embeddings", embeddings)));

        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put("text", text);
        fieldMap.put("inference", inference);

        return Map.of(fieldName, fieldMap);
    }
}
