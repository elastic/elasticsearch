/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestRerankingServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestStreamingCompletionServiceExtension;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.inference.ModelConfigurations.SERVICE;
import static org.elasticsearch.inference.ModelConfigurations.SERVICE_SETTINGS;
import static org.elasticsearch.inference.SimilarityMeasure.COSINE;
import static org.elasticsearch.inference.SimilarityMeasure.L2_NORM;
import static org.elasticsearch.inference.TaskType.ANY;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.ELEMENT_TYPE;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings.API_KEY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class CreateInferenceEndpointIT extends ESIntegTestCase {

    public static final String INFERENCE_ID = "inference-id";
    public static final String NOT_MODIFIED_INFERENCE_ID = "not-modified-inference-id";
    public static final String SEMANTIC_TEXT_FIELD_NAME = "semantic-text-field";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class);
    }

    public void testCreateInferenceEndpoint_fails_whenSemanticTextFieldUsingTheInferenceIdExists_andTaskTypeIsIncompatible()
        throws IOException {
        modifyEndpointAndAssertFailure(true, null);
    }

    public void testCreateInferenceEndpoint_fails_whenSemanticTextFieldUsingTheInferenceIdExists_andDimensionsAreIncompatible()
        throws IOException {
        modifyEndpointAndAssertFailure(false, DIMENSIONS);
    }

    public void testCreateInferenceEndpoint_fails_whenSemanticTextFieldUsingTheInferenceIdExists_andElementTypeIsIncompatible()
        throws IOException {
        modifyEndpointAndAssertFailure(false, ELEMENT_TYPE);
    }

    public void testCreateInferenceEndpoint_fails_whenSemanticTextFieldUsingTheInferenceIdExists_andSimilarityIsIncompatible()
        throws IOException {
        modifyEndpointAndAssertFailure(false, SIMILARITY);
    }

    public void testCreateInferenceEndpoint_succeeds_whenSemanticTextFieldUsingTheInferenceIdExists_andAllSettingsAreTheSame()
        throws IOException {
        modifyEndpointAndAssertSuccess(null, true);
    }

    public void testCreateInferenceEndpoint_succeeds_whenSemanticTextFieldUsingTheInferenceIdExists_andModelIdIsDifferent()
        throws IOException {
        modifyEndpointAndAssertSuccess(MODEL_ID, true);
    }

    public void testCreateInferenceEndpoint_succeeds_whenSemanticTextFieldUsingTheInferenceIdExists_andApiKeyIsDifferent()
        throws IOException {
        modifyEndpointAndAssertSuccess(API_KEY, true);
    }

    public void testCreateInferenceEndpoint_succeeds_whenNoDocumentsUsingSemanticTextHaveBeenIndexed() throws IOException {
        String fieldToModify = randomFrom(DIMENSIONS, ELEMENT_TYPE, SIMILARITY);
        modifyEndpointAndAssertSuccess(fieldToModify, false);
    }

    public void testCreateInferenceEndpoint_succeeds_whenIndexIsCreatedBeforeInferenceEndpoint() throws IOException {
        String inferenceId = NOT_MODIFIED_INFERENCE_ID;
        String indexName = createIndexWithSemanticTextMapping(inferenceId);

        assertEndpointCreationSuccessful(randomTaskType(), getRandomServiceSettings(), inferenceId);

        IntegrationTestUtils.deleteIndex(client(), indexName);
    }

    private void modifyEndpointAndAssertFailure(boolean modifyTaskType, String settingsFieldToModify) throws IOException {
        TaskType taskType = TEXT_EMBEDDING;
        Map<String, Object> serviceSettings = getRandomServiceSettings();
        Set<String> indicesUsingInferenceId = new HashSet<>();

        String indexNotUsingInferenceId = indexDocumentsAndDeleteEndpoint(taskType, serviceSettings, indicesUsingInferenceId, true);

        ElasticsearchStatusException statusException = expectThrows(
            ElasticsearchStatusException.class,
            () -> createEndpointWithModifiedSettings(modifyTaskType, settingsFieldToModify, taskType, serviceSettings)
        );

        assertThat(statusException.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            statusException.getMessage(),
            containsString(
                "Inference endpoint ["
                    + INFERENCE_ID
                    + "] could not be created because the inference_id is being used in mappings with incompatible settings for indices: ["
            )
        );

        // Make sure we only report the indices that were using the inference ID
        indicesUsingInferenceId.forEach(index -> assertThat(statusException.getMessage(), containsString(index)));
        assertThat(statusException.getMessage(), not(containsString(indexNotUsingInferenceId)));

        indicesUsingInferenceId.forEach(index -> IntegrationTestUtils.deleteIndex(client(), index));
        IntegrationTestUtils.deleteIndex(client(), indexNotUsingInferenceId);
    }

    private void modifyEndpointAndAssertSuccess(String fieldToModify, boolean documentHasSemanticText) throws IOException {
        TaskType taskType = TEXT_EMBEDDING;
        Map<String, Object> serviceSettings = getRandomServiceSettings();
        HashSet<String> indicesUsingInferenceId = new HashSet<>();

        String indexNotUsingInferenceId = indexDocumentsAndDeleteEndpoint(
            taskType,
            serviceSettings,
            indicesUsingInferenceId,
            documentHasSemanticText
        );

        PutInferenceModelAction.Response response = createEndpointWithModifiedSettings(false, fieldToModify, taskType, serviceSettings);
        assertThat(response.getModel().getInferenceEntityId(), equalTo(INFERENCE_ID));

        indicesUsingInferenceId.forEach(index -> IntegrationTestUtils.deleteIndex(client(), index));
        IntegrationTestUtils.deleteIndex(client(), indexNotUsingInferenceId);
    }

    private String indexDocumentsAndDeleteEndpoint(
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Set<String> indicesUsingInferenceId,
        boolean documentHasSemanticText
    ) throws IOException {
        assertEndpointCreationSuccessful(taskType, serviceSettings, INFERENCE_ID);
        assertEndpointCreationSuccessful(taskType, serviceSettings, NOT_MODIFIED_INFERENCE_ID);

        // Create several indices to confirm that we can identify them all in the error message
        for (int i = 0; i < 5; ++i) {
            String indexUsingInferenceId = createIndexWithSemanticTextMapping(INFERENCE_ID, indicesUsingInferenceId);
            indexDocument(indexUsingInferenceId, documentHasSemanticText);
            indicesUsingInferenceId.add(indexUsingInferenceId);
        }

        // Also create a second endpoint which will not be deleted and recreated, and an index which is using it
        String indexNotUsingInferenceId = createIndexWithSemanticTextMapping(NOT_MODIFIED_INFERENCE_ID, indicesUsingInferenceId);
        indexDocument(indexNotUsingInferenceId, documentHasSemanticText);

        forceDeleteInferenceEndpoint(INFERENCE_ID, taskType);
        return indexNotUsingInferenceId;
    }

    private PutInferenceModelAction.Response createEndpointWithModifiedSettings(
        boolean modifyTaskType,
        String fieldToModify,
        TaskType taskType,
        Map<String, Object> serviceSettings
    ) {
        TaskType newTaskType = modifyTaskType ? randomValueOtherThan(taskType, CreateInferenceEndpointIT::randomTaskType) : taskType;
        Map<String, Object> newSettings = fieldToModify != null ? modifyServiceSettings(serviceSettings, fieldToModify) : serviceSettings;
        return createEndpoint(newTaskType, newSettings, INFERENCE_ID).actionGet(TEST_REQUEST_TIMEOUT);
    }

    private void assertEndpointCreationSuccessful(TaskType taskType, Map<String, Object> serviceSettings, String inferenceId) {
        assertThat(
            createEndpoint(taskType, serviceSettings, inferenceId).actionGet(TEST_REQUEST_TIMEOUT).getModel().getInferenceEntityId(),
            equalTo(inferenceId)
        );
    }

    private ActionFuture<PutInferenceModelAction.Response> createEndpoint(
        TaskType taskType,
        Map<String, Object> serviceSettings,
        String inferenceId
    ) {
        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field(SERVICE, getServiceForTaskType(taskType));
            builder.field(SERVICE_SETTINGS, serviceSettings);
            builder.endObject();
            content = BytesReference.bytes(builder);
        } catch (IOException ex) {
            throw new AssertionError(ex);
        }

        var request = new PutInferenceModelAction.Request(taskType, inferenceId, content, XContentType.JSON, TEST_REQUEST_TIMEOUT);
        return client().execute(PutInferenceModelAction.INSTANCE, request);
    }

    private String getServiceForTaskType(TaskType taskType) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> TestDenseInferenceServiceExtension.TestInferenceService.NAME;
            case SPARSE_EMBEDDING -> TestSparseInferenceServiceExtension.TestInferenceService.NAME;
            case RERANK -> TestRerankingServiceExtension.TestInferenceService.NAME;
            case COMPLETION, CHAT_COMPLETION -> TestStreamingCompletionServiceExtension.TestInferenceService.NAME;
            default -> throw new IllegalStateException("Unexpected value: " + taskType);
        };
    }

    private static TaskType randomTaskType() {
        EnumSet<TaskType> taskTypes = EnumSet.allOf(TaskType.class);
        taskTypes.remove(ANY);
        return randomFrom(taskTypes);
    }

    private String createIndexWithSemanticTextMapping(String inferenceId) throws IOException {
        return createIndexWithSemanticTextMapping(inferenceId, Set.of());
    }

    private String createIndexWithSemanticTextMapping(String inferenceId, Set<String> existingIndexNames) throws IOException {
        // Ensure that all index names are unique
        String indexName = randomValueOtherThanMany(
            existingIndexNames::contains,
            () -> ESTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        );
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject(ElasticsearchMappings.PROPERTIES);
        mapping.startObject(SEMANTIC_TEXT_FIELD_NAME);
        mapping.field(ElasticsearchMappings.TYPE, SemanticTextFieldMapper.CONTENT_TYPE);
        mapping.field(SemanticTextField.INFERENCE_ID_FIELD, inferenceId);
        mapping.endObject().endObject().endObject();

        assertAcked(prepareCreate(indexName).setMapping(mapping));
        return indexName;
    }

    private static void indexDocument(String indexName, boolean withSemanticText) {
        var source = new HashMap<String, Object>();
        source.put("field", "value");
        if (withSemanticText) {
            source.put(SEMANTIC_TEXT_FIELD_NAME, randomAlphaOfLength(10));
        }
        DocWriteResponse response = client().prepareIndex(indexName).setSource(source).get(TEST_REQUEST_TIMEOUT);
        assertThat(response.getResult(), is(DocWriteResponse.Result.CREATED));
        client().admin().indices().prepareRefresh(indexName).get();
    }

    private static Map<String, Object> getRandomServiceSettings() {
        Map<String, Object> settings = new HashMap<>();
        settings.put(MODEL_ID, randomIdentifier());
        settings.put(API_KEY, randomIdentifier());
        // Always use a dimension that's a multiple of 8 because the BIT element type requires that
        settings.put(DIMENSIONS, randomIntBetween(1, 32) * 8);
        ElementType elementType = randomFrom(ElementType.values());
        settings.put(ELEMENT_TYPE, elementType.toString());
        if (elementType == ElementType.BIT) {
            // The only supported similarity measure for BIT vectors is L2_NORM
            settings.put(SIMILARITY, L2_NORM.toString());
        } else if (elementType == ElementType.BYTE) {
            // DOT_PRODUCT similarity does not work with BYTE due to how TestDenseInferenceServiceExtension creates embeddings
            settings.put(SIMILARITY, randomFrom(L2_NORM, COSINE).toString());
        } else {
            settings.put(SIMILARITY, randomFrom(SimilarityMeasure.values()).toString());
        }
        return settings;
    }

    private static Map<String, Object> modifyServiceSettings(Map<String, Object> serviceSettings, String fieldToModify) {
        var newServiceSettings = new HashMap<>(serviceSettings);
        switch (fieldToModify) {
            case MODEL_ID, API_KEY -> newServiceSettings.compute(
                fieldToModify,
                (k, value) -> randomValueOtherThan(value, ESTestCase::randomIdentifier)
            );
            case DIMENSIONS -> newServiceSettings.compute(
                DIMENSIONS,
                (k, dimensions) -> randomValueOtherThan(dimensions, () -> randomIntBetween(8, 128) * 8)
            );
            case ELEMENT_TYPE -> newServiceSettings.compute(
                ELEMENT_TYPE,
                (k, elementType) -> randomValueOtherThan(elementType, () -> randomFrom(ElementType.values()).toString())
            );
            case SIMILARITY -> newServiceSettings.compute(
                SIMILARITY,
                (k, similarity) -> randomValueOtherThan(similarity, () -> randomFrom(SimilarityMeasure.values()).toString())
            );
            default -> throw new AssertionError("Invalid service settings field " + fieldToModify);
        }
        return newServiceSettings;
    }

    private void forceDeleteInferenceEndpoint(String inferenceId, TaskType taskType) {
        var request = new DeleteInferenceEndpointAction.Request(inferenceId, taskType, true, false);
        var responseFuture = client().execute(DeleteInferenceEndpointAction.INSTANCE, request);
        responseFuture.actionGet(TEST_REQUEST_TIMEOUT);
    }
}
