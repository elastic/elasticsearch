/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.core.inference.chunking.NoneChunkingSettings;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportUpdateInferenceModelActionTests extends ESTestCase {

    private static final String INFERENCE_ENTITY_ID_VALUE = "some_inference_entity_id";
    private static final String LOCATION_INITIAL_VALUE = "some_location";
    private static final String PROJECT_ID_INITIAL_VALUE = "some_project";
    private static final String MODEL_ID_INITIAL_VALUE = "some_model";
    private static final String SERVICE_ACCOUNT_JSON_INITIAL_VALUE = "some_service_account";
    private static final String SERVICE_NAME_VALUE = "some_service_name";
    private static final int MAX_BATCH_SIZE_INITIAL_VALUE = 2;
    private static final InputType INPUT_TYPE_INITIAL_VALUE = InputType.SEARCH;
    private static final Boolean AUTO_TRUNCATE_INITIAL_VALUE = Boolean.FALSE;

    private MockLicenseState licenseState;
    private TransportUpdateInferenceModelAction action;
    private ModelRegistry mockModelRegistry;
    private InferenceServiceRegistry mockInferenceServiceRegistry;
    private InferenceService service;

    @Before
    public void createAction() throws Exception {
        super.setUp();
        mockModelRegistry = mock(ModelRegistry.class);
        mockInferenceServiceRegistry = mock(InferenceServiceRegistry.class);
        licenseState = MockLicenseState.createMock();
        service = mock(InferenceService.class);
        when(service.name()).thenReturn(SERVICE_NAME_VALUE);
        action = new TransportUpdateInferenceModelAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            licenseState,
            mockModelRegistry,
            mockInferenceServiceRegistry,
            mock(Client.class),
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
    }

    public void testMasterOperation_ResourceNotFoundExceptionThrown_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToFailWithException(new ResourceNotFoundException("Model not found"));

        var listener = callMasterOperationWithActionFuture();

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(
            exception.getMessage(),
            is(Strings.format("The inference endpoint [%s] does not exist and cannot be updated", INFERENCE_ENTITY_ID_VALUE))
        );
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_RuntimeExceptionThrown_ThrowsSameException() {
        var simulatedException = new RuntimeException("Model not found");
        mockGetModelWithSecretsToFailWithException(simulatedException);

        var listener = callMasterOperationWithActionFuture();

        var actualException = expectThrows(RuntimeException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException, sameInstance(simulatedException));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_NullReturned_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(null);

        var listener = callMasterOperationWithActionFuture();

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(
            exception.getMessage(),
            is(Strings.format("The inference endpoint [%s] does not exist and cannot be updated", INFERENCE_ENTITY_ID_VALUE))
        );
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_ServiceNotFoundInRegistry_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of())
        );

        mockServiceRegistryToReturnService(null);
        var listener = callMasterOperationWithActionFuture();

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is(Strings.format("Service [%s] not found", SERVICE_NAME_VALUE)));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_LicenseCheckFailed_ThrowsSecurityException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of())
        );
        mockServiceRegistryToReturnService(service);

        // return false for license check, so the action fails before any other processing
        mockLicenseStateIsAllowed(false);

        var listener = callMasterOperationWithActionFuture();

        var exception = expectThrows(ElasticsearchSecurityException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is("current license is non-compliant for [inference]"));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_ValidationFailed_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of())
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        mockParsePersistedConfigWithSecretsToReturnModel(createModel());
        mockBuildModelFromConfigAndSecretsToReturnNewModel();

        doAnswer(invocationOnMock -> {
            ActionListener<InferenceServiceResults> listener = invocationOnMock.getArgument(9);
            listener.onFailure(new RuntimeException("validation failed"));
            return Void.TYPE;
        }).when(service)
            .infer(
                any(GoogleVertexAiEmbeddingsModel.class),
                isNull(),
                isNull(),
                isNull(),
                anyList(),
                anyBoolean(),
                anyMap(),
                any(InputType.class),
                any(TimeValue.class),
                any()
            );

        var listener = callMasterOperationWithActionFuture();

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("Could not complete inference endpoint creation as validation call to service threw an exception.")
        );
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_UpdatedModelIsEqualToExistingModel_ValidationAndUpdateIsSkipped() {
        var unparsedModel = new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of());
        mockGetModelWithSecretsToReturnUnparsedModel(unparsedModel);
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        GoogleVertexAiEmbeddingsModel model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        when(service.buildModelFromConfigAndSecrets(any(ModelConfigurations.class), any(ModelSecrets.class))).thenReturn(model);
        mockModelRegistryGetModelToReturnUnparsedModel(unparsedModel);
        mockParsePersistedConfigToReturnModel(model);

        var listener = callMasterOperationWithActionFuture();

        var response = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(response.getModel(), is(model.getConfigurations()));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_UpdateModelTransactionFailedDueToRuntimeException_ThrowsSameException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of())
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        var model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        mockBuildModelFromConfigAndSecretsToReturnNewModel();
        mockServiceInferCallToReturnDenseEmbeddingFloatResults();
        mockUpdateModelWithEmbeddingDetailsToReturnSameModel();

        var simulatedException = new RuntimeException("update failed");
        doAnswer(invocationOnMock -> {
            ActionListener<Boolean> listener = invocationOnMock.getArgument(2);
            listener.onFailure(simulatedException);
            return Void.TYPE;
        }).when(mockModelRegistry).updateModelTransaction(any(GoogleVertexAiEmbeddingsModel.class), eq(model), any());

        var listener = callMasterOperationWithActionFuture();

        var actualException = expectThrows(RuntimeException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException, sameInstance(simulatedException));
        verifyModelRegistryUpdateInvoked();
    }

    public void testMasterOperation_UpdateModelTransactionReturnedFalse_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of())
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        var model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        mockBuildModelFromConfigAndSecretsToReturnNewModel();
        mockServiceInferCallToReturnDenseEmbeddingFloatResults();
        mockUpdateModelWithEmbeddingDetailsToReturnSameModel();

        mockUpdateModelTransactionToReturnBoolean(false, model);

        var listener = callMasterOperationWithActionFuture();

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is("Failed to update model"));
        verifyModelRegistryUpdateInvoked();
    }

    public void testMasterOperation_GetModelReturnedNull_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of())
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        var model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        mockBuildModelFromConfigAndSecretsToReturnNewModel();
        mockServiceInferCallToReturnDenseEmbeddingFloatResults();
        mockUpdateModelWithEmbeddingDetailsToReturnSameModel();
        mockUpdateModelTransactionToReturnBoolean(true, model);

        mockModelRegistryGetModelToReturnUnparsedModel(null);

        var listener = callMasterOperationWithActionFuture();

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is("Failed to update model, updated model not found"));
        verifyModelRegistryUpdateInvoked();
    }

    public void testMasterOperation_GetModelThrownException_ThrowsSameException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of())
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        var model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        mockBuildModelFromConfigAndSecretsToReturnNewModel();
        mockServiceInferCallToReturnDenseEmbeddingFloatResults();
        mockUpdateModelWithEmbeddingDetailsToReturnSameModel();
        mockUpdateModelTransactionToReturnBoolean(true, model);

        var simulatedException = new RuntimeException("updated model not found");
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onFailure(simulatedException);
            return Void.TYPE;
        }).when(mockModelRegistry).getModel(eq(INFERENCE_ENTITY_ID_VALUE), any());

        var listener = callMasterOperationWithActionFuture();

        var actualException = expectThrows(RuntimeException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException, sameInstance(simulatedException));
        verifyModelRegistryUpdateInvoked();
    }

    public void testMasterOperation_UpdatesModelSettingsSuccessfully() {
        var unparsedModel = new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of());
        mockGetModelWithSecretsToReturnUnparsedModel(unparsedModel);
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        var model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        mockBuildModelFromConfigAndSecretsToReturnNewModel();
        mockServiceInferCallToReturnDenseEmbeddingFloatResults();
        mockUpdateModelWithEmbeddingDetailsToReturnSameModel();
        mockUpdateModelTransactionToReturnBoolean(true, model);
        mockModelRegistryGetModelToReturnUnparsedModel(unparsedModel);
        mockParsePersistedConfigToReturnModel(model);

        var listener = callMasterOperationWithActionFuture();
        var response = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(response.getModel(), is(model.getConfigurations()));
        verifyModelRegistryUpdateInvoked();
    }

    private static GoogleVertexAiEmbeddingsModel createModel() {
        return new GoogleVertexAiEmbeddingsModel(
            INFERENCE_ENTITY_ID_VALUE,
            TaskType.TEXT_EMBEDDING,
            SERVICE_NAME_VALUE,
            new GoogleVertexAiEmbeddingsServiceSettings(
                LOCATION_INITIAL_VALUE,
                PROJECT_ID_INITIAL_VALUE,
                MODEL_ID_INITIAL_VALUE,
                Boolean.FALSE,
                null,
                null,
                MAX_BATCH_SIZE_INITIAL_VALUE,
                null,
                null
            ),
            new GoogleVertexAiEmbeddingsTaskSettings(AUTO_TRUNCATE_INITIAL_VALUE, INPUT_TYPE_INITIAL_VALUE),
            NoneChunkingSettings.INSTANCE,
            new GoogleVertexAiSecretSettings(new SecureString(SERVICE_ACCOUNT_JSON_INITIAL_VALUE.toCharArray()))
        );
    }

    private void mockGetModelWithSecretsToFailWithException(RuntimeException exception) {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onFailure(exception);
            return Void.TYPE;
        }).when(mockModelRegistry).getModelWithSecrets(eq(INFERENCE_ENTITY_ID_VALUE), any());
    }

    private void mockGetModelWithSecretsToReturnUnparsedModel(UnparsedModel result) {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(result);
            return Void.TYPE;
        }).when(mockModelRegistry).getModelWithSecrets(eq(INFERENCE_ENTITY_ID_VALUE), any());
    }

    private void mockLicenseStateIsAllowed(boolean value) {
        when(licenseState.isAllowed(any(LicensedFeature.class))).thenReturn(value);
    }

    private void mockParsePersistedConfigWithSecretsToReturnModel(GoogleVertexAiEmbeddingsModel model) {
        when(service.parsePersistedConfigWithSecrets(eq(INFERENCE_ENTITY_ID_VALUE), eq(TaskType.TEXT_EMBEDDING), anyMap(), anyMap()))
            .thenReturn(model);
    }

    private void mockServiceRegistryToReturnService(InferenceService service) {
        when(mockInferenceServiceRegistry.getService(SERVICE_NAME_VALUE)).thenReturn(Optional.ofNullable(service));
    }

    private void mockBuildModelFromConfigAndSecretsToReturnNewModel() {
        when(service.buildModelFromConfigAndSecrets(any(ModelConfigurations.class), any(ModelSecrets.class))).thenAnswer(
            invocationOnMock -> new GoogleVertexAiEmbeddingsModel(
                (ModelConfigurations) invocationOnMock.getArgument(0),
                invocationOnMock.getArgument(1)
            )
        );
    }

    private void mockServiceInferCallToReturnDenseEmbeddingFloatResults() {
        doAnswer(invocationOnMock -> {
            ActionListener<InferenceServiceResults> listener = invocationOnMock.getArgument(9);
            listener.onResponse(
                new DenseEmbeddingFloatResults(List.of(new EmbeddingFloatResults.Embedding(new float[] { 1.0f, 2.0f, 3.0f })))
            );
            return Void.TYPE;
        }).when(service)
            .infer(
                any(GoogleVertexAiEmbeddingsModel.class),
                isNull(),
                isNull(),
                isNull(),
                anyList(),
                anyBoolean(),
                anyMap(),
                any(InputType.class),
                any(TimeValue.class),
                any()
            );
    }

    private void mockUpdateModelWithEmbeddingDetailsToReturnSameModel() {
        when(service.updateModelWithEmbeddingDetails(any(GoogleVertexAiEmbeddingsModel.class), eq(3))).thenAnswer(
            invocationOnMock -> invocationOnMock.getArgument(0)
        );
    }

    private void mockUpdateModelTransactionToReturnBoolean(boolean result, GoogleVertexAiEmbeddingsModel model) {
        doAnswer(invocationOnMock -> {
            ActionListener<Boolean> listener = invocationOnMock.getArgument(2);
            listener.onResponse(result);
            return Void.TYPE;
        }).when(mockModelRegistry).updateModelTransaction(any(GoogleVertexAiEmbeddingsModel.class), eq(model), any());
    }

    private void mockParsePersistedConfigToReturnModel(GoogleVertexAiEmbeddingsModel model) {
        when(service.parsePersistedConfig(eq(INFERENCE_ENTITY_ID_VALUE), eq(TaskType.TEXT_EMBEDDING), anyMap())).thenReturn(model);
    }

    private void verifyNoModelRegistryMutations() {
        verify(mockModelRegistry, never()).storeModel(any(), any(), any());
        verify(mockModelRegistry, never()).storeModels(any(), any(), any());
        verify(mockModelRegistry, never()).updateModelTransaction(any(), any(), any());
    }

    private void verifyModelRegistryUpdateInvoked() {
        verify(mockModelRegistry).updateModelTransaction(any(), any(), any());
    }

    private void mockModelRegistryGetModelToReturnUnparsedModel(UnparsedModel result) {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(result);
            return Void.TYPE;
        }).when(mockModelRegistry).getModel(eq(INFERENCE_ENTITY_ID_VALUE), any());
    }

    private PlainActionFuture<UpdateInferenceModelAction.Response> callMasterOperationWithActionFuture() {
        var listener = new PlainActionFuture<UpdateInferenceModelAction.Response>();

        var requestBody = """
            {
                "task_type": "text_embedding",
                "service_settings": {
                    "service_account_json": "some_new_service_account",
                    "max_batch_size": 16
                },
                "task_settings": {
                    "input_type": "ingest",
                    "auto_truncate": true
                }
            }""";

        action.masterOperation(
            mock(Task.class),
            new UpdateInferenceModelAction.Request(
                INFERENCE_ENTITY_ID_VALUE,
                new BytesArray(requestBody),
                XContentType.JSON,
                TaskType.TEXT_EMBEDDING,
                TimeValue.timeValueSeconds(1)
            ),
            ClusterState.EMPTY_STATE,
            listener
        );
        return listener;
    }

    public void testCombineExistingModelConfigurationsWithNewSettings_NewConfigMapsAreNull_ReturnsExistingConfigs() {
        var serviceSettings = mock(ServiceSettings.class);
        var taskSettings = mock(TaskSettings.class);

        var model = createMockedModel(serviceSettings, taskSettings, mock(SecretSettings.class));
        var resultModelConfigurations = action.combineExistingModelConfigurationsWithNewSettings(
            model,
            new UpdateInferenceModelAction.Settings(null, null, TaskType.TEXT_EMBEDDING),
            SERVICE_NAME_VALUE
        );
        verifyNoInteractions(serviceSettings);
        verifyNoInteractions(taskSettings);

        assertThat(resultModelConfigurations.getInferenceEntityId(), sameInstance(model.getInferenceEntityId()));
        assertThat(resultModelConfigurations.getTaskType(), sameInstance(model.getTaskType()));
        assertThat(resultModelConfigurations.getService(), sameInstance(SERVICE_NAME_VALUE));
        assertThat(resultModelConfigurations.getServiceSettings(), sameInstance(model.getConfigurations().getServiceSettings()));
        assertThat(resultModelConfigurations.getTaskSettings(), sameInstance(model.getConfigurations().getTaskSettings()));
        assertThat(resultModelConfigurations.getChunkingSettings(), sameInstance(model.getConfigurations().getChunkingSettings()));
    }

    public void testCombineExistingModelConfigurationsWithNewSettings_NewServiceAndTaskSettings_UpdatesConfig() {
        Map<String, Object> newServiceSettingsMap = Map.of("some_service_key", "some_service_value");
        var originalServiceSettings = mock(ServiceSettings.class);
        var updatedServiceSettings = mock(ServiceSettings.class);
        when(originalServiceSettings.updateServiceSettings(newServiceSettingsMap)).thenReturn(updatedServiceSettings);

        Map<String, Object> newTaskSettingsMap = Map.of("some_task_key", "some_task_value");
        var originalTaskSettings = mock(TaskSettings.class);
        var updatedTaskSettings = mock(TaskSettings.class);
        when(originalTaskSettings.updatedTaskSettings(newTaskSettingsMap)).thenReturn(updatedTaskSettings);

        var model = createMockedModel(originalServiceSettings, originalTaskSettings, mock(SecretSettings.class));
        var resultModelConfigurations = action.combineExistingModelConfigurationsWithNewSettings(
            model,
            new UpdateInferenceModelAction.Settings(newServiceSettingsMap, newTaskSettingsMap, TaskType.TEXT_EMBEDDING),
            SERVICE_NAME_VALUE
        );

        verify(originalServiceSettings).updateServiceSettings(newServiceSettingsMap);
        verify(originalTaskSettings).updatedTaskSettings(newTaskSettingsMap);

        assertThat(resultModelConfigurations.getInferenceEntityId(), sameInstance(model.getInferenceEntityId()));
        assertThat(resultModelConfigurations.getTaskType(), sameInstance(model.getTaskType()));
        assertThat(resultModelConfigurations.getService(), sameInstance(SERVICE_NAME_VALUE));
        assertThat(resultModelConfigurations.getServiceSettings(), sameInstance(updatedServiceSettings));
        assertThat(resultModelConfigurations.getTaskSettings(), sameInstance(updatedTaskSettings));
        assertThat(resultModelConfigurations.getChunkingSettings(), sameInstance(model.getConfigurations().getChunkingSettings()));
    }

    public void testCombineExistingSecretsWithNewSecrets_NewSecretsMapIsNull_ReturnsExistingSecrets() {
        var secretSettings = mock(SecretSettings.class);

        var model = createMockedModel(mock(ServiceSettings.class), mock(TaskSettings.class), secretSettings);
        var modelSecrets = action.combineExistingSecretsWithNewSecrets(model, null);

        assertThat(modelSecrets.getSecretSettings(), sameInstance(secretSettings));
        verifyNoInteractions(secretSettings);
    }

    public void testCombineExistingSecretsWithNewSecrets_ExistingSecretSettingsAreNull_ReturnsNull() {
        Map<String, Object> newSecretsMap = Map.of("some_secret_key", "some_secret_value");

        var model = createMockedModel(mock(ServiceSettings.class), mock(TaskSettings.class), null);
        var modelSecrets = action.combineExistingSecretsWithNewSecrets(model, newSecretsMap);

        assertThat(modelSecrets.getSecretSettings(), nullValue());
    }

    public void testCombineExistingSecretsWithNewSecrets_NewSecretSettings_UpdatesSecrets() {
        Map<String, Object> newSecretsMap = Map.of("some_secret_key", "some_secret_value");
        var originalSecretSettings = mock(SecretSettings.class);
        var updatedSecretSettings = mock(SecretSettings.class);
        when(originalSecretSettings.newSecretSettings(newSecretsMap)).thenReturn(updatedSecretSettings);

        var model = createMockedModel(mock(ServiceSettings.class), mock(TaskSettings.class), originalSecretSettings);
        var modelSecrets = action.combineExistingSecretsWithNewSecrets(model, newSecretsMap);

        assertThat(modelSecrets.getSecretSettings(), sameInstance(updatedSecretSettings));
        verify(originalSecretSettings).newSecretSettings(newSecretsMap);
    }

    private static Model createMockedModel(
        ServiceSettings originalServiceSettings,
        TaskSettings originalTaskSettings,
        SecretSettings originalSecretSettings
    ) {
        // Mock ModelConfigurations
        var modelConfigurations = mock(ModelConfigurations.class);
        when(modelConfigurations.getServiceSettings()).thenReturn(originalServiceSettings);
        when(modelConfigurations.getTaskSettings()).thenReturn(originalTaskSettings);
        when(modelConfigurations.getChunkingSettings()).thenReturn(mock(ChunkingSettings.class));

        // Mock Model
        var model = mock(Model.class);
        when(model.getInferenceEntityId()).thenReturn(INFERENCE_ENTITY_ID_VALUE);
        when(model.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);
        when(model.getConfigurations()).thenReturn(modelConfigurations);
        when(model.getSecretSettings()).thenReturn(originalSecretSettings);

        return model;
    }

    public void testValidateResolvedTaskType_MatchingTaskType_Success() {
        var model = mock(Model.class);
        when(model.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);

        TransportUpdateInferenceModelAction.validateResolvedTaskType(model, TaskType.TEXT_EMBEDDING);
    }

    public void testValidateResolvedTaskType_MismatchingTaskType_Failure() {
        var model = mock(Model.class);
        when(model.getTaskType()).thenReturn(TaskType.RERANK);

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportUpdateInferenceModelAction.validateResolvedTaskType(model, TaskType.TEXT_EMBEDDING)
        );

        assertThat(exception.getMessage(), is("Task type must match the task type of the existing endpoint"));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
    }
}
