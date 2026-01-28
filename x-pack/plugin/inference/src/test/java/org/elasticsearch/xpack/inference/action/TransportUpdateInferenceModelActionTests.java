/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
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
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransportUpdateInferenceModelActionTests extends ESTestCase {

    private static final String INFERENCE_ENTITY_ID_VALUE = "some_inference_entity_id";
    private static final String LOCATION_INITIAL_VALUE = "some_location";
    private static final String PROJECT_ID_INITIAL_VALUE = "some_project";
    private static final String MODEL_ID_INITIAL_VALUE = "some_model";
    private static final String SERVICE_ACCOUNT_JSON_INITIAL_VALUE = "some_service_account";
    private static final String SERVICE_NAME_VALUE = "some_service_name";

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

    public void testUpdateInferenceModel_ThrowsSecurityExceptionWhenLicenseCheckFails() {
        mockFailedLicenseCheck();

        var listener = new PlainActionFuture<UpdateInferenceModelAction.Response>();
        var requestBody = """
            {
                "service_settings": {
                    "some_key": "some_value"
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

        var exception = expectThrows(ElasticsearchSecurityException.class, () -> listener.actionGet(TimeValue.timeValueSeconds(5)));
        assertThat(exception.getMessage(), is("current license is non-compliant for [inference]"));
    }

    private void mockFailedLicenseCheck() {
        mockGetModelWithSecretsCall(SERVICE_NAME_VALUE);

        when(service.name()).thenReturn(SERVICE_NAME_VALUE);
        mockGetServiceCall(SERVICE_NAME_VALUE);

        // return false for license check, so the action fails before any other processing
        when(licenseState.isAllowed(any())).thenReturn(false);
    }

    private void mockGetServiceCall(String serviceName) {
        when(mockInferenceServiceRegistry.getService(serviceName)).thenReturn(Optional.of(service));
    }

    private void mockGetModelWithSecretsCall(String serviceName) {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, serviceName, Map.of(), Map.of()));
            return Void.TYPE;
        }).when(mockModelRegistry).getModelWithSecrets(eq(INFERENCE_ENTITY_ID_VALUE), any());
    }

    public void testUpdateInferenceModel_UpdatesModelSettingsSuccessfully() {
        mockSuccessfulModelUpdate();

        var listener = new PlainActionFuture<UpdateInferenceModelAction.Response>();

        var requestBody = """
            {
                "task_type": "text_embedding",
                "service_settings": {
                    "location": "some_new_location",
                    "project_id": "some_new_project",
                    "model_id": "some_new_model",
                    "uri": "some_new_uri",
                    "service_account_json": "some_new_service_account"
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
        var response = listener.actionGet(TimeValue.timeValueSeconds(5));
        assertThat(response.getModel(), is(notNullValue()));
    }

    private void mockSuccessfulModelUpdate() {
        mockGetModelWithSecretsCall(GoogleVertexAiService.NAME);

        when(service.name()).thenReturn(GoogleVertexAiService.NAME);
        mockGetServiceCall(GoogleVertexAiService.NAME);
        var model = createModel();

        when(service.parsePersistedConfigWithSecrets(eq(INFERENCE_ENTITY_ID_VALUE), eq(TaskType.TEXT_EMBEDDING), any(), any())).thenReturn(
            model
        );

        when(service.buildModelFromConfigAndSecrets(any(ModelConfigurations.class), any(ModelSecrets.class))).thenAnswer(
            invocationOnMock -> new GoogleVertexAiEmbeddingsModel(
                (ModelConfigurations) invocationOnMock.getArgument(0),
                invocationOnMock.getArgument(1)
            )
        );
        doAnswer(invocationOnMock -> {
            ActionListener<InferenceServiceResults> listener = invocationOnMock.getArgument(9);
            listener.onResponse(
                new DenseEmbeddingFloatResults(List.of(new EmbeddingFloatResults.Embedding(new float[] { 1.0f, 2.0f, 3.0f })))
            );
            return Void.TYPE;
        }).when(service)
            .infer(
                any(GoogleVertexAiEmbeddingsModel.class),
                any(),
                any(),
                any(),
                any(),
                anyBoolean(),
                anyMap(),
                any(InputType.class),
                any(TimeValue.class),
                any()
            );
        when(service.updateModelWithEmbeddingDetails(any(), anyInt())).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
        doAnswer(invocationOnMock -> {
            ActionListener<Boolean> listener = invocationOnMock.getArgument(2);
            listener.onResponse(true);
            return Void.TYPE;
        }).when(mockModelRegistry).updateModelTransaction(any(), any(), any());

        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(
                new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, GoogleVertexAiService.NAME, Map.of(), Map.of())
            );
            return Void.TYPE;
        }).when(mockModelRegistry).getModel(anyString(), any());

        when(service.parsePersistedConfig(eq(INFERENCE_ENTITY_ID_VALUE), eq(TaskType.TEXT_EMBEDDING), any())).thenReturn(model);

        when(licenseState.isAllowed(any())).thenReturn(true);
    }

    private static GoogleVertexAiEmbeddingsModel createModel() {
        return new GoogleVertexAiEmbeddingsModel(
            INFERENCE_ENTITY_ID_VALUE,
            TaskType.TEXT_EMBEDDING,
            GoogleVertexAiService.NAME,
            new GoogleVertexAiEmbeddingsServiceSettings(
                LOCATION_INITIAL_VALUE,
                PROJECT_ID_INITIAL_VALUE,
                MODEL_ID_INITIAL_VALUE,
                Boolean.FALSE,
                null,
                null,
                null,
                null,
                null
            ),
            new GoogleVertexAiEmbeddingsTaskSettings(Boolean.FALSE, InputType.INGEST),
            NoneChunkingSettings.INSTANCE,
            new GoogleVertexAiSecretSettings(new SecureString(SERVICE_ACCOUNT_JSON_INITIAL_VALUE.toCharArray()))
        );
    }

    public void testCombineExistingModelConfigurationsWithNewSettings_NewConfigMapsAreNull_ReturnsExistingConfigs() {
        var serviceSettings = mock(ServiceSettings.class);
        var taskSettings = mock(TaskSettings.class);
        var secretSettings = mock(SecretSettings.class);

        var model = createMockedModel(serviceSettings, taskSettings, secretSettings);
        var resultModelConfigurations = action.combineExistingModelConfigurationsWithNewSettings(
            model,
            new UpdateInferenceModelAction.Settings(null, null, TaskType.TEXT_EMBEDDING),
            SERVICE_NAME_VALUE
        );

        verify(model).getConfigurations();
        verify(model).getInferenceEntityId();
        verify(model).getTaskType();
        verifyNoMoreInteractions(model);

        verify(model.getConfigurations()).getServiceSettings();
        verify(model.getConfigurations()).getTaskSettings();
        verify(model.getConfigurations()).getChunkingSettings();

        verifyNoInteractions(serviceSettings);
        verifyNoInteractions(taskSettings);
        verifyNoInteractions(model.getConfigurations().getChunkingSettings());
        verifyNoInteractions(secretSettings);

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

        var secretSettings = mock(SecretSettings.class);

        var model = createMockedModel(originalServiceSettings, originalTaskSettings, secretSettings);
        var resultModelConfigurations = action.combineExistingModelConfigurationsWithNewSettings(
            model,
            new UpdateInferenceModelAction.Settings(newServiceSettingsMap, newTaskSettingsMap, TaskType.TEXT_EMBEDDING),
            SERVICE_NAME_VALUE
        );

        verify(model).getConfigurations();
        verify(model).getInferenceEntityId();
        verify(model).getTaskType();
        verifyNoMoreInteractions(model);

        verify(model.getConfigurations()).getServiceSettings();
        verify(model.getConfigurations()).getTaskSettings();
        verify(model.getConfigurations()).getChunkingSettings();

        verify(originalServiceSettings).updateServiceSettings(newServiceSettingsMap);
        verifyNoMoreInteractions(originalServiceSettings);

        verify(originalTaskSettings).updatedTaskSettings(newTaskSettingsMap);
        verifyNoMoreInteractions(originalTaskSettings);

        verifyNoInteractions(updatedServiceSettings);
        verifyNoInteractions(updatedTaskSettings);
        verifyNoInteractions(model.getConfigurations().getChunkingSettings());
        verifyNoInteractions(secretSettings);

        assertThat(resultModelConfigurations.getInferenceEntityId(), sameInstance(model.getInferenceEntityId()));
        assertThat(resultModelConfigurations.getTaskType(), sameInstance(model.getTaskType()));
        assertThat(resultModelConfigurations.getService(), sameInstance(SERVICE_NAME_VALUE));
        assertThat(resultModelConfigurations.getServiceSettings(), sameInstance(updatedServiceSettings));
        assertThat(resultModelConfigurations.getTaskSettings(), sameInstance(updatedTaskSettings));
        assertThat(resultModelConfigurations.getChunkingSettings(), sameInstance(model.getConfigurations().getChunkingSettings()));
    }

    public void testCombineExistingSecretsWithNewSecrets_NewSecretsMapIsNull_ReturnsExistingSecrets() {
        var serviceSettings = mock(ServiceSettings.class);
        var taskSettings = mock(TaskSettings.class);
        var secretSettings = mock(SecretSettings.class);

        var model = createMockedModel(serviceSettings, taskSettings, secretSettings);
        var modelSecrets = action.combineExistingSecretsWithNewSecrets(model, null);

        assertThat(modelSecrets.getSecretSettings(), sameInstance(secretSettings));

        verify(model).getSecretSettings();
        verifyNoMoreInteractions(model);

        verifyNoInteractions(secretSettings);
        verifyNoInteractions(serviceSettings);
        verifyNoInteractions(taskSettings);
    }

    public void testCombineExistingSecretsWithNewSecrets_ExistingSecretSettingsAreNull_ReturnsNull() {
        var serviceSettings = mock(ServiceSettings.class);
        var taskSettings = mock(TaskSettings.class);

        Map<String, Object> newSecretsMap = Map.of("some_secret_key", "some_secret_value");

        var model = createMockedModel(serviceSettings, taskSettings, null);
        var modelSecrets = action.combineExistingSecretsWithNewSecrets(model, newSecretsMap);

        assertThat(modelSecrets.getSecretSettings(), nullValue());

        verify(model).getSecretSettings();
        verifyNoMoreInteractions(model);

        verifyNoInteractions(serviceSettings);
        verifyNoInteractions(taskSettings);
    }

    public void testCombineExistingSecretsWithNewSecrets_NewSecretSettings_UpdatesSecrets() {
        var serviceSettings = mock(ServiceSettings.class);
        var taskSettings = mock(TaskSettings.class);

        Map<String, Object> newSecretsMap = Map.of("some_secret_key", "some_secret_value");
        var originalSecretSettings = mock(SecretSettings.class);
        var updatedSecretSettings = mock(SecretSettings.class);
        when(originalSecretSettings.newSecretSettings(newSecretsMap)).thenReturn(updatedSecretSettings);

        var model = createMockedModel(serviceSettings, taskSettings, originalSecretSettings);
        var modelSecrets = action.combineExistingSecretsWithNewSecrets(model, newSecretsMap);

        assertThat(modelSecrets.getSecretSettings(), sameInstance(updatedSecretSettings));

        verify(originalSecretSettings).newSecretSettings(newSecretsMap);
        verifyNoMoreInteractions(originalSecretSettings);

        verify(model).getSecretSettings();
        verifyNoMoreInteractions(model);

        verifyNoInteractions(serviceSettings);
        verifyNoInteractions(taskSettings);
        verifyNoInteractions(updatedSecretSettings);
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

        verify(model).getTaskType();
    }

    public void testValidateResolvedTaskType_MismatchingTaskType_Failure() {
        var model = mock(Model.class);
        when(model.getTaskType()).thenReturn(TaskType.RERANK);

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportUpdateInferenceModelAction.validateResolvedTaskType(model, TaskType.TEXT_EMBEDDING)
        );

        verify(model).getTaskType();

        assertThat(exception.getMessage(), is("Task type must match the task type of the existing endpoint"));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
    }
}
