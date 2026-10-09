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
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
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
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsOptions;
import org.elasticsearch.xpack.core.inference.chunking.NoneChunkingSettings;
import org.elasticsearch.xpack.core.inference.chunking.WordBoundaryChunkingSettings;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportUpdateInferenceModelActionTests extends ESTestCase {

    private static final String INFERENCE_ENTITY_ID_VALUE = "some_inference_entity_id";
    private static final String DEFAULT_INFERENCE_ENTITY_ID_VALUE = ".some_default_inference_entity_id";
    private static final String LOCATION_INITIAL_VALUE = "some_location";
    private static final String PROJECT_ID_INITIAL_VALUE = "some_project";
    private static final String MODEL_ID_INITIAL_VALUE = "some_model";
    private static final String SERVICE_ACCOUNT_JSON_INITIAL_VALUE = "some_service_account";
    private static final String SERVICE_NAME_VALUE = "some_service_name";
    private static final int MAX_BATCH_SIZE_INITIAL_VALUE = 2;
    private static final int MAX_BATCH_SIZE_UPDATED_VALUE = 16;
    private static final InputType INPUT_TYPE_INITIAL_VALUE = InputType.SEARCH;
    private static final Boolean AUTO_TRUNCATE_INITIAL_VALUE = Boolean.FALSE;
    private static final String SERVICE_SETTINGS_KEY = "some_service_key";
    private static final String SERVICE_SETTINGS_VALUE = "some_service_value";
    private static final String TASK_SETTINGS_KEY = "some_task_key";
    private static final String TASK_SETTINGS_VALUE = "some_task_value";
    private static final String SECRET_SETTINGS_KEY = "some_secret_key";
    private static final String SECRET_SETTINGS_VALUE = "some_secret_value";
    private static final int NEW_CHUNK_SIZE_VALUE = 100;
    private static final int NEW_OVERLAP_VALUE = 25;
    private static final String UNKNOWN_SETTING_KEY = "unknown_setting";
    private static final String UNKNOWN_SETTING_VALUE = "unknown_value";
    private static final String ENDPOINT_DOES_NOT_EXIST_ERROR_PATTERN = "The inference endpoint [%s] does not exist and cannot be updated";
    private static final String DEFAULT_UPDATE_REQUEST_BODY = buildDefaultUpdateRequestBody();

    private static String buildDefaultUpdateRequestBody() {
        return Strings.format("""
            {
                "task_type": "text_embedding",
                "service_settings": {
                    "service_account_json": "some_new_service_account",
                    "max_batch_size": %d
                },
                "task_settings": {
                    "input_type": "ingest",
                    "auto_truncate": true
                }
            }
            """, MAX_BATCH_SIZE_UPDATED_VALUE);
    }

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
        // Mockito doesn't invoke interface default methods; stub onModelUpdated to call the listener
        // so the post-persist hook in TransportUpdateInferenceModelAction completes the chain.
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(service).onModelUpdated(any(), any(), any());
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

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is(Strings.format(ENDPOINT_DOES_NOT_EXIST_ERROR_PATTERN, INFERENCE_ENTITY_ID_VALUE)));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_RuntimeExceptionThrown_ThrowsSameException() {
        var simulatedException = new RuntimeException("Model not found");
        mockGetModelWithSecretsToFailWithException(simulatedException);

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var actualException = expectThrows(RuntimeException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException, sameInstance(simulatedException));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_NullReturned_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(null, INFERENCE_ENTITY_ID_VALUE);

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is(Strings.format(ENDPOINT_DOES_NOT_EXIST_ERROR_PATTERN, INFERENCE_ENTITY_ID_VALUE)));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_ServiceNotFoundInRegistry_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
        );

        mockServiceRegistryToReturnService(null);
        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is(Strings.format("Service [%s] not found", SERVICE_NAME_VALUE)));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_DefaultEndpointCheckFailed_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(DEFAULT_INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            DEFAULT_INFERENCE_ENTITY_ID_VALUE
        );
        mockServiceRegistryToReturnService(service);

        var listener = callMasterOperation(DEFAULT_INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(
            exception.getMessage(),
            is(Strings.format("Default endpoint [%s] cannot be updated", DEFAULT_INFERENCE_ENTITY_ID_VALUE))
        );
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_LicenseCheckFailed_ThrowsSecurityException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
        );
        mockServiceRegistryToReturnService(service);

        // return false for license check, so the action fails before any other processing
        mockLicenseStateIsAllowed(false);

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var exception = expectThrows(ElasticsearchSecurityException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is("current license is non-compliant for [inference]"));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_ValidationFailed_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        mockParsePersistedConfigWithSecretsToReturnModel(createModel());
        mockBuildModelFromConfigAndSecretsToReturnNewModel();

        doAnswer(invocationOnMock -> {
            ActionListener<InferenceServiceResults> listener = invocationOnMock.getArgument(6);
            listener.onFailure(new RuntimeException("validation failed"));
            return Void.TYPE;
        }).when(service)
            .infer(
                any(GoogleVertexAiEmbeddingsModel.class),
                anyList(),
                anyBoolean(),
                anyMap(),
                any(InputType.class),
                any(TimeValue.class),
                any()
            );

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("Could not complete inference endpoint creation as validation call to service threw an exception.")
        );
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_UpdatedModelIsEqualToExistingModel_ValidationAndUpdateIsSkipped() {
        var unparsedModel = new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of());
        mockGetModelWithSecretsToReturnUnparsedModel(unparsedModel, INFERENCE_ENTITY_ID_VALUE);
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        GoogleVertexAiEmbeddingsModel model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        when(service.buildModelFromConfigAndSecrets(any(ModelConfigurations.class), any(ModelSecrets.class))).thenReturn(model);
        mockModelRegistryGetModelToReturnUnparsedModel(unparsedModel);
        when(service.parsePersistedConfig(unparsedModel)).thenReturn(model);

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var response = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(response.getModel(), is(model.getConfigurations()));
        verifyNoModelRegistryMutations();
        // The refs are only set on a real update; on a no-op the hook must never be invoked.
        verify(service, never()).onModelUpdated(any(), any(), any());
    }

    public void testMasterOperation_UpdateModelTransactionFailedDueToRuntimeException_ThrowsSameException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
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

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var actualException = expectThrows(RuntimeException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException, sameInstance(simulatedException));
        verifyModelRegistryUpdateInvoked();
    }

    public void testMasterOperation_UpdateModelTransactionReturnedFalse_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        var model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        mockBuildModelFromConfigAndSecretsToReturnNewModel();
        mockServiceInferCallToReturnDenseEmbeddingFloatResults();
        mockUpdateModelWithEmbeddingDetailsToReturnSameModel();

        mockUpdateModelTransactionToReturnBoolean(false, model);

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is("Failed to update model"));
        verifyModelRegistryUpdateInvoked();
    }

    public void testMasterOperation_GetModelReturnedNull_ThrowsElasticsearchStatusException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
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

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is("Failed to update model, updated model not found"));
        verifyModelRegistryUpdateInvoked();
    }

    public void testMasterOperation_GetModelThrownException_ThrowsSameException() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
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

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var actualException = expectThrows(RuntimeException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException, sameInstance(simulatedException));
        verifyModelRegistryUpdateInvoked();
    }

    public void testMasterOperation_UnknownServiceSetting_ThrowsBadRequest() {
        assertMasterOperation_UnknownSetting_ThrowsBadRequest("""
            {
                "service_settings": {
                    "unknown_setting": "unknown_value"
                }
            }
            """);
    }

    public void testMasterOperation_UnknownServiceSetting_ThrowsWhenParserUsedForServiceSettings() {
        when(service.usesParserForServiceSettings()).thenReturn(true);
        var serviceSettings = mock(ServiceSettings.class);
        var parserException = new ElasticsearchStatusException(
            Strings.format("unknown field [%s]", UNKNOWN_SETTING_KEY),
            RestStatus.BAD_REQUEST
        );
        when(serviceSettings.updateServiceSettings(anyMap())).thenThrow(parserException);
        var model = createMockedModel(serviceSettings, mock(TaskSettings.class), null, null);

        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        when(service.parsePersistedConfig(any(UnparsedModel.class))).thenReturn(model);

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, """
            {
                "service_settings": {
                    "unknown_setting": "unknown_value"
                }
            }
            """);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception, sameInstance(parserException));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_UnknownTaskSetting_ThrowsBadRequest() {
        assertMasterOperation_UnknownSetting_ThrowsBadRequest("""
            {
                "task_settings": {
                    "unknown_setting": "unknown_value"
                }
            }
            """);
    }

    private void assertMasterOperation_UnknownSetting_ThrowsBadRequest(String requestBody) {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        mockParsePersistedConfigWithSecretsToReturnModel(createModel());

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, requestBody);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Configuration contains settings [{%s=%s}] unknown to the [%s] service",
                    UNKNOWN_SETTING_KEY,
                    UNKNOWN_SETTING_VALUE,
                    SERVICE_NAME_VALUE
                )
            )
        );
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_UnknownTaskSetting_ThrowsWhenParserUsedForTaskSettings() {
        when(service.usesParserForTaskSettings()).thenReturn(true);
        var taskSettings = mock(TaskSettings.class);
        var parserException = new ElasticsearchStatusException(
            Strings.format("unknown field [%s]", UNKNOWN_SETTING_KEY),
            RestStatus.BAD_REQUEST
        );
        when(taskSettings.updatedTaskSettings(anyMap())).thenThrow(parserException);
        var model = createMockedModel(mock(ServiceSettings.class), taskSettings, null, null);

        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        when(service.parsePersistedConfig(any(UnparsedModel.class))).thenReturn(model);

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, """
            {
                "task_settings": {
                    "unknown_setting": "unknown_value"
                }
            }
            """);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception, sameInstance(parserException));
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_UnknownChunkingSetting_ThrowsBadRequest() {
        mockGetModelWithSecretsToReturnUnparsedModel(
            new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of()),
            INFERENCE_ENTITY_ID_VALUE
        );
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        mockParsePersistedConfigWithSecretsToReturnModel(createModel());

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, """
            {
                "chunking_settings": {
                    "strategy": "sentence",
                    "max_chunk_size": 20,
                    "sentence_overlap": 0,
                    "unknown_setting": "unknown_value"
                }
            }
            """);

        var exception = expectThrows(ValidationException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.validationErrors().size(), is(1));
        assertThat(
            exception.validationErrors().getFirst(),
            is("Sentence based chunking settings can not have the following settings: [unknown_setting]")
        );
        verifyNoModelRegistryMutations();
    }

    public void testMasterOperation_UpdatesModelSettingsSuccessfully() {
        var unparsedModel = new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of());
        mockGetModelWithSecretsToReturnUnparsedModel(unparsedModel, INFERENCE_ENTITY_ID_VALUE);
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        var model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        mockBuildModelFromConfigAndSecretsToReturnNewModel();
        mockServiceInferCallToReturnDenseEmbeddingFloatResults();
        mockUpdateModelWithEmbeddingDetailsToReturnSameModel();
        mockUpdateModelTransactionToReturnBoolean(true, model);
        mockModelRegistryGetModelToReturnUnparsedModel(unparsedModel);
        when(service.parsePersistedConfig(unparsedModel)).thenReturn(model);

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);
        var response = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(response.getModel(), is(model.getConfigurations()));
        verifyModelRegistryUpdateInvoked();
    }

    public void testMasterOperation_ForwardsRequestTimeoutToValidation() {
        var unparsedModel = new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of());
        mockGetModelWithSecretsToReturnUnparsedModel(unparsedModel, INFERENCE_ENTITY_ID_VALUE);
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        var model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        mockBuildModelFromConfigAndSecretsToReturnNewModel();
        mockServiceInferCallToReturnDenseEmbeddingFloatResults();
        mockUpdateModelWithEmbeddingDetailsToReturnSameModel();
        mockUpdateModelTransactionToReturnBoolean(true, model);
        mockModelRegistryGetModelToReturnUnparsedModel(unparsedModel);
        when(service.parsePersistedConfig(unparsedModel)).thenReturn(model);

        var requestTimeout = TimeValue.timeValueSeconds(42);
        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY, requestTimeout);
        listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

        // The user-supplied timeout must be forwarded to the validation call (service.infer) rather than the task-type default.
        var timeoutCaptor = ArgumentCaptor.forClass(TimeValue.class);
        verify(service).infer(
            any(GoogleVertexAiEmbeddingsModel.class),
            anyList(),
            anyBoolean(),
            anyMap(),
            any(InputType.class),
            timeoutCaptor.capture(),
            any()
        );
        assertThat(timeoutCaptor.getValue(), is(requestTimeout));
    }

    public void testMasterOperation_SuccessfulUpdate_InvokesOnModelUpdatedWithExistingAndMergedModels() {
        var unparsedModel = new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of());
        mockGetModelWithSecretsToReturnUnparsedModel(unparsedModel, INFERENCE_ENTITY_ID_VALUE);
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);

        // existingModel and mergedModel must be distinct instances so that the update path is taken
        // (equals() returning false prevents the no-op early-return at lines 164-168).
        var existingModel = createModel(MAX_BATCH_SIZE_INITIAL_VALUE);
        var mergedModel = createModel(MAX_BATCH_SIZE_UPDATED_VALUE);
        mockParsePersistedConfigWithSecretsToReturnModel(existingModel);
        when(service.buildModelFromConfigAndSecrets(any(ModelConfigurations.class), any(ModelSecrets.class))).thenReturn(mergedModel);
        mockServiceInferCallToReturnDenseEmbeddingFloatResults();
        // Embedding-details update returns its argument unchanged, so the persisted model stays mergedModel.
        when(service.updateModelWithEmbeddingDetails(any(GoogleVertexAiEmbeddingsModel.class), eq(3))).thenAnswer(
            invocationOnMock -> invocationOnMock.getArgument(0)
        );
        mockUpdateModelTransactionToReturnBoolean(true, existingModel);
        mockModelRegistryGetModelToReturnUnparsedModel(unparsedModel);
        when(service.parsePersistedConfig(unparsedModel)).thenReturn(existingModel);

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);
        var response = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(response.getModel(), is(existingModel.getConfigurations()));
        verifyModelRegistryUpdateInvoked();

        // The hook must be invoked with exactly (existingModel, mergedModel).
        var oldCaptor = ArgumentCaptor.forClass(Model.class);
        var newCaptor = ArgumentCaptor.forClass(Model.class);
        verify(service).onModelUpdated(oldCaptor.capture(), newCaptor.capture(), any());
        assertThat(oldCaptor.getValue(), sameInstance(existingModel));
        assertThat(newCaptor.getValue(), sameInstance(mergedModel));
    }

    public void testMasterOperation_OnModelUpdatedFails_PropagatesExceptionAndSkipsGetModel() {
        var unparsedModel = new UnparsedModel(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, SERVICE_NAME_VALUE, Map.of(), Map.of());
        mockGetModelWithSecretsToReturnUnparsedModel(unparsedModel, INFERENCE_ENTITY_ID_VALUE);
        mockServiceRegistryToReturnService(service);
        mockLicenseStateIsAllowed(true);
        var model = createModel();
        mockParsePersistedConfigWithSecretsToReturnModel(model);
        mockBuildModelFromConfigAndSecretsToReturnNewModel();
        mockServiceInferCallToReturnDenseEmbeddingFloatResults();
        mockUpdateModelWithEmbeddingDetailsToReturnSameModel();
        mockUpdateModelTransactionToReturnBoolean(true, model);

        // Override the @Before no-op stub to simulate a cache-invalidation failure.
        var simulatedException = new RuntimeException("cache invalidation failed");
        doAnswer(invocation -> {
            ActionListener<Void> hookListener = invocation.getArgument(2);
            hookListener.onFailure(simulatedException);
            return null;
        }).when(service).onModelUpdated(any(), any(), any());

        var listener = callMasterOperation(INFERENCE_ENTITY_ID_VALUE, DEFAULT_UPDATE_REQUEST_BODY);

        var actualException = expectThrows(RuntimeException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException, sameInstance(simulatedException));
        // The persist step ran before the hook, so the registry update must have been invoked.
        verifyModelRegistryUpdateInvoked();
        // The failure must short-circuit the chain: the re-fetch stage must never run.
        verify(mockModelRegistry, never()).getModel(eq(INFERENCE_ENTITY_ID_VALUE), any());
    }

    private static GoogleVertexAiEmbeddingsModel createModel() {
        return createModel(MAX_BATCH_SIZE_INITIAL_VALUE);
    }

    private static GoogleVertexAiEmbeddingsModel createModel(int maxBatchSize) {
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
                maxBatchSize,
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

    private void mockGetModelWithSecretsToReturnUnparsedModel(UnparsedModel result, String inferenceEntityId) {
        doAnswer(invocationOnMock -> {
            ActionListener<UnparsedModel> listener = invocationOnMock.getArgument(1);
            listener.onResponse(result);
            return Void.TYPE;
        }).when(mockModelRegistry).getModelWithSecrets(eq(inferenceEntityId), any());
    }

    private void mockLicenseStateIsAllowed(boolean value) {
        when(licenseState.isAllowed(any(LicensedFeature.class))).thenReturn(value);
    }

    private void mockParsePersistedConfigWithSecretsToReturnModel(GoogleVertexAiEmbeddingsModel model) {
        doAnswer((Answer<Object>) invocation -> {
            UnparsedModel unparsedModel = invocation.getArgument(0);
            assertThat(unparsedModel.inferenceEntityId(), is(INFERENCE_ENTITY_ID_VALUE));
            assertThat(unparsedModel.taskType(), is(model.getTaskType()));
            return model;
        }).when(service).parsePersistedConfig(any(UnparsedModel.class));
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
            ActionListener<InferenceServiceResults> listener = invocationOnMock.getArgument(6);
            listener.onResponse(
                new DenseEmbeddingFloatResults(List.of(new EmbeddingFloatResults.Embedding(new float[] { 1.0f, 2.0f, 3.0f })))
            );
            return Void.TYPE;
        }).when(service)
            .infer(
                any(GoogleVertexAiEmbeddingsModel.class),
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

    private void verifyNoModelRegistryMutations() {
        verify(mockModelRegistry, never()).storeModel(any(), any(), any());
        verify(mockModelRegistry, never()).storeModels(any(), anyBoolean(), any(), any());
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

    private TestPlainActionFuture<UpdateInferenceModelAction.Response> callMasterOperation(String inferenceEntityId, String requestBody) {
        return callMasterOperation(inferenceEntityId, requestBody, TimeValue.timeValueSeconds(1));
    }

    private TestPlainActionFuture<UpdateInferenceModelAction.Response> callMasterOperation(
        String inferenceEntityId,
        String requestBody,
        TimeValue timeout
    ) {
        var listener = new TestPlainActionFuture<UpdateInferenceModelAction.Response>();

        action.masterOperation(
            mock(Task.class),
            new UpdateInferenceModelAction.Request(
                inferenceEntityId,
                new BytesArray(requestBody),
                XContentType.JSON,
                TaskType.TEXT_EMBEDDING,
                timeout,
                TimeValue.timeValueSeconds(30),
                TimeValue.timeValueSeconds(30)
            ),
            ClusterState.EMPTY_STATE,
            listener
        );
        return listener;
    }

    public void testValidateConsumedUpdateSettings_NullMaps_DoesNotThrow() {
        TransportUpdateInferenceModelAction.validateConsumedUpdateSettings(service, SERVICE_NAME_VALUE, null, null);
    }

    public void testValidateConsumedUpdateSettings_EmptyMaps_DoesNotThrow() {
        TransportUpdateInferenceModelAction.validateConsumedUpdateSettings(service, SERVICE_NAME_VALUE, new HashMap<>(), new HashMap<>());
    }

    public void testValidateConsumedUpdateSettings_UnknownServiceSetting_ThrowsWhenParserNotUsed() {
        var serviceSettings = new HashMap<String, Object>();
        serviceSettings.put(UNKNOWN_SETTING_KEY, UNKNOWN_SETTING_VALUE);

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportUpdateInferenceModelAction.validateConsumedUpdateSettings(service, SERVICE_NAME_VALUE, serviceSettings, null)
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Configuration contains settings [{%s=%s}] unknown to the [%s] service",
                    UNKNOWN_SETTING_KEY,
                    UNKNOWN_SETTING_VALUE,
                    SERVICE_NAME_VALUE
                )
            )
        );
    }

    /**
     * Parser-based services do not remove service settings keys from the request map, so this method must not require an
     * empty map. Unknown fields are rejected later by the service settings parser in {@link ServiceSettings#updateServiceSettings}.
     */
    public void testValidateConsumedUpdateSettings_DoesNotEnforceServiceSettingsMapEmptyWhenParserUsed() {
        when(service.usesParserForServiceSettings()).thenReturn(true);
        var serviceSettings = new HashMap<String, Object>();
        serviceSettings.put(UNKNOWN_SETTING_KEY, UNKNOWN_SETTING_VALUE);

        TransportUpdateInferenceModelAction.validateConsumedUpdateSettings(service, SERVICE_NAME_VALUE, serviceSettings, null);
    }

    public void testValidateConsumedUpdateSettings_UnknownTaskSetting_ThrowsWhenParserNotUsed() {
        when(service.usesParserForTaskSettings()).thenReturn(false);
        var taskSettings = new HashMap<String, Object>();
        taskSettings.put(UNKNOWN_SETTING_KEY, UNKNOWN_SETTING_VALUE);

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportUpdateInferenceModelAction.validateConsumedUpdateSettings(service, SERVICE_NAME_VALUE, null, taskSettings)
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Configuration contains settings [{%s=%s}] unknown to the [%s] service",
                    UNKNOWN_SETTING_KEY,
                    UNKNOWN_SETTING_VALUE,
                    SERVICE_NAME_VALUE
                )
            )
        );
    }

    /**
     * Parser-based services do not remove task settings keys from the request map, so this method must not require an
     * empty map. Unknown fields are rejected later by the task settings parser in {@link TaskSettings#updatedTaskSettings}.
     */
    public void testValidateConsumedUpdateSettings_DoesNotEnforceTaskSettingsMapEmptyWhenParserUsed() {
        when(service.usesParserForTaskSettings()).thenReturn(true);
        var taskSettings = new HashMap<String, Object>();
        taskSettings.put(UNKNOWN_SETTING_KEY, UNKNOWN_SETTING_VALUE);

        TransportUpdateInferenceModelAction.validateConsumedUpdateSettings(service, SERVICE_NAME_VALUE, null, taskSettings);
    }

    public void testCombineExistingModelConfigurationsWithNewSettings_NewConfigMapsAreNull_ReturnsExistingConfigs() {
        var serviceSettings = mock(ServiceSettings.class);
        var taskSettings = mock(TaskSettings.class);
        var chunkingSettings = mock(ChunkingSettings.class);

        var model = createMockedModel(serviceSettings, taskSettings, chunkingSettings, mock(SecretSettings.class));
        var resultModelConfigurations = action.combineExistingModelConfigurationsWithNewSettings(
            model,
            null,
            null,
            null,
            SERVICE_NAME_VALUE
        );
        verifyNoInteractions(serviceSettings);
        verifyNoInteractions(taskSettings);
        verifyNoInteractions(chunkingSettings);

        assertThat(resultModelConfigurations.getInferenceEntityId(), sameInstance(model.getInferenceEntityId()));
        assertThat(resultModelConfigurations.getTaskType(), sameInstance(model.getTaskType()));
        assertThat(resultModelConfigurations.getService(), sameInstance(SERVICE_NAME_VALUE));
        assertThat(resultModelConfigurations.getServiceSettings(), sameInstance(model.getConfigurations().getServiceSettings()));
        assertThat(resultModelConfigurations.getTaskSettings(), sameInstance(model.getConfigurations().getTaskSettings()));
        assertThat(resultModelConfigurations.getChunkingSettings(), sameInstance(model.getConfigurations().getChunkingSettings()));
    }

    public void testCombineExistingModelConfigurationsWithNewSettings_NewServiceSettings_UpdatesConfig() {
        Map<String, Object> newServiceSettingsMap = Map.of(SERVICE_SETTINGS_KEY, SERVICE_SETTINGS_VALUE);
        var originalServiceSettings = mock(ServiceSettings.class);
        var updatedServiceSettings = mock(ServiceSettings.class);
        when(originalServiceSettings.updateServiceSettings(newServiceSettingsMap)).thenReturn(updatedServiceSettings);

        var originalTaskSettings = mock(TaskSettings.class);
        var originalChunkingSettings = mock(ChunkingSettings.class);
        var model = createMockedModel(originalServiceSettings, originalTaskSettings, originalChunkingSettings, mock(SecretSettings.class));

        var resultModelConfigurations = action.combineExistingModelConfigurationsWithNewSettings(
            model,
            newServiceSettingsMap,
            null,
            null,
            SERVICE_NAME_VALUE
        );

        verify(originalServiceSettings).updateServiceSettings(newServiceSettingsMap);
        verifyNoInteractions(originalTaskSettings);
        verifyNoInteractions(originalChunkingSettings);

        assertThat(resultModelConfigurations.getInferenceEntityId(), sameInstance(model.getInferenceEntityId()));
        assertThat(resultModelConfigurations.getTaskType(), sameInstance(model.getTaskType()));
        assertThat(resultModelConfigurations.getService(), sameInstance(SERVICE_NAME_VALUE));
        assertThat(resultModelConfigurations.getServiceSettings(), sameInstance(updatedServiceSettings));
        assertThat(resultModelConfigurations.getTaskSettings(), sameInstance(originalTaskSettings));
        assertThat(resultModelConfigurations.getChunkingSettings(), sameInstance(originalChunkingSettings));
    }

    public void testCombineExistingModelConfigurationsWithNewSettings_NewTaskSettings_UpdatesConfig() {
        Map<String, Object> newTaskSettingsMap = Map.of(TASK_SETTINGS_KEY, TASK_SETTINGS_VALUE);
        var originalTaskSettings = mock(TaskSettings.class);
        var updatedTaskSettings = mock(TaskSettings.class);
        when(originalTaskSettings.updatedTaskSettings(newTaskSettingsMap)).thenReturn(updatedTaskSettings);

        var originalServiceSettings = mock(ServiceSettings.class);
        var originalChunkingSettings = mock(ChunkingSettings.class);
        var model = createMockedModel(originalServiceSettings, originalTaskSettings, originalChunkingSettings, mock(SecretSettings.class));

        var resultModelConfigurations = action.combineExistingModelConfigurationsWithNewSettings(
            model,
            null,
            newTaskSettingsMap,
            null,
            SERVICE_NAME_VALUE
        );

        verify(originalTaskSettings).updatedTaskSettings(newTaskSettingsMap);
        verifyNoInteractions(originalServiceSettings);
        verifyNoInteractions(originalChunkingSettings);

        assertThat(resultModelConfigurations.getInferenceEntityId(), sameInstance(model.getInferenceEntityId()));
        assertThat(resultModelConfigurations.getTaskType(), sameInstance(model.getTaskType()));
        assertThat(resultModelConfigurations.getService(), sameInstance(SERVICE_NAME_VALUE));
        assertThat(resultModelConfigurations.getServiceSettings(), sameInstance(originalServiceSettings));
        assertThat(resultModelConfigurations.getTaskSettings(), sameInstance(updatedTaskSettings));
        assertThat(resultModelConfigurations.getChunkingSettings(), sameInstance(originalChunkingSettings));
    }

    public void testCombineExistingModelConfigurationsWithNewSettings_NewChunkingSettings_UpdatesConfig() {
        Map<String, Object> newChunkingSettingsMap = new HashMap<>();
        newChunkingSettingsMap.put(ChunkingSettingsOptions.STRATEGY.toString(), "word");
        newChunkingSettingsMap.put(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), NEW_CHUNK_SIZE_VALUE);
        newChunkingSettingsMap.put(ChunkingSettingsOptions.OVERLAP.toString(), NEW_OVERLAP_VALUE);

        var originalServiceSettings = mock(ServiceSettings.class);
        var originalTaskSettings = mock(TaskSettings.class);
        var originalChunkingSettings = mock(ChunkingSettings.class);
        var model = createMockedModel(originalServiceSettings, originalTaskSettings, originalChunkingSettings, mock(SecretSettings.class));

        var resultModelConfigurations = action.combineExistingModelConfigurationsWithNewSettings(
            model,
            null,
            null,
            newChunkingSettingsMap,
            SERVICE_NAME_VALUE
        );

        verifyNoInteractions(originalServiceSettings);
        verifyNoInteractions(originalTaskSettings);
        verifyNoInteractions(originalChunkingSettings);

        assertThat(resultModelConfigurations.getInferenceEntityId(), sameInstance(model.getInferenceEntityId()));
        assertThat(resultModelConfigurations.getTaskType(), sameInstance(model.getTaskType()));
        assertThat(resultModelConfigurations.getService(), sameInstance(SERVICE_NAME_VALUE));
        assertThat(resultModelConfigurations.getServiceSettings(), sameInstance(originalServiceSettings));
        assertThat(resultModelConfigurations.getTaskSettings(), sameInstance(originalTaskSettings));
        // Chunking settings are *replaced* (not merged) on update: the result must be a fresh
        // ChunkingSettings built from the supplied map, regardless of the existing value.
        assertThat(
            resultModelConfigurations.getChunkingSettings(),
            is(new WordBoundaryChunkingSettings(NEW_CHUNK_SIZE_VALUE, NEW_OVERLAP_VALUE))
        );
    }

    public void testCombineExistingModelConfigurationsWithNewSettings_NewChunkingSettingsEquivalentToExisting_StillReplacesInstance() {
        // Guards the replace-not-merge contract: even with an equivalent map, the result must be
        // a freshly built instance, not the existing one.
        var existingChunkingSettings = new WordBoundaryChunkingSettings(NEW_CHUNK_SIZE_VALUE, NEW_OVERLAP_VALUE);
        Map<String, Object> equivalentChunkingSettingsMap = new HashMap<>();
        equivalentChunkingSettingsMap.put(ChunkingSettingsOptions.STRATEGY.toString(), "word");
        equivalentChunkingSettingsMap.put(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), NEW_CHUNK_SIZE_VALUE);
        equivalentChunkingSettingsMap.put(ChunkingSettingsOptions.OVERLAP.toString(), NEW_OVERLAP_VALUE);

        var model = createMockedModel(
            mock(ServiceSettings.class),
            mock(TaskSettings.class),
            existingChunkingSettings,
            mock(SecretSettings.class)
        );

        var resultModelConfigurations = action.combineExistingModelConfigurationsWithNewSettings(
            model,
            null,
            null,
            equivalentChunkingSettingsMap,
            SERVICE_NAME_VALUE
        );

        var resultChunkingSettings = resultModelConfigurations.getChunkingSettings();
        assertThat(resultChunkingSettings, is(existingChunkingSettings));
        assertThat(resultChunkingSettings, not(sameInstance(existingChunkingSettings)));
    }

    public void testCombineExistingModelConfigurationsWithNewSettings_PassesNewSettingsMapsThroughDirectlyToParsers() {
        // Request#getServiceSettings / #getTaskSettings already return freshly deep-copied maps,
        // so this consumer must pass them straight through without re-copying.
        Map<String, Object> newServiceSettingsMap = Map.of(SERVICE_SETTINGS_KEY, SERVICE_SETTINGS_VALUE);
        var originalServiceSettings = mock(ServiceSettings.class);
        var updatedServiceSettings = mock(ServiceSettings.class);
        when(originalServiceSettings.updateServiceSettings(newServiceSettingsMap)).thenReturn(updatedServiceSettings);

        Map<String, Object> newTaskSettingsMap = Map.of(TASK_SETTINGS_KEY, TASK_SETTINGS_VALUE);
        var originalTaskSettings = mock(TaskSettings.class);
        var updatedTaskSettings = mock(TaskSettings.class);
        when(originalTaskSettings.updatedTaskSettings(newTaskSettingsMap)).thenReturn(updatedTaskSettings);

        var model = createMockedModel(
            originalServiceSettings,
            originalTaskSettings,
            mock(ChunkingSettings.class),
            mock(SecretSettings.class)
        );
        var resultModelConfigurations = action.combineExistingModelConfigurationsWithNewSettings(
            model,
            newServiceSettingsMap,
            newTaskSettingsMap,
            null,
            SERVICE_NAME_VALUE
        );

        verify(originalServiceSettings).updateServiceSettings(same(newServiceSettingsMap));
        verify(originalTaskSettings).updatedTaskSettings(same(newTaskSettingsMap));

        assertThat(resultModelConfigurations.getServiceSettings(), sameInstance(updatedServiceSettings));
        assertThat(resultModelConfigurations.getTaskSettings(), sameInstance(updatedTaskSettings));
    }

    public void testCombineExistingSecretsWithNewSecrets_NewSecretsMapIsNull_ReturnsExistingSecrets() {
        var secretSettings = mock(SecretSettings.class);

        var model = createMockedModel(mock(ServiceSettings.class), mock(TaskSettings.class), mock(ChunkingSettings.class), secretSettings);
        var modelSecrets = action.combineExistingSecretsWithNewSecrets(model, null);

        assertThat(modelSecrets.getSecretSettings(), sameInstance(secretSettings));
        verifyNoInteractions(secretSettings);
    }

    public void testCombineExistingSecretsWithNewSecrets_ExistingSecretSettingsAreNull_ReturnsNull() {
        Map<String, Object> newSecretsMap = Map.of(SECRET_SETTINGS_KEY, SECRET_SETTINGS_VALUE);

        var model = createMockedModel(mock(ServiceSettings.class), mock(TaskSettings.class), mock(ChunkingSettings.class), null);
        var modelSecrets = action.combineExistingSecretsWithNewSecrets(model, newSecretsMap);

        assertThat(modelSecrets.getSecretSettings(), nullValue());
    }

    public void testCombineExistingSecretsWithNewSecrets_NewSecretSettings_UpdatesSecrets() {
        Map<String, Object> newSecretsMap = Map.of(SECRET_SETTINGS_KEY, SECRET_SETTINGS_VALUE);
        var originalSecretSettings = mock(SecretSettings.class);
        var updatedSecretSettings = mock(SecretSettings.class);
        when(originalSecretSettings.newSecretSettings(newSecretsMap)).thenReturn(updatedSecretSettings);

        var model = createMockedModel(
            mock(ServiceSettings.class),
            mock(TaskSettings.class),
            mock(ChunkingSettings.class),
            originalSecretSettings
        );
        var modelSecrets = action.combineExistingSecretsWithNewSecrets(model, newSecretsMap);

        assertThat(modelSecrets.getSecretSettings(), sameInstance(updatedSecretSettings));
        verify(originalSecretSettings).newSecretSettings(newSecretsMap);
    }

    public void testCombineExistingSecretsWithNewSecrets_PassesNewSecretsMapThroughDirectlyToParser() {
        // Request#getServiceSettings already returns a freshly deep-copied map, so this
        // consumer must pass it straight through without re-copying.
        Map<String, Object> newSecretsMap = Map.of(SECRET_SETTINGS_KEY, SECRET_SETTINGS_VALUE);
        var originalSecretSettings = mock(SecretSettings.class);
        var updatedSecretSettings = mock(SecretSettings.class);
        when(originalSecretSettings.newSecretSettings(newSecretsMap)).thenReturn(updatedSecretSettings);

        var model = createMockedModel(
            mock(ServiceSettings.class),
            mock(TaskSettings.class),
            mock(ChunkingSettings.class),
            originalSecretSettings
        );
        var modelSecrets = action.combineExistingSecretsWithNewSecrets(model, newSecretsMap);

        assertThat(modelSecrets.getSecretSettings(), sameInstance(updatedSecretSettings));
        verify(originalSecretSettings).newSecretSettings(same(newSecretsMap));
    }

    private static Model createMockedModel(
        ServiceSettings originalServiceSettings,
        TaskSettings originalTaskSettings,
        ChunkingSettings originalChunkingSettings,
        SecretSettings originalSecretSettings
    ) {
        // Mock ModelConfigurations
        var modelConfigurations = mock(ModelConfigurations.class);
        when(modelConfigurations.getServiceSettings()).thenReturn(originalServiceSettings);
        when(modelConfigurations.getTaskSettings()).thenReturn(originalTaskSettings);
        when(modelConfigurations.getChunkingSettings()).thenReturn(originalChunkingSettings);

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
