/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.core.XPackSettings.MACHINE_LEARNING_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.ML_NATIVE_CODE_PLATFORMS;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetInferenceModelActionTests extends ESTestCase {

    private static final String ES_ENDPOINT_ID = "es-endpoint-id";
    private static final String ELSER_ENDPOINT_ID = "elser-endpoint-id";
    private static final String TEST_ENDPOINT_ID = "test-endpoint-id";
    private static final String TEST_ENDPOINT_ID_2 = "zzz-test-endpoint-id";
    private static final String OTHER_TEST_ENDPOINT_ID = "aaa-other-test-endpoint-id";
    private static final String TEST_SERVICE_NAME = "test-service";
    private static final String OTHER_TEST_SERVICE_NAME = "other-test-service";
    private static final String UNKNOWN_SERVICE_NAME = "unknown-service";

    private static final Settings ML_DISABLED_SETTINGS = Settings.builder().put(MACHINE_LEARNING_ENABLED.getKey(), false).build();

    private ThreadPool threadPool;
    private ModelRegistry mockModelRegistry;
    private InferenceServiceRegistry mockInferenceServiceRegistry;

    @Before
    public void setUpMocks() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        mockModelRegistry = mock(ModelRegistry.class);
        mockInferenceServiceRegistry = mock(InferenceServiceRegistry.class);
    }

    @After
    public void terminateThreadPool() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testGetAllModels_SkipsElasticsearchEndpoint_WhenMlDisabled() {
        var esEndpoint = unparsedModel(ES_ENDPOINT_ID, ElasticsearchInternalService.NAME);
        var testEndpoint = unparsedModel(TEST_ENDPOINT_ID, TEST_SERVICE_NAME);
        mockGetAllModels(List.of(esEndpoint, testEndpoint));

        registerService(TEST_SERVICE_NAME);
        when(mockInferenceServiceRegistry.getService(ElasticsearchInternalService.NAME)).thenReturn(Optional.empty());

        var response = executeGetAll(ML_DISABLED_SETTINGS);

        assertThat(response.getEndpoints(), hasSize(1));
        assertThat(response.getEndpoints().get(0).getInferenceEntityId(), is(TEST_ENDPOINT_ID));
    }

    public void testGetAllModels_SkipsElserAliasEndpoint_WhenMlDisabled() {
        // An endpoint persisted before 8.13 will have "service": "elser" rather than "elasticsearch"
        var elserEndpoint = unparsedModel(ELSER_ENDPOINT_ID, ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME);
        var testEndpoint = unparsedModel(TEST_ENDPOINT_ID, TEST_SERVICE_NAME);
        mockGetAllModels(List.of(elserEndpoint, testEndpoint));

        registerService(TEST_SERVICE_NAME);
        when(mockInferenceServiceRegistry.getService(ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME)).thenReturn(Optional.empty());

        var response = executeGetAll(ML_DISABLED_SETTINGS);

        assertThat(response.getEndpoints(), hasSize(1));
        assertThat(response.getEndpoints().get(0).getInferenceEntityId(), is(TEST_ENDPOINT_ID));
    }

    public void testGetAllModels_ReturnsEmptyResponse_WhenAllEndpointsAreSkipped() {
        // Regression test: if every persisted endpoint is skipped, parsedModelsByService is empty.
        // GroupedActionListener rejects a group size of 0, so we must short-circuit before building it.
        mockGetAllModels(
            List.of(
                unparsedModel(ES_ENDPOINT_ID, ElasticsearchInternalService.NAME),
                unparsedModel(ELSER_ENDPOINT_ID, ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME)
            )
        );
        when(mockInferenceServiceRegistry.getService(ElasticsearchInternalService.NAME)).thenReturn(Optional.empty());
        when(mockInferenceServiceRegistry.getService(ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME)).thenReturn(Optional.empty());

        var response = executeGetAll(ML_DISABLED_SETTINGS);

        assertThat(response.getEndpoints(), empty());
    }

    public void testGetAllModels_ReturnsEmptyResponse_WhenRegistryReturnsNoModels() {
        mockGetAllModels(List.of());

        var response = executeGetAll(ML_DISABLED_SETTINGS);

        assertThat(response.getEndpoints(), empty());
    }

    public void testGetAllModels_ThrowsUnknownService_ForOtherMissingService() {
        // The skip only applies to the elasticsearch service/alias; other missing services must still throw.
        mockGetAllModels(List.of(unparsedModel(TEST_ENDPOINT_ID, UNKNOWN_SERVICE_NAME)));
        when(mockInferenceServiceRegistry.getService(UNKNOWN_SERVICE_NAME)).thenReturn(Optional.empty());

        var future = new TestPlainActionFuture<GetInferenceModelAction.Response>();
        createAction(ML_DISABLED_SETTINGS).doExecute(mock(Task.class), getAllRequest(), future);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), containsString("Unknown service [" + UNKNOWN_SERVICE_NAME + "]"));
    }

    public void testGetAllModels_ThrowsUnknownService_ForElasticsearchService_WhenMlEnabled() {
        // The skip must NOT apply when ML is enabled — a missing elasticsearch service is then an error.
        assumeTrue("ML native code required", ML_NATIVE_CODE_PLATFORMS.contains(Platforms.PLATFORM_NAME));

        mockGetAllModels(List.of(unparsedModel(ES_ENDPOINT_ID, ElasticsearchInternalService.NAME)));
        when(mockInferenceServiceRegistry.getService(ElasticsearchInternalService.NAME)).thenReturn(Optional.empty());

        var future = new TestPlainActionFuture<GetInferenceModelAction.Response>();
        createAction(Settings.EMPTY).doExecute(mock(Task.class), getAllRequest(), future);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), containsString("Unknown service [" + ElasticsearchInternalService.NAME + "]"));
    }

    public void testGetAllModels_ThrowsUnknownService_ForElserAlias_WhenMlEnabled() {
        // Same as above for the elser alias.
        assumeTrue("ML native code required", ML_NATIVE_CODE_PLATFORMS.contains(Platforms.PLATFORM_NAME));

        mockGetAllModels(List.of(unparsedModel(ELSER_ENDPOINT_ID, ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME)));
        when(mockInferenceServiceRegistry.getService(ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME)).thenReturn(Optional.empty());

        var future = new TestPlainActionFuture<GetInferenceModelAction.Response>();
        createAction(Settings.EMPTY).doExecute(mock(Task.class), getAllRequest(), future);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), containsString("Unknown service [" + ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME + "]"));
    }

    public void testGetAllModels_SortsResultsByInferenceEntityId() {
        // Endpoints from two services come back sorted by entity id, not grouped by service.
        mockGetAllModels(
            List.of(unparsedModel(TEST_ENDPOINT_ID_2, TEST_SERVICE_NAME), unparsedModel(OTHER_TEST_ENDPOINT_ID, OTHER_TEST_SERVICE_NAME))
        );
        registerService(TEST_SERVICE_NAME);
        registerService(OTHER_TEST_SERVICE_NAME);

        var response = executeGetAll(ML_DISABLED_SETTINGS);

        assertThat(response.getEndpoints(), hasSize(2));
        assertThat(response.getEndpoints().get(0).getInferenceEntityId(), is(OTHER_TEST_ENDPOINT_ID));
        assertThat(response.getEndpoints().get(1).getInferenceEntityId(), is(TEST_ENDPOINT_ID_2));
    }

    public void testGetModelsByTaskType_SkipsElasticsearchEndpoint_WhenMlDisabled() {
        var esEndpoint = unparsedModel(ES_ENDPOINT_ID, ElasticsearchInternalService.NAME);
        var testEndpoint = unparsedModel(TEST_ENDPOINT_ID, TEST_SERVICE_NAME);
        mockGetModelsByTaskType(List.of(esEndpoint, testEndpoint));

        registerService(TEST_SERVICE_NAME);
        when(mockInferenceServiceRegistry.getService(ElasticsearchInternalService.NAME)).thenReturn(Optional.empty());

        var future = new TestPlainActionFuture<GetInferenceModelAction.Response>();
        createAction(ML_DISABLED_SETTINGS).doExecute(
            mock(Task.class),
            new GetInferenceModelAction.Request("_all", TaskType.SPARSE_EMBEDDING, false),
            future
        );
        var response = future.actionGet(TEST_REQUEST_TIMEOUT);

        assertThat(response.getEndpoints(), hasSize(1));
        assertThat(response.getEndpoints().get(0).getInferenceEntityId(), is(TEST_ENDPOINT_ID));
    }

    public void testGetSingleModel_ThrowsUnknownService_ForElasticsearchService_WhenMlDisabled() {
        // The single-endpoint path (GET _inference/<id>) intentionally does NOT skip the error —
        // asking for one specific endpoint by id should surface the problem rather than succeed silently.
        mockGetModel(unparsedModel(ES_ENDPOINT_ID, ElasticsearchInternalService.NAME));
        when(mockInferenceServiceRegistry.getService(ElasticsearchInternalService.NAME)).thenReturn(Optional.empty());

        var future = new TestPlainActionFuture<GetInferenceModelAction.Response>();
        createAction(ML_DISABLED_SETTINGS).doExecute(
            mock(Task.class),
            new GetInferenceModelAction.Request(ES_ENDPOINT_ID, TaskType.ANY),
            future
        );

        var exception = expectThrows(ElasticsearchStatusException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), containsString("Unknown service [" + ElasticsearchInternalService.NAME + "]"));
    }

    public void testGetSingleModel_ThrowsMismatchedTaskType() {
        registerService(TEST_SERVICE_NAME);
        var unparsed = new UnparsedModel(TEST_ENDPOINT_ID, TaskType.SPARSE_EMBEDDING, TEST_SERVICE_NAME, Map.of(), Map.of());
        mockGetModel(unparsed);
        // No need to stub parsePersistedConfig — the task type mismatch check fires before it is called

        var future = new TestPlainActionFuture<GetInferenceModelAction.Response>();
        createAction(ML_DISABLED_SETTINGS).doExecute(
            mock(Task.class),
            new GetInferenceModelAction.Request(TEST_ENDPOINT_ID, TaskType.TEXT_EMBEDDING),
            future
        );

        var exception = expectThrows(ElasticsearchStatusException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
    }

    private TransportGetInferenceModelAction createAction(Settings settings) {
        return new TransportGetInferenceModelAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            threadPool,
            mockModelRegistry,
            mockInferenceServiceRegistry,
            settings
        );
    }

    private GetInferenceModelAction.Request getAllRequest() {
        return new GetInferenceModelAction.Request("_all", TaskType.ANY, false);
    }

    private GetInferenceModelAction.Response executeGetAll(Settings settings) {
        var future = new TestPlainActionFuture<GetInferenceModelAction.Response>();
        createAction(settings).doExecute(mock(Task.class), getAllRequest(), future);
        return future.actionGet(TEST_REQUEST_TIMEOUT);
    }

    private void mockGetAllModels(List<UnparsedModel> models) {
        doAnswer(invocation -> {
            ActionListener<List<UnparsedModel>> listener = invocation.getArgument(1);
            listener.onResponse(models);
            return null;
        }).when(mockModelRegistry).getAllModels(anyBoolean(), any());
    }

    private void mockGetModelsByTaskType(List<UnparsedModel> models) {
        doAnswer(invocation -> {
            ActionListener<List<UnparsedModel>> listener = invocation.getArgument(1);
            listener.onResponse(models);
            return null;
        }).when(mockModelRegistry).getModelsByTaskType(any(), any());
    }

    private void mockGetModel(UnparsedModel model) {
        doAnswer(invocation -> {
            ActionListener<UnparsedModel> listener = invocation.getArgument(1);
            listener.onResponse(model);
            return null;
        }).when(mockModelRegistry).getModel(anyString(), any());
    }

    /**
     * Registers a mock service under {@code name}. The mock stubs {@code parsePersistedConfig} to return
     * a model whose {@code inferenceEntityId} matches the entity id of the {@link UnparsedModel} passed to it,
     * and stubs {@code updateModelsWithDynamicFields} to echo models back to the listener.
     *
     * Note: {@code updateModelsWithDynamicFields} is a default interface method; Mockito does not run
     * default methods, so it must be explicitly stubbed or the listener will never complete.
     */
    private InferenceService registerService(String name) {
        var service = mock(InferenceService.class);
        when(service.name()).thenReturn(name);
        when(service.parsePersistedConfig(any())).thenAnswer(invocation -> {
            UnparsedModel unparsed = invocation.getArgument(0);
            return mockModel(unparsed.inferenceEntityId(), name);
        });
        doAnswer(invocation -> {
            List<Model> models = invocation.getArgument(0);
            ActionListener<List<Model>> listener = invocation.getArgument(1);
            listener.onResponse(models);
            return null;
        }).when(service).updateModelsWithDynamicFields(anyList(), any());
        when(mockInferenceServiceRegistry.getService(name)).thenReturn(Optional.of(service));
        return service;
    }

    private static UnparsedModel unparsedModel(String inferenceEntityId, String service) {
        return new UnparsedModel(inferenceEntityId, TaskType.SPARSE_EMBEDDING, service, Map.of(), Map.of());
    }

    private static Model mockModel(String inferenceEntityId, String serviceName) {
        var model = mock(Model.class);
        var configs = mock(ModelConfigurations.class);
        when(configs.getInferenceEntityId()).thenReturn(inferenceEntityId);
        when(configs.getService()).thenReturn(serviceName);
        when(model.getInferenceEntityId()).thenReturn(inferenceEntityId);
        when(model.getConfigurations()).thenReturn(configs);
        return model;
    }
}
