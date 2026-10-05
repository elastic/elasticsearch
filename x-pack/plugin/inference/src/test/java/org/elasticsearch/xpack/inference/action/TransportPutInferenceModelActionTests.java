/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandEmbeddingModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalTextEmbeddingServiceSettings;
import org.junit.Before;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.xpack.inference.InferenceFeatures.EMBEDDING_TASK_TYPE;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutInferenceModelActionTests extends ESTestCase {

    private static final String INFERENCE_ID = "test-inference-id";
    private static final String MODEL_ID = "test-model-id";
    private static final String PUT_REQUEST_BODY = Strings.format("""
        {
          "service": "elasticsearch",
          "service_settings": {
            "model_id": "%s",
            "num_allocations": 1,
            "num_threads": 1
          }
        }
        """, MODEL_ID);

    private ModelRegistry mockModelRegistry;
    private ElasticsearchInternalService mockService;
    private TransportPutInferenceModelAction action;

    @Before
    public void createAction() throws Exception {
        super.setUp();

        var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(any(LicensedFeature.class))).thenReturn(true);

        mockModelRegistry = mock(ModelRegistry.class);
        when(mockModelRegistry.containsPreconfiguredInferenceEndpointId(any())).thenReturn(false);

        mockService = mock(ElasticsearchInternalService.class);
        when(mockService.getMinimalSupportedVersion()).thenReturn(TransportVersion.minimumCompatible());

        var mockServiceRegistry = mock(InferenceServiceRegistry.class);
        when(mockServiceRegistry.getService(ElasticsearchInternalService.NAME)).thenReturn(Optional.of(mockService));

        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        var clusterServiceMock = mock(ClusterService.class);
        when(clusterServiceMock.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(InferencePlugin.SKIP_VALIDATE_AND_START))
        );
        when(clusterServiceMock.state()).thenReturn(ClusterState.EMPTY_STATE);

        var featureServiceMock = mock(FeatureService.class);
        when(featureServiceMock.clusterHasFeature(any(), any())).thenReturn(true);

        action = new TransportPutInferenceModelAction(
            mock(TransportService.class),
            clusterServiceMock,
            mockThreadPool,
            mock(ActionFilters.class),
            licenseState,
            mockModelRegistry,
            mockServiceRegistry,
            Settings.EMPTY,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            featureServiceMock
        );
    }

    public void testResolveTaskType() {

        assertEquals(TaskType.SPARSE_EMBEDDING, ServiceUtils.resolveTaskType(TaskType.SPARSE_EMBEDDING, null));
        assertEquals(TaskType.SPARSE_EMBEDDING, ServiceUtils.resolveTaskType(TaskType.ANY, TaskType.SPARSE_EMBEDDING.toString()));

        var e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.resolveTaskType(TaskType.ANY, null));
        assertThat(e.getMessage(), containsString("model is missing required setting [task_type]"));

        e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.resolveTaskType(TaskType.ANY, TaskType.ANY.toString()));
        assertThat(e.getMessage(), containsString("task_type [any] is not valid type for inference"));

        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> ServiceUtils.resolveTaskType(TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING.toString())
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Cannot resolve conflicting task_type parameter in the request URL [sparse_embedding] and the request body [text_embedding]"
            )
        );
    }

    public void testEmbeddingTaskType_withUnsupportedNodeFeature_returnsStatusException() throws Exception {
        var featureServiceMock = mock(FeatureService.class);
        var clusterServiceMock = mock(ClusterService.class);
        when(featureServiceMock.clusterHasFeature(any(), eq(EMBEDDING_TASK_TYPE))).thenReturn(false);
        when(clusterServiceMock.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(InferencePlugin.SKIP_VALIDATE_AND_START))
        );
        var localAction = new TransportPutInferenceModelAction(
            mock(),
            clusterServiceMock,
            mock(),
            mock(),
            mock(),
            mock(),
            mock(),
            Settings.EMPTY,
            mock(),
            featureServiceMock
        );

        var taskMock = mock(Task.class);
        var request = new PutInferenceModelAction.Request(
            TaskType.EMBEDDING,
            randomIdentifier(),
            new BytesArray(""),
            XContentType.JSON,
            null
        );
        var state = mock(ClusterState.class);
        var listener = new TestPlainActionFuture<PutInferenceModelAction.Response>();

        localAction.masterOperation(taskMock, request, state, listener);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is(
                "task_type ["
                    + TaskType.EMBEDDING
                    + "] is not supported by all nodes in the cluster; "
                    + "please complete upgrades before creating an endpoint with this task_type"
            )
        );
    }

    /**
     * When validation deploys the model and a subsequent failure occurs (e.g. storing the endpoint
     * fails), the action must stop the deployment it started and propagate the original error.
     */
    public void testCreate_WhenDeploymentStarted_AndStoreModelFails_StopsDeployment_AndPropagatesOriginalError() throws Exception {
        var model = createCustomElandEmbeddingModel();
        stubParseRequestConfigToReturnModel(model);
        stubServiceStartToSucceed(model);
        stubServiceInferToReturnDenseEmbedding();
        when(mockService.updateModelWithEmbeddingDetails(eq(model), anyInt())).thenReturn(model);

        var storeException = new RuntimeException("store failed");
        stubStoreModelToFail(storeException);
        stubServiceStopToSucceed(model);

        var listener = callMasterOperation();

        var actualException = expectThrows(RuntimeException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException, sameInstance(storeException));
        verify(mockService).stop(eq(model), any());
    }

    /**
     * When both the post-validation failure and the subsequent stop call fail, the action must wrap
     * them into a 500 error with the stop exception as the cause and the original failure suppressed.
     */
    public void testCreate_WhenDeploymentStarted_AndStopFails_ReturnsWrappedInternalServerError() throws Exception {
        var model = createCustomElandEmbeddingModel();
        stubParseRequestConfigToReturnModel(model);
        stubServiceStartToSucceed(model);
        stubServiceInferToReturnDenseEmbedding();
        when(mockService.updateModelWithEmbeddingDetails(eq(model), anyInt())).thenReturn(model);

        var storeException = new RuntimeException("store failed");
        stubStoreModelToFail(storeException);

        var stopException = new RuntimeException("stop failed");
        stubServiceStopToFail(model, stopException);

        var listener = callMasterOperation();

        var actualException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(
            actualException.getMessage(),
            containsString("Failed to create the inference endpoint [" + INFERENCE_ID + "] and the model deployment could not be stopped")
        );
        // The stop exception is the cause; the original store failure is suppressed on it
        assertThat(actualException.getCause(), sameInstance(stopException));
        assertThat(stopException.getSuppressed()[0], sameInstance(storeException));
        verify(mockService).stop(eq(model), any());
    }

    /**
     * When no deployment is started during validation (e.g. a non-text-embedding elasticsearch model),
     * a downstream failure must not trigger a stop call.
     */
    public void testCreate_WhenDeploymentNotStarted_DoesNotStopDeployment() throws Exception {
        // Return a model that is not a CustomElandEmbeddingModel so requiresDeploymentValidation=false,
        // meaning validation returns deploymentStarted=false without calling service.start().
        var nonDeployedModel = mock(Model.class);
        when(nonDeployedModel.getInferenceEntityId()).thenReturn(INFERENCE_ID);
        stubParseRequestConfigToReturnModel(nonDeployedModel);

        var storeException = new RuntimeException("store failed");
        stubStoreModelToFail(storeException);

        var listener = callMasterOperation();

        var actualException = expectThrows(RuntimeException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException, sameInstance(storeException));
        verify(mockService, never()).stop(any(), any());
    }

    /**
     * When parsing the request config fails, the validation result reference is never populated,
     * so stopModelDeploymentIfStarted sees a null result and must not call stop.
     */
    public void testCreate_WhenParseRequestConfigFails_DoesNotStopDeployment() throws Exception {
        var parseException = new RuntimeException("parse failed");
        doAnswer(invocation -> {
            ActionListener<Model> listener = invocation.getArgument(3);
            listener.onFailure(parseException);
            return null;
        }).when(mockService).parseRequestConfig(eq(INFERENCE_ID), any(TaskType.class), anyMap(), any());

        var listener = callMasterOperation();

        var actualException = expectThrows(RuntimeException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(actualException, sameInstance(parseException));
        verify(mockService, never()).stop(any(), any());
        verify(mockModelRegistry, never()).storeModel(any(), any(), any());
    }

    private CustomElandEmbeddingModel createCustomElandEmbeddingModel() {
        var mockServiceSettings = mock(ElasticsearchInternalTextEmbeddingServiceSettings.class);
        when(mockServiceSettings.modelId()).thenReturn(MODEL_ID);
        when(mockServiceSettings.dimensionsSetByUser()).thenReturn(false);
        return new CustomElandEmbeddingModel(
            INFERENCE_ID,
            TaskType.TEXT_EMBEDDING,
            ElasticsearchInternalService.NAME,
            mockServiceSettings,
            ChunkingSettingsTests.createRandomChunkingSettings()
        );
    }

    private void stubParseRequestConfigToReturnModel(Model model) {
        doAnswer(invocation -> {
            ActionListener<Model> listener = invocation.getArgument(3);
            listener.onResponse(model);
            return null;
        }).when(mockService).parseRequestConfig(any(), any(TaskType.class), anyMap(), any());
    }

    private void stubServiceStartToSucceed(Model model) {
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(mockService).start(eq(model), any(), any());
    }

    private void stubServiceInferToReturnDenseEmbedding() {
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(6);
            listener.onResponse(
                new DenseEmbeddingFloatResults(List.of(new EmbeddingFloatResults.Embedding(new float[] { 1.0f, 2.0f, 3.0f })))
            );
            return null;
        }).when(mockService).infer(any(), anyList(), anyBoolean(), anyMap(), any(InputType.class), any(), any());
    }

    private void stubStoreModelToFail(Exception e) {
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onFailure(e);
            return null;
        }).when(mockModelRegistry).storeModel(any(), any(), any());
    }

    private void stubServiceStopToSucceed(Model model) {
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(mockService).stop(eq(model), any());
    }

    private void stubServiceStopToFail(Model model, Exception e) {
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onFailure(e);
            return null;
        }).when(mockService).stop(eq(model), any());
    }

    private TestPlainActionFuture<PutInferenceModelAction.Response> callMasterOperation() throws Exception {
        var listener = new TestPlainActionFuture<PutInferenceModelAction.Response>();
        action.masterOperation(
            mock(Task.class),
            new PutInferenceModelAction.Request(
                TaskType.TEXT_EMBEDDING,
                INFERENCE_ID,
                new BytesArray(PUT_REQUEST_BODY),
                XContentType.JSON,
                null
            ),
            ClusterState.EMPTY_STATE,
            listener
        );
        return listener;
    }
}
