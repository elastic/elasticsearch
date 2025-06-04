/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInputTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LangIdentNeuralNetwork;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.ml.utils.TaskRetrieverTests.getTaskInfoListOfOne;
import static org.elasticsearch.xpack.ml.utils.TaskRetrieverTests.mockClientWithTasksResponse;
import static org.elasticsearch.xpack.ml.utils.TaskRetrieverTests.mockListTasksClient;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutTrainedModelActionTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testParseInferenceConfigFromModelPackage() throws IOException {

        InferenceConfig inferenceConfig = randomFrom(
            new InferenceConfig[] {
                ClassificationConfigTests.randomClassificationConfig(),
                RegressionConfigTests.randomRegressionConfig(),
                NerConfigTests.createRandom(),
                PassThroughConfigTests.createRandom(),
                TextClassificationConfigTests.createRandom(),
                FillMaskConfigTests.createRandom(),
                TextEmbeddingConfigTests.createRandom(),
                QuestionAnsweringConfigTests.createRandom(),
                TextSimilarityConfigTests.createRandom(),
                TextExpansionConfigTests.createRandom() }
        );

        Map<String, Object> inferenceConfigMap;
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = inferenceConfig.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            inferenceConfigMap = XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON).v2();
        }

        assertNotNull(inferenceConfigMap);
        InferenceConfig parsedInferenceConfig = TransportPutTrainedModelAction.parseInferenceConfigFromModelPackage(
            Collections.singletonMap(inferenceConfig.getWriteableName(), inferenceConfigMap),
            xContentRegistry()
        );

        assertEquals(inferenceConfig, parsedInferenceConfig);
    }

    public void testSetTrainedModelConfigFieldsFromPackagedModel() throws IOException {
        ModelPackageConfig packageConfig = ModelPackageConfigTests.randomModulePackageConfig();

        TrainedModelConfig.Builder trainedModelConfigBuilder = new TrainedModelConfig.Builder().setModelId(
            "." + packageConfig.getPackagedModelId()
        ).setInput(TrainedModelInputTests.createRandomInput());

        TransportPutTrainedModelAction.setTrainedModelConfigFieldsFromPackagedModel(
            trainedModelConfigBuilder,
            packageConfig,
            xContentRegistry()
        );

        TrainedModelConfig trainedModelConfig = trainedModelConfigBuilder.build();

        assertEquals(packageConfig.getModelType(), trainedModelConfig.getModelType().toString());
        assertEquals(packageConfig.getDescription(), trainedModelConfig.getDescription());
        assertEquals(packageConfig.getMetadata(), trainedModelConfig.getMetadata());
        assertEquals(packageConfig.getTags(), trainedModelConfig.getTags());
        assertEquals(packageConfig.getPrefixStrings(), trainedModelConfig.getPrefixStrings());

        // fully tested in {@link #testParseInferenceConfigFromModelPackage}
        assertNotNull(trainedModelConfig.getInferenceConfig());

        assertEquals(
            TrainedModelType.fromString(packageConfig.getModelType()).getDefaultLocation(trainedModelConfig.getModelId()),
            trainedModelConfig.getLocation()
        );
    }

    public void testCheckForExistingTaskCallsOnFailureForAnError() {
        var client = mockListTasksClient(threadPool);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> actionListener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            actionListener.onFailure(new Exception("error"));
            return Void.TYPE;
        }).when(client).execute(same(TransportListTasksAction.TYPE), any(), any());

        var responseListener = new PlainActionFuture<PutTrainedModelAction.Response>();

        TransportPutTrainedModelAction.checkForExistingModelDownloadTask(
            client,
            "inferenceEntityId",
            true,
            responseListener,
            () -> {},
            TIMEOUT
        );

        var exception = expectThrows(ElasticsearchException.class, () -> responseListener.actionGet(TIMEOUT));
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), is("Unable to retrieve task information for model id [inferenceEntityId]"));
    }

    public void testCheckForExistingTaskCallsStoreModelListenerWhenNoTasksExist() {
        var client = mockClientWithTasksResponse(Collections.emptyList(), threadPool);

        var createModelCalled = new AtomicBoolean();

        TransportPutTrainedModelAction.checkForExistingModelDownloadTask(
            client,
            "inferenceEntityId",
            true,
            new PlainActionFuture<>(),
            () -> createModelCalled.set(Boolean.TRUE),
            TIMEOUT
        );

        assertTrue(createModelCalled.get());
    }

    public void testCheckForExistingTaskThrowsNoModelFoundError() {
        var client = mockClientWithTasksResponse(getTaskInfoListOfOne(), threadPool);
        prepareGetTrainedModelResponse(client, Collections.emptyList());

        var respListener = new PlainActionFuture<PutTrainedModelAction.Response>();
        TransportPutTrainedModelAction.checkForExistingModelDownloadTask(
            client,
            "inferenceEntityId",
            true,
            respListener,
            () -> {},
            TIMEOUT
        );

        var exception = expectThrows(ElasticsearchException.class, () -> respListener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("No model information found for a concurrent create model execution for model id [inferenceEntityId]")
        );
    }

    public void testCheckForExistingTaskReturnsTask() {
        var client = mockClientWithTasksResponse(getTaskInfoListOfOne(), threadPool);

        TrainedModelConfig trainedModel = TrainedModelConfigTests.createTestInstance("inferenceEntityId")
            .setTags(Collections.singletonList("prepackaged"))
            .setModelSize(1000)
            .setEstimatedOperations(2000)
            .build();
        prepareGetTrainedModelResponse(client, List.of(trainedModel));

        var respListener = new PlainActionFuture<PutTrainedModelAction.Response>();
        TransportPutTrainedModelAction.checkForExistingModelDownloadTask(
            client,
            "inferenceEntityId",
            true,
            respListener,
            () -> {},
            TIMEOUT
        );

        var returnedModel = respListener.actionGet(TIMEOUT);
        assertThat(returnedModel.getResponse().getModelId(), is(trainedModel.getModelId()));
    }

    public void testVerifyMlNodesAndModelArchitectures_GivenIllegalArgumentException_ThenSetHeaderWarning() {

        TransportPutTrainedModelAction actionSpy = spy(createTransportPutTrainedModelAction());
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<TrainedModelConfig>> failureListener = ArgumentCaptor.forClass(ActionListener.class);
        @SuppressWarnings("unchecked")
        ActionListener<TrainedModelConfig> mockConfigToReturnListener = mock(ActionListener.class);
        TrainedModelConfig mockConfigToReturn = mock(TrainedModelConfig.class);
        doNothing().when(mockConfigToReturnListener).onResponse(any());

        doNothing().when(actionSpy).callVerifyMlNodesAndModelArchitectures(any(), any(), any(), any());
        actionSpy.verifyMlNodesAndModelArchitectures(mockConfigToReturn, null, threadPool, mockConfigToReturnListener);
        verify(actionSpy).verifyMlNodesAndModelArchitectures(any(), any(), any(), any());
        verify(actionSpy).callVerifyMlNodesAndModelArchitectures(any(), failureListener.capture(), any(), any());

        String warningMessage = "TEST HEADER WARNING";
        failureListener.getValue().onFailure(new IllegalArgumentException(warningMessage));
        assertWarnings(warningMessage);
    }

    public void testVerifyMlNodesAndModelArchitectures_GivenArchitecturesMatch_ThenTriggerOnResponse() {

        TransportPutTrainedModelAction actionSpy = spy(createTransportPutTrainedModelAction());
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<TrainedModelConfig>> successListener = ArgumentCaptor.forClass(ActionListener.class);
        @SuppressWarnings("unchecked")
        ActionListener<TrainedModelConfig> mockConfigToReturnListener = mock(ActionListener.class);
        TrainedModelConfig mockConfigToReturn = mock(TrainedModelConfig.class);

        doNothing().when(actionSpy).callVerifyMlNodesAndModelArchitectures(any(), any(), any(), any());
        actionSpy.verifyMlNodesAndModelArchitectures(mockConfigToReturn, null, threadPool, mockConfigToReturnListener);
        verify(actionSpy).callVerifyMlNodesAndModelArchitectures(any(), successListener.capture(), any(), any());

        ensureNoWarnings();
    }

    public void testValidateModelDefinition_FailsWhenLicenseIsNotSupported() throws IOException {
        ModelPackageConfig packageConfig = ModelPackageConfigTests.randomModulePackageConfig();

        TrainedModelConfig.Builder trainedModelConfigBuilder = new TrainedModelConfig.Builder().setModelId(
            "." + packageConfig.getPackagedModelId()
        ).setInput(TrainedModelInputTests.createRandomInput());

        TransportPutTrainedModelAction.setTrainedModelConfigFieldsFromPackagedModel(
            trainedModelConfigBuilder,
            packageConfig,
            xContentRegistry()
        );

        var mockTrainedModelDefinition = mock(TrainedModelDefinition.class);
        when(mockTrainedModelDefinition.getTrainedModel()).thenReturn(mock(LangIdentNeuralNetwork.class));
        var trainedModelConfig = trainedModelConfigBuilder.setLicenseLevel("basic").build();

        var mockModelInferenceConfig = spy(new LearningToRankConfig(1, List.of(), Map.of()));
        when(mockModelInferenceConfig.isTargetTypeSupported(any())).thenReturn(true);

        var mockTrainedModelConfig = spy(trainedModelConfig);
        when(mockTrainedModelConfig.getModelType()).thenReturn(TrainedModelType.LANG_IDENT);
        when(mockTrainedModelConfig.getModelDefinition()).thenReturn(mockTrainedModelDefinition);
        when(mockTrainedModelConfig.getInferenceConfig()).thenReturn(mockModelInferenceConfig);

        ActionListener<PutTrainedModelAction.Response> responseListener = ActionListener.wrap(
            response -> fail("Expected exception, but got response: " + response),
            exception -> {
                assertThat(exception, instanceOf(ElasticsearchSecurityException.class));
                assertThat(exception.getMessage(), is("Model of type [learning_to_rank] requires [ENTERPRISE] license level"));
            }
        );

        var mockClusterState = mock(ClusterState.class);

        AtomicInteger currentTime = new AtomicInteger(100);
        var mockXPackLicenseStatus = new XPackLicenseStatus(License.OperationMode.BASIC, true, "");
        var mockLicenseState = new XPackLicenseState(currentTime::get, mockXPackLicenseStatus);

        assertThat(
            TransportPutTrainedModelAction.validateModelDefinition(
                mockTrainedModelConfig,
                mockClusterState,
                mockLicenseState,
                responseListener
            ),
            is(false)
        );
    }

    private static void prepareGetTrainedModelResponse(Client client, List<TrainedModelConfig> trainedModels) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetTrainedModelsAction.Response> actionListener = (ActionListener<
                GetTrainedModelsAction.Response>) invocationOnMock.getArguments()[2];
            actionListener.onResponse(
                new GetTrainedModelsAction.Response(
                    new QueryPage<>(trainedModels, trainedModels.size(), GetDataFrameAnalyticsAction.Response.RESULTS_FIELD)
                )
            );

            return Void.TYPE;
        }).when(client).execute(same(GetTrainedModelsAction.INSTANCE), any(), any());
    }

    private TransportPutTrainedModelAction createTransportPutTrainedModelAction() {
        TransportService mockTransportService = mock(TransportService.class);
        doReturn(threadPool).when(mockTransportService).getThreadPool();
        ClusterService mockClusterService = mock(ClusterService.class);
        ActionFilters mockFilters = mock(ActionFilters.class);
        doReturn(null).when(mockFilters).filters();
        Client mockClient = mock(Client.class);
        doReturn(null).when(mockClient).settings();
        doReturn(threadPool).when(mockClient).threadPool();

        return new TransportPutTrainedModelAction(
            mockTransportService,
            mockClusterService,
            threadPool,
            null,
            mockFilters,
            mockClient,
            null,
            null
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }
}
