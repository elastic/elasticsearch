/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
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
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInputTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportPutTrainedModelActionTests extends ESTestCase {

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
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE
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

        // fully tested in {@link #testParseInferenceConfigFromModelPackage}
        assertNotNull(trainedModelConfig.getInferenceConfig());

        assertEquals(
            TrainedModelType.fromString(packageConfig.getModelType()).getDefaultLocation(trainedModelConfig.getModelId()),
            trainedModelConfig.getLocation()
        );
    }

    public void testCheckForExistingTaskCallsOnFailureForAnError() {
        var client = mock(Client.class);
        setupClusterClient(client);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> actionListener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            actionListener.onFailure(new Exception("error"));
            return Void.TYPE;
        }).when(client).execute(same(ListTasksAction.INSTANCE), any(), any());

        var responseListener = new PlainActionFuture<PutTrainedModelAction.Response>();

        TransportPutTrainedModelAction.checkForExistingTask(client, "modelId", true, responseListener, new PlainActionFuture<Void>());

        var exception = expectThrows(ElasticsearchException.class, responseListener::actionGet);
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), is("Unable to retrieve task information for model id [modelId]"));
    }

    public void testCheckForExistingTaskCallsStoreModelListenerWhenNoTasksExist() {
        var client = mock(Client.class);
        prepareTasksResponse(client, Collections.emptyList());

        var storeListener = new PlainActionFuture<Void>();

        TransportPutTrainedModelAction.checkForExistingTask(client, "modelId", true, new PlainActionFuture<>(), storeListener);

        assertThat(storeListener.actionGet(), nullValue());
    }

    public void testCheckForExistingTaskThrowsNoModelFoundError() {
        var client = mock(Client.class);
        prepareTasksResponse(client, List.of(getTaskInfo()));
        prepareGetTrainedModelResponse(client, Collections.emptyList());

        var respListener = new PlainActionFuture<PutTrainedModelAction.Response>();
        TransportPutTrainedModelAction.checkForExistingTask(client, "modelId", true, respListener, new PlainActionFuture<>());

        var exception = expectThrows(ElasticsearchException.class, respListener::actionGet);
        assertThat(exception.getMessage(), is("No model information found for a concurrent create model execution for model id [modelId]"));
    }

    public void testCheckForExistingTaskReturnsTask() {
        var client = mock(Client.class);
        prepareTasksResponse(client, List.of(getTaskInfo()));

        TrainedModelConfig trainedModel = TrainedModelConfigTests.createTestInstance("modelId")
            .setTags(Collections.singletonList("prepackaged"))
            .setModelSize(1000)
            .setEstimatedOperations(2000)
            .build();
        prepareGetTrainedModelResponse(client, List.of(trainedModel));

        var respListener = new PlainActionFuture<PutTrainedModelAction.Response>();
        TransportPutTrainedModelAction.checkForExistingTask(client, "modelId", true, respListener, new PlainActionFuture<>());

        var returnedModel = respListener.actionGet();
        assertThat(returnedModel.getResponse().getModelId(), is(trainedModel.getModelId()));
    }

    private static void prepareTasksResponse(Client client, List<TaskInfo> taskInfo) {
        setupClusterClient(client);

        var listTasksResponse = mock(ListTasksResponse.class);
        when(listTasksResponse.getTasks()).thenReturn(taskInfo);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> actionListener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            actionListener.onResponse(listTasksResponse);

            return Void.TYPE;
        }).when(client).execute(same(ListTasksAction.INSTANCE), any(), any());
    }

    private static void setupClusterClient(Client client) {
        var cluster = mock(ClusterAdminClient.class);
        var admin = mock(AdminClient.class);

        when(client.admin()).thenReturn(admin);
        when(admin.cluster()).thenReturn(cluster);
        when(cluster.prepareListTasks()).thenReturn(new ListTasksRequestBuilder(client, ListTasksAction.INSTANCE));
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

    private static TaskInfo getTaskInfo() {
        return new TaskInfo(
            new TaskId("test", 123),
            "test",
            "test",
            "test",
            null,
            0,
            0,
            true,
            false,
            new TaskId("test", 456),
            Collections.emptyMap()
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }
}
