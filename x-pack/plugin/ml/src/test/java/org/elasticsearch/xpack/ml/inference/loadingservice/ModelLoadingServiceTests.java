/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ModelLoadingServiceTests extends ESTestCase {

    private TrainedModelProvider trainedModelProvider;
    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setUpComponents() {
        threadPool = new TestThreadPool("ModelLoadingServiceTests", new ScalingExecutorBuilder(UTILITY_THREAD_POOL_NAME,
            1, 4, TimeValue.timeValueMinutes(10), "xpack.ml.utility_thread_pool"));
        trainedModelProvider = mock(TrainedModelProvider.class);
        clusterService = mock(ClusterService.class);
        doAnswer((invocationOnMock) -> null).when(clusterService).addListener(any(ClusterStateListener.class));
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name")).build());
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testGetCachedModels() throws Exception {
        String model1 = "test-load-model-1";
        String model2 = "test-load-model-2";
        String model3 = "test-load-model-3";
        withTrainedModel(model1, 0);
        withTrainedModel(model2, 0);
        withTrainedModel(model3, 0);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider, threadPool, clusterService);

        modelLoadingService.clusterChanged(ingestChangedEvent(model1, model2, model3));

        String[] modelIds = new String[]{model1, model2, model3};
        for(int i = 0; i < 10; i++) {
            String model = modelIds[i%3];
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            modelLoadingService.getModel(model, 0, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model1), eq(0L), any());
        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model2), eq(0L), any());
        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model3), eq(0L), any());
    }

    public void testGetCachedMissingModel() throws Exception {
        String model = "test-load-cached-missing-model";
        withMissingModel(model, 0);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider, threadPool, clusterService);
        modelLoadingService.clusterChanged(ingestChangedEvent(model));

        PlainActionFuture<Model> future = new PlainActionFuture<>();
        modelLoadingService.getModel(model, 0, future);

        try {
            future.get();
            fail("Should not have succeeded in loaded model");
        } catch (Exception ex) {
            assertThat(ex.getCause().getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model, 0)));
        }

        verify(trainedModelProvider, atMost(2)).getTrainedModel(eq(model), eq(0L), any());
    }

    public void testGetMissingModel() {
        String model = "test-load-missing-model";
        withMissingModel(model, 0);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider, threadPool, clusterService);

        PlainActionFuture<Model> future = new PlainActionFuture<>();
        modelLoadingService.getModel(model, 0, future);
        try {
            future.get();
            fail("Should not have succeeded");
        } catch (Exception ex) {
            assertThat(ex.getCause().getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model, 0)));
        }
    }

    public void testGetModelEagerly() throws Exception {
        String model = "test-get-model-eagerly";
        withTrainedModel(model, 0);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider, threadPool, clusterService);

        for(int i = 0; i < 3; i++) {
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            modelLoadingService.getModel(model, 0, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(3)).getTrainedModel(eq(model), eq(0L), any());
    }

    @SuppressWarnings("unchecked")
    private void withTrainedModel(String modelId, long modelVersion) {
        TrainedModelConfig trainedModelConfig = buildTrainedModelConfigBuilder(modelId, modelVersion).build(Version.CURRENT);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onResponse(trainedModelConfig);
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq(modelId), eq(modelVersion), any());
    }

    private void withMissingModel(String modelId, long modelVersion) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onFailure(new ResourceNotFoundException(
                Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId, modelVersion)));
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq(modelId), eq(modelVersion), any());
    }

    private static TrainedModelConfig.Builder buildTrainedModelConfigBuilder(String modelId, long modelVersion) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setDefinition(TrainedModelDefinitionTests.createRandomBuilder())
            .setDescription("trained model config for test")
            .setModelId(modelId)
            .setModelType("binary_decision_tree")
            .setModelVersion(modelVersion);
    }

    private static ClusterChangedEvent ingestChangedEvent(String... modelId) throws IOException {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.changedCustomMetaDataSet()).thenReturn(Collections.singleton(IngestMetadata.TYPE));
        when(event.state()).thenReturn(buildClusterStateWithModelReferences(modelId));
        return event;
    }

    private static ClusterState buildClusterStateWithModelReferences(String... modelId) throws IOException {
        Map<String, PipelineConfiguration> configurations = new HashMap<>(modelId.length);
        for (String id : modelId) {
            configurations.put("pipeline_with_model_" + id, newConfigurationWithInferenceProcessor(id));
        }
        IngestMetadata ingestMetadata = new IngestMetadata(configurations);

        return ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
    }

    private static PipelineConfiguration newConfigurationWithInferenceProcessor(String modelId) throws IOException {
        try(XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(Collections.singletonMap("processors",
            Collections.singletonList(
                Collections.singletonMap(InferenceProcessor.TYPE,
                    Collections.singletonMap(InferenceProcessor.MODEL_ID,
                        modelId)))))) {
            return new PipelineConfiguration("pipeline_with_model_" + modelId, BytesReference.bytes(xContentBuilder), XContentType.JSON);
        }
    }
}
