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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
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
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ModelLoadingServiceTests extends ESTestCase {

    private TrainedModelProvider trainedModelProvider;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private InferenceAuditor auditor;
    private TrainedModelStatsService trainedModelStatsService;

    @Before
    public void setUpComponents() {
        threadPool = new TestThreadPool("ModelLoadingServiceTests", new ScalingExecutorBuilder(UTILITY_THREAD_POOL_NAME,
            1, 4, TimeValue.timeValueMinutes(10), "xpack.ml.utility_thread_pool"));
        trainedModelProvider = mock(TrainedModelProvider.class);
        clusterService = mock(ClusterService.class);
        auditor = mock(InferenceAuditor.class);
        trainedModelStatsService = mock(TrainedModelStatsService.class);
        doAnswer(a -> null).when(auditor).error(any(String.class), any(String.class));
        doAnswer(a -> null).when(auditor).info(any(String.class), any(String.class));
        doAnswer(a -> null).when(auditor).warning(any(String.class), any(String.class));
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
        withTrainedModel(model1, 1L);
        withTrainedModel(model2, 1L);
        withTrainedModel(model3, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            NamedXContentRegistry.EMPTY,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node");

        modelLoadingService.clusterChanged(ingestChangedEvent(model1, model2, model3));

        String[] modelIds = new String[]{model1, model2, model3};
        for(int i = 0; i < 10; i++) {
            String model = modelIds[i%3];
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            modelLoadingService.getModel(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model1), eq(true), any());
        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model2), eq(true), any());
        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model3), eq(true), any());

        // Test invalidate cache for model3
        modelLoadingService.clusterChanged(ingestChangedEvent(model1, model2));
        for(int i = 0; i < 10; i++) {
            String model = modelIds[i%3];
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            modelLoadingService.getModel(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model1), eq(true), any());
        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model2), eq(true), any());
        // It is not referenced, so called eagerly
        verify(trainedModelProvider, times(4)).getTrainedModel(eq(model3), eq(true), any());
    }

    public void testMaxCachedLimitReached() throws Exception {
        String model1 = "test-cached-limit-load-model-1";
        String model2 = "test-cached-limit-load-model-2";
        String model3 = "test-cached-limit-load-model-3";
        String[] modelIds = new String[]{model1, model2, model3};
        withTrainedModel(model1, 10L);
        withTrainedModel(model2, 6L);
        withTrainedModel(model3, 15L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            NamedXContentRegistry.EMPTY,
            trainedModelStatsService,
            Settings.builder().put(ModelLoadingService.INFERENCE_MODEL_CACHE_SIZE.getKey(), new ByteSizeValue(20L)).build(),
            "test-node");

        // We want to be notified when the models are loaded which happens in a background thread
        ModelLoadedTracker loadedTracker = new ModelLoadedTracker(Arrays.asList(modelIds));
        for (String modelId : modelIds) {
            modelLoadingService.addModelLoadedListener(modelId, loadedTracker.actionListener());
        }

        modelLoadingService.clusterChanged(ingestChangedEvent(model1, model2, model3));

        // Should have been loaded from the cluster change event but it is unknown in what order
        // the loading occurred or which models are currently in the cache due to evictions.
        // Verify that we have at least loaded all three
        assertBusy(() -> {
            verify(trainedModelProvider, times(1)).getTrainedModel(eq(model1), eq(true), any());
            verify(trainedModelProvider, times(1)).getTrainedModel(eq(model2), eq(true), any());
            verify(trainedModelProvider, times(1)).getTrainedModel(eq(model3), eq(true), any());
        });

        // all models loaded put in the cache
        assertBusy(() -> assertTrue(loadedTracker.allModelsLoaded()), 2, TimeUnit.SECONDS);

        for(int i = 0; i < 10; i++) {
            // Only reference models 1 and 2, so that cache is only invalidated once for model3 (after initial load)
            String model = modelIds[i%2];
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            modelLoadingService.getModel(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        // Depending on the order the models were first loaded in the first step
        // models 1 & 2 may have been evicted by model 3 in which case they have
        // been loaded at most twice
        verify(trainedModelProvider, atMost(2)).getTrainedModel(eq(model1), eq(true), any());
        verify(trainedModelProvider, atMost(2)).getTrainedModel(eq(model2), eq(true), any());
        // Only loaded requested once on the initial load from the change event
        verify(trainedModelProvider, times(1)).getTrainedModel(eq(model3), eq(true), any());

        // model 3 has been loaded and evicted exactly once
        verify(trainedModelStatsService, times(1)).queueStats(argThat(new ArgumentMatcher<>() {
            @Override
            public boolean matches(final Object o) {
                return ((InferenceStats)o).getModelId().equals(model3);
            }
        }), anyBoolean());

        // Load model 3, should invalidate 1 and 2
        for(int i = 0; i < 10; i++) {
            PlainActionFuture<Model> future3 = new PlainActionFuture<>();
            modelLoadingService.getModel(model3, future3);
            assertThat(future3.get(), is(not(nullValue())));
        }
        verify(trainedModelProvider, times(2)).getTrainedModel(eq(model3), eq(true), any());

        verify(trainedModelStatsService, atMost(2)).queueStats(argThat(new ArgumentMatcher<>() {
            @Override
            public boolean matches(final Object o) {
                return ((InferenceStats)o).getModelId().equals(model1);
            }
        }), anyBoolean());
        verify(trainedModelStatsService, atMost(2)).queueStats(argThat(new ArgumentMatcher<>() {
            @Override
            public boolean matches(final Object o) {
                return ((InferenceStats)o).getModelId().equals(model2);
            }
        }), anyBoolean());

        // Load model 1, should invalidate 3
        for(int i = 0; i < 10; i++) {
            PlainActionFuture<Model> future1 = new PlainActionFuture<>();
            modelLoadingService.getModel(model1, future1);
            assertThat(future1.get(), is(not(nullValue())));
        }
        verify(trainedModelProvider, atMost(3)).getTrainedModel(eq(model1), eq(true), any());
        verify(trainedModelStatsService, times(2)).queueStats(argThat(new ArgumentMatcher<>() {
            @Override
            public boolean matches(final Object o) {
                return ((InferenceStats)o).getModelId().equals(model3);
            }
        }), anyBoolean());

        // Load model 2
        for(int i = 0; i < 10; i++) {
            PlainActionFuture<Model> future2 = new PlainActionFuture<>();
            modelLoadingService.getModel(model2, future2);
            assertThat(future2.get(), is(not(nullValue())));
        }
        verify(trainedModelProvider, atMost(3)).getTrainedModel(eq(model2), eq(true), any());

        // Test invalidate cache for model3
        // Now both model 1 and 2 should fit in cache without issues
        modelLoadingService.clusterChanged(ingestChangedEvent(model1, model2));
        for(int i = 0; i < 10; i++) {
            String model = modelIds[i%3];
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            modelLoadingService.getModel(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, atMost(3)).getTrainedModel(eq(model1), eq(true), any());
        verify(trainedModelProvider, atMost(3)).getTrainedModel(eq(model2), eq(true), any());
        verify(trainedModelProvider, times(5)).getTrainedModel(eq(model3), eq(true), any());
    }


    public void testWhenCacheEnabledButNotIngestNode() throws Exception {
        String model1 = "test-uncached-not-ingest-model-1";
        withTrainedModel(model1, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            NamedXContentRegistry.EMPTY,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node");

        modelLoadingService.clusterChanged(ingestChangedEvent(false, model1));

        for(int i = 0; i < 10; i++) {
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            modelLoadingService.getModel(model1, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(10)).getTrainedModel(eq(model1), eq(true), any());
        verify(trainedModelStatsService, never()).queueStats(any(InferenceStats.class), anyBoolean());
    }

    public void testGetCachedMissingModel() throws Exception {
        String model = "test-load-cached-missing-model";
        withMissingModel(model);

        ModelLoadingService modelLoadingService =new ModelLoadingService(trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            NamedXContentRegistry.EMPTY,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node");
        modelLoadingService.clusterChanged(ingestChangedEvent(model));

        PlainActionFuture<Model> future = new PlainActionFuture<>();
        modelLoadingService.getModel(model, future);

        try {
            future.get();
            fail("Should not have succeeded in loaded model");
        } catch (Exception ex) {
            assertThat(ex.getCause().getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model)));
        }

        verify(trainedModelProvider, atMost(2)).getTrainedModel(eq(model), eq(true), any());
        verify(trainedModelStatsService, never()).queueStats(any(InferenceStats.class), anyBoolean());
    }

    public void testGetMissingModel() {
        String model = "test-load-missing-model";
        withMissingModel(model);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            NamedXContentRegistry.EMPTY,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node");

        PlainActionFuture<Model> future = new PlainActionFuture<>();
        modelLoadingService.getModel(model, future);
        try {
            future.get();
            fail("Should not have succeeded");
        } catch (Exception ex) {
            assertThat(ex.getCause().getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model)));
        }
    }

    public void testGetModelEagerly() throws Exception {
        String model = "test-get-model-eagerly";
        withTrainedModel(model, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            NamedXContentRegistry.EMPTY,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node");

        for(int i = 0; i < 3; i++) {
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            modelLoadingService.getModel(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(3)).getTrainedModel(eq(model), eq(true), any());
        verify(trainedModelStatsService, never()).queueStats(any(InferenceStats.class), anyBoolean());
    }

    @SuppressWarnings("unchecked")
    private void withTrainedModel(String modelId, long size) throws IOException {
        TrainedModelDefinition definition = mock(TrainedModelDefinition.class);
        when(definition.ramBytesUsed()).thenReturn(size);
        TrainedModelConfig trainedModelConfig = mock(TrainedModelConfig.class);
        when(trainedModelConfig.getModelDefinition()).thenReturn(definition);
        when(trainedModelConfig.getModelId()).thenReturn(modelId);
        when(trainedModelConfig.getInferenceConfig()).thenReturn(ClassificationConfig.EMPTY_PARAMS);
        when(trainedModelConfig.getInput()).thenReturn(new TrainedModelInput(Arrays.asList("foo", "bar", "baz")));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onResponse(trainedModelConfig);
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq(modelId), eq(true), any());
        doAnswer(invocationOnMock -> trainedModelConfig).when(trainedModelConfig).ensureParsedDefinition(any());
    }

    private void withMissingModel(String modelId) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onFailure(new ResourceNotFoundException(
                Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId)));
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq(modelId), eq(true), any());
    }

    private static ClusterChangedEvent ingestChangedEvent(String... modelId) throws IOException {
        return ingestChangedEvent(true, modelId);
    }

    private static ClusterChangedEvent ingestChangedEvent(boolean isIngestNode, String... modelId) throws IOException {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.changedCustomMetadataSet()).thenReturn(Collections.singleton(IngestMetadata.TYPE));
        when(event.state()).thenReturn(buildClusterStateWithModelReferences(isIngestNode, modelId));
        return event;
    }

    private static ClusterState buildClusterStateWithModelReferences(boolean isIngestNode, String... modelId) throws IOException {
        Map<String, PipelineConfiguration> configurations = new HashMap<>(modelId.length);
        for (String id : modelId) {
            configurations.put("pipeline_with_model_" + id, newConfigurationWithInferenceProcessor(id));
        }
        IngestMetadata ingestMetadata = new IngestMetadata(configurations);

        return ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .nodes(DiscoveryNodes.builder().add(
                new DiscoveryNode("node_name",
                    "node_id",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Collections.emptyMap(),
                    isIngestNode ? Collections.singleton(DiscoveryNodeRole.INGEST_ROLE) : Collections.emptySet(),
                    Version.CURRENT))
                .localNodeId("node_id")
                .build())
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

    private static class ModelLoadedTracker {
        private final Set<String> expectedModelIds;

        ModelLoadedTracker(Collection<String> expectedModelIds) {
            this.expectedModelIds = new HashSet<>(expectedModelIds);
        }

        private synchronized boolean allModelsLoaded() {
            return expectedModelIds.isEmpty();
        }

        private synchronized void onModelLoaded(Model model) {
            expectedModelIds.remove(model.getModelId());
        }

        private void onFailure(Exception e) {
            fail(e.getMessage());
        }

        ActionListener<Model> actionListener() {
            return ActionListener.wrap(this::onModelLoaded, this::onFailure);
        }
    }
}
