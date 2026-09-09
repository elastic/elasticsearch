/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceServiceTests extends ESTestCase {
    private TestThreadPool threadPool;

    @Before
    public void setThreadPool() {
        threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                "inference_utility",
                between(1, 10),
                1024,
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testResolveInferenceIds() throws Exception {
        InferenceService inferenceService = inferenceService();
        List<String> inferenceIds = List.of("rerank-plan");
        SetOnce<InferenceResolution> inferenceResolutionSetOnce = new SetOnce<>();

        inferenceService.resolveInferenceIds(
            inferenceIds,
            assertAnswerUsingSearchCoordinationThreadPool(ActionListener.wrap(inferenceResolutionSetOnce::set, ESTestCase::fail))
        );

        assertBusy(() -> {
            InferenceResolution inferenceResolution = inferenceResolutionSetOnce.get();
            assertNotNull(inferenceResolution);
            assertThat(inferenceResolution.resolvedInferences(), contains(new ResolvedInference("rerank-plan", TaskType.RERANK)));
            assertThat(inferenceResolution.hasError(), equalTo(false));
        });
    }

    public void testResolveMultipleInferenceIds() throws Exception {
        InferenceService inferenceService = inferenceService();
        List<String> inferenceIds = List.of("rerank-plan", "rerank-plan", "completion-plan");
        SetOnce<InferenceResolution> inferenceResolutionSetOnce = new SetOnce<>();

        inferenceService.resolveInferenceIds(
            inferenceIds,
            assertAnswerUsingSearchCoordinationThreadPool(ActionListener.wrap(inferenceResolutionSetOnce::set, ESTestCase::fail))
        );

        assertBusy(() -> {
            InferenceResolution inferenceResolution = inferenceResolutionSetOnce.get();
            assertNotNull(inferenceResolution);

            assertThat(
                inferenceResolution.resolvedInferences(),
                contains(
                    new ResolvedInference("rerank-plan", TaskType.RERANK),
                    new ResolvedInference("completion-plan", TaskType.COMPLETION)
                )
            );
            assertThat(inferenceResolution.hasError(), equalTo(false));
        });
    }

    public void testResolveMissingInferenceIds() throws Exception {
        InferenceService inferenceService = inferenceService();
        List<String> inferenceIds = List.of("missing-inference-id");

        SetOnce<InferenceResolution> inferenceResolutionSetOnce = new SetOnce<>();

        inferenceService.resolveInferenceIds(
            inferenceIds,
            assertAnswerUsingSearchCoordinationThreadPool(ActionListener.wrap(inferenceResolutionSetOnce::set, ESTestCase::fail))
        );

        assertBusy(() -> {
            InferenceResolution inferenceResolution = inferenceResolutionSetOnce.get();
            assertNotNull(inferenceResolution);

            assertThat(inferenceResolution.resolvedInferences(), empty());
            assertThat(inferenceResolution.hasError(), equalTo(true));
            assertThat(inferenceResolution.getError("missing-inference-id"), equalTo("inference endpoint not found"));
        });
    }

    public void testDenseVectorBatchSizeUpdatesWhileRunning() {
        RunningInference running = runningInference();
        assertThat(
            running.service().inferenceSettings().denseVectorBatchSize(),
            equalTo(InferenceSettings.DENSE_VECTOR_DEFAULT_BATCH_SIZE)
        );

        int updatedBatchSize = between(1, InferenceSettings.DENSE_VECTOR_MAX_BATCH_SIZE);
        applyBatchSize(running.clusterSettings(), updatedBatchSize);

        assertThat(running.service().inferenceSettings().denseVectorBatchSize(), equalTo(updatedBatchSize));
    }

    /**
     * Removing the override falls back to the default: the batch size starts at the default, changes to a custom value, then
     * returns to the default once the override is cleared (equivalent to setting it to {@code null}).
     */
    public void testDenseVectorBatchSizeRevertsToDefault() {
        RunningInference running = runningInference();
        assertThat(
            running.service().inferenceSettings().denseVectorBatchSize(),
            equalTo(InferenceSettings.DENSE_VECTOR_DEFAULT_BATCH_SIZE)
        );

        int customBatchSize = randomValueOtherThan(
            InferenceSettings.DENSE_VECTOR_DEFAULT_BATCH_SIZE,
            () -> between(1, InferenceSettings.DENSE_VECTOR_MAX_BATCH_SIZE)
        );
        applyBatchSize(running.clusterSettings(), customBatchSize);
        assertThat(running.service().inferenceSettings().denseVectorBatchSize(), equalTo(customBatchSize));

        running.clusterSettings().applySettings(Settings.EMPTY);
        assertThat(
            running.service().inferenceSettings().denseVectorBatchSize(),
            equalTo(InferenceSettings.DENSE_VECTOR_DEFAULT_BATCH_SIZE)
        );
    }

    /**
     * Consecutive updates each take effect. Every value differs from the previous one, so the running service reflects the latest
     * value after each update.
     */
    public void testDenseVectorBatchSizeUpdatesRepeatedly() {
        RunningInference running = runningInference();

        int previousBatchSize = InferenceSettings.DENSE_VECTOR_DEFAULT_BATCH_SIZE;
        int iterations = between(3, 6);
        for (int i = 0; i < iterations; i++) {
            int nextBatchSize = randomValueOtherThan(previousBatchSize, () -> between(1, InferenceSettings.DENSE_VECTOR_MAX_BATCH_SIZE));
            applyBatchSize(running.clusterSettings(), nextBatchSize);
            assertThat(running.service().inferenceSettings().denseVectorBatchSize(), equalTo(nextBatchSize));
            previousBatchSize = nextBatchSize;
        }
    }

    /**
     * A live update to an out-of-range value is rejected, and the value already in effect is left unchanged.
     */
    public void testDenseVectorBatchSizeRejectsOutOfRangeUpdate() {
        RunningInference running = runningInference();

        assertOutOfRangeUpdateRejected(running.clusterSettings(), 0, "must be >= 1");
        assertOutOfRangeUpdateRejected(running.clusterSettings(), -1, "must be >= 1");
        assertOutOfRangeUpdateRejected(
            running.clusterSettings(),
            InferenceSettings.DENSE_VECTOR_MAX_BATCH_SIZE + between(1, 100),
            "must be <= " + InferenceSettings.DENSE_VECTOR_MAX_BATCH_SIZE
        );

        assertThat(
            running.service().inferenceSettings().denseVectorBatchSize(),
            equalTo(InferenceSettings.DENSE_VECTOR_DEFAULT_BATCH_SIZE)
        );
    }

    private static void applyBatchSize(ClusterSettings clusterSettings, int batchSize) {
        clusterSettings.applySettings(
            Settings.builder().put(InferenceSettings.DENSE_VECTOR_BATCH_SIZE_SETTING.getKey(), batchSize).build()
        );
    }

    private static void assertOutOfRangeUpdateRejected(ClusterSettings clusterSettings, int batchSize, String boundMessage) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> applyBatchSize(clusterSettings, batchSize));
        assertThat(e.getMessage(), containsString(InferenceSettings.DENSE_VECTOR_BATCH_SIZE_SETTING.getKey()));
        assertThat(e.getMessage(), containsString(boundMessage));
    }

    @SuppressWarnings("unchecked")
    private Client mockClient() {
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(i -> {
            Runnable sendResponse = () -> {
                GetInferenceModelAction.Request request = i.getArgument(1, GetInferenceModelAction.Request.class);
                ActionListener<ActionResponse> listener = (ActionListener<ActionResponse>) i.getArgument(2, ActionListener.class);
                ActionResponse response = getInferenceModelResponse(request);

                if (response == null) {
                    listener.onFailure(new ResourceNotFoundException("inference endpoint not found"));
                } else {
                    listener.onResponse(response);
                }
            };

            threadPool.schedule(sendResponse, TimeValue.timeValueNanos(between(1, 1_000)), threadPool.executor("inference_utility"));

            return null;
        }).when(client).execute(eq(GetInferenceModelAction.INSTANCE), any(), any());
        return client;
    }

    private <T> ActionListener<T> assertAnswerUsingSearchCoordinationThreadPool(ActionListener<T> actionListener) {
        return ActionListener.runBefore(actionListener, () -> ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH_COORDINATION));
    }

    private static ActionResponse getInferenceModelResponse(GetInferenceModelAction.Request request) {
        GetInferenceModelAction.Response response = mock(GetInferenceModelAction.Response.class);

        if (request.getInferenceEntityId().equals("rerank-plan")) {
            when(response.getEndpoints()).thenReturn(List.of(mockModelConfig("rerank-plan", TaskType.RERANK)));
            return response;
        }

        if (request.getInferenceEntityId().equals("completion-plan")) {
            when(response.getEndpoints()).thenReturn(List.of(mockModelConfig("completion-plan", TaskType.COMPLETION)));
            return response;
        }

        return null;
    }

    private InferenceService inferenceService() {
        return runningInference().service();
    }

    /**
     * An {@link InferenceService} wired to a live {@link ClusterSettings}, so tests can apply setting updates and observe the
     * service react to them.
     */
    private record RunningInference(InferenceService service, ClusterSettings clusterSettings) {}

    private RunningInference runningInference() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(InferenceSettings.getSettings()));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

        return new RunningInference(new InferenceService(mockClient(), clusterService), clusterSettings);
    }

    private static ModelConfigurations mockModelConfig(String inferenceId, TaskType taskType) {
        return new ModelConfigurations(inferenceId, taskType, randomIdentifier(), mock(ServiceSettings.class));
    }
}
