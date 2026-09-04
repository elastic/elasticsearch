/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.RerankAction;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class InferenceService {

    /** Value of the {@code X-Elastic-Product-Use-Case} header for ESQL requests. */
    public static final String ESQL_PRODUCT_USE_CASE = "ESQL";

    private InferenceSettings inferenceSettings;

    private final Client client;
    private final ThreadPool threadPool;

    /**
     * Creates a new inference service with the given client.
     *
     * @param client the Elasticsearch client for inference operations
     * @param clusterService used to read and update inference settings
     */
    public InferenceService(Client client, ClusterService clusterService) {
        this(client, clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(this::updateInferenceSettings, InferenceSettings.getSettings());

    }

    private InferenceService(Client client, Settings settings) {
        this.client = client;
        this.threadPool = client.threadPool();
        updateInferenceSettings(settings);
    }

    /**
     * Returns the settings for ES|QL inference features.
     *
     * @return the inference settings
     */
    public InferenceSettings inferenceSettings() {
        return inferenceSettings;
    }

    private void updateInferenceSettings(Settings settings) {
        inferenceSettings = new InferenceSettings(settings);
    }

    /**
     * Resolves a list of inference deployment IDs to their metadata.
     * @param inferenceIds List of inference deployment IDs to resolve
     * @param listener     Callback to receive the resolution results
     */
    public void resolveInferenceIds(List<String> inferenceIds, ActionListener<InferenceResolution> listener) {
        resolveInferenceIds(Set.copyOf(inferenceIds), listener);
    }

    void resolveInferenceIds(Set<String> inferenceIds, ActionListener<InferenceResolution> listener) {

        if (inferenceIds.isEmpty()) {
            listener.onResponse(InferenceResolution.EMPTY);
            return;
        }

        final InferenceResolution.Builder inferenceResolutionBuilder = InferenceResolution.builder();

        final CountDownActionListener countdownListener = new CountDownActionListener(
            inferenceIds.size(),
            listener.delegateFailureIgnoreResponseAndWrap(l -> l.onResponse(inferenceResolutionBuilder.build()))
        );

        for (var inferenceId : inferenceIds) {
            client.execute(
                GetInferenceModelAction.INSTANCE,
                new GetInferenceModelAction.Request(inferenceId, TaskType.ANY),
                new ThreadedActionListener<>(threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION), ActionListener.wrap(r -> {
                    ResolvedInference resolvedInference = new ResolvedInference(inferenceId, r.getEndpoints().getFirst().getTaskType());
                    inferenceResolutionBuilder.withResolvedInference(resolvedInference);
                    countdownListener.onResponse(null);
                }, e -> {
                    inferenceResolutionBuilder.withError(inferenceId, e.getMessage());
                    countdownListener.onResponse(null);
                }))
            );
        }
    }

    /**
     * Executes a plain inference request.
     *
     * @param request  the inference request to execute
     * @param listener the listener to notify upon completion
     */
    public void executeInference(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        executeAsyncWithOrigin(client, INFERENCE_ORIGIN, InferenceAction.INSTANCE, request, listener);
    }

    /**
     * Executes an embedding inference request.
     *
     * @param request  the embedding request to execute
     * @param listener the listener to notify upon completion
     */
    public void executeEmbeddingInference(EmbeddingAction.Request request, ActionListener<InferenceAction.Response> listener) {
        executeAsyncWithOrigin(client, INFERENCE_ORIGIN, EmbeddingAction.INSTANCE, request, listener);
    }

    /**
     * Executes a rerank inference request.
     *
     * @param request  the rerank request to execute
     * @param listener the listener to notify upon completion
     */
    public void executeRerankInference(RerankAction.Request request, ActionListener<InferenceAction.Response> listener) {
        executeAsyncWithOrigin(client, INFERENCE_ORIGIN, RerankAction.INSTANCE, request, listener);
    }

    public ThreadPool threadPool() {
        return threadPool;
    }

    public ThreadContext threadContext() {
        return threadPool.getThreadContext();
    }
}
