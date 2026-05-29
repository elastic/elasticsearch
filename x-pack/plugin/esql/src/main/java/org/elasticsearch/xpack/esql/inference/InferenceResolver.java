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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;

import java.util.List;
import java.util.Set;

/**
 * Resolves inference deployment IDs collected during pre-analysis to their metadata.
 */
public class InferenceResolver {

    private final Client client;
    private final ThreadPool threadPool;

    /**
     * Constructs a new {@code InferenceResolver}.
     *
     * @param client The Elasticsearch client for executing inference deployment lookups
     */
    public InferenceResolver(Client client, ThreadPool threadPool) {
        this.client = client;
        this.threadPool = threadPool;
    }

    /**
     * Resolves a list of inference deployment IDs to their metadata.
     * <p>
     * For each inference ID, this method:
     * <ol>
     *   <li>Queries the inference service to verify the deployment exists</li>
     *   <li>Retrieves the deployment's task type and configuration</li>
     *   <li>Builds an {@link InferenceResolution} containing resolved metadata or errors</li>
     * </ol>
     *
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

    public static Factory factory(Client client) {
        return new Factory(client, client.threadPool());
    }

    public static class Factory {
        private final Client client;
        private final ThreadPool threadPool;

        private Factory(Client client, ThreadPool threadPool) {
            this.client = client;
            this.threadPool = threadPool;
        }

        public InferenceResolver create() {
            return new InferenceResolver(client, threadPool);
        }
    }
}
