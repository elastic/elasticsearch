/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class InferenceRunner {

    private final Client client;
    private final ThreadPool threadPool;

    public InferenceRunner(Client client, ThreadPool threadPool) {
        this.client = client;
        this.threadPool = threadPool;
    }

    public ThreadPool threadPool() {
        return threadPool;
    }

    public void resolveInferenceIds(List<InferencePlan<?>> plans, ActionListener<InferenceResolution> listener) {
        resolveInferenceIds(plans.stream().map(InferenceRunner::planInferenceId).collect(Collectors.toSet()), listener);

    }

    private void resolveInferenceIds(Set<String> inferenceIds, ActionListener<InferenceResolution> listener) {

        if (inferenceIds.isEmpty()) {
            listener.onResponse(InferenceResolution.EMPTY);
            return;
        }

        final InferenceResolution.Builder inferenceResolutionBuilder = InferenceResolution.builder();

        final CountDownActionListener countdownListener = new CountDownActionListener(
            inferenceIds.size(),
            ActionListener.wrap(_r -> listener.onResponse(inferenceResolutionBuilder.build()), listener::onFailure)
        );

        for (var inferenceId : inferenceIds) {
            client.execute(
                GetInferenceModelAction.INSTANCE,
                new GetInferenceModelAction.Request(inferenceId, TaskType.ANY),
                ActionListener.wrap(r -> {
                    ResolvedInference resolvedInference = new ResolvedInference(inferenceId, r.getEndpoints().getFirst().getTaskType());
                    inferenceResolutionBuilder.withResolvedInference(resolvedInference);
                    countdownListener.onResponse(null);
                }, e -> {
                    inferenceResolutionBuilder.withError(inferenceId, e.getMessage());
                    countdownListener.onResponse(null);
                })
            );
        }
    }

    private static String planInferenceId(InferencePlan<?> plan) {
        return plan.inferenceId().fold(FoldContext.small()).toString();
    }

    public void doInference(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        client.execute(InferenceAction.INSTANCE, request, listener);
    }
}
