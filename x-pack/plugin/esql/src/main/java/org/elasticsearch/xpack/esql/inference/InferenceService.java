/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class InferenceService {

    private final Client client;

    public InferenceService(Client client) {
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    public ThreadContext getThreadContext() {
        return client.threadPool().getThreadContext();
    }

    public void resolveInferences(List<InferencePlan> plans, ActionListener<InferenceResolution> listener) {

        if (plans.isEmpty()) {
            listener.onResponse(InferenceResolution.EMPTY);
            return;
        }

        Set<String> inferenceIds = plans.stream()
            .map(p -> p.inferenceId().fold(FoldContext.small()).toString())
            .collect(Collectors.toSet());

        CountDownLatch countDownLatch = new CountDownLatch(inferenceIds.size());
        InferenceResolution.Builder inferenceResolutionBuilder = InferenceResolution.builder();

        for (var inferenceId : inferenceIds) {
            client.execute(
                GetInferenceModelAction.INSTANCE,
                new GetInferenceModelAction.Request(inferenceId, TaskType.ANY),
                ActionListener.wrap(r -> {
                    ResolvedInference resolvedInference = new ResolvedInference(inferenceId, r.getEndpoints().getFirst().getTaskType());
                    inferenceResolutionBuilder.withResolvedInference(resolvedInference);
                    countDownLatch.countDown();
                }, e -> {
                    inferenceResolutionBuilder.withError(inferenceId, e.getMessage());
                    countDownLatch.countDown();
                })
            );
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        listener.onResponse(inferenceResolutionBuilder.build());
    }

    public void doInference(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        client.execute(InferenceAction.INSTANCE, request, listener);
    }
}
