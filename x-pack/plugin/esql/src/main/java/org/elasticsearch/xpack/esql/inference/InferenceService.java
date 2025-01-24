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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class InferenceService {

    private static final Logger logger = LogManager.getLogger(InferenceService.class);

    private final Client client;

    public InferenceService(Client client) {
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    public void resolveInference(InferencePlan inferencePlan, ActionListener<InferenceResolution> listener) {
        String inferenceId = inferencePlan.inferenceId();
        TaskType taskType = inferencePlan.taskType();

        client.execute(
            GetInferenceModelAction.INSTANCE,
            new GetInferenceModelAction.Request(inferenceId, taskType),
            ActionListener.wrap(response -> listener.onResponse(new InferenceResolution(inferenceId, taskType)), listener::onFailure)
        );
    }

    public void infer(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        logger.debug("Executing inference request [{}]", request);
        client.execute(InferenceAction.INSTANCE, request, listener);
    }
}
