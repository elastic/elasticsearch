/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.inference.InferenceProvider;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;

/**
 * InferenceProvider implementation that uses the inference action to retrieve inference results.
 */
public class InferenceActionInferenceProvider implements InferenceProvider {

    private final Client client;

    public InferenceActionInferenceProvider(Client client) {
        this.client = new OriginSettingClient(client, INFERENCE_ORIGIN);
    }

    @Override
    public void textInference(String modelId, List<String> texts, ActionListener<List<InferenceResults>> listener) {
        InferenceAction.Request inferenceRequest = new InferenceAction.Request(
            TaskType.SPARSE_EMBEDDING, // TODO Change when task type doesn't need to be specified
            modelId,
            texts,
            Map.of(),
            InputType.INGEST
        );

        client.execute(InferenceAction.INSTANCE, inferenceRequest, listener.delegateFailure((l, response) -> {
            InferenceServiceResults results = response.getResults();
            if (results == null) {
                throw new IllegalArgumentException("No inference retrieved for model ID " + modelId);
            }

            @SuppressWarnings("unchecked")
            List<InferenceResults> result = (List<InferenceResults>) results.transformToLegacyFormat();
            l.onResponse(result);
        }));
    }

    @Override
    public boolean performsInference() {
        return true;
    }
}
