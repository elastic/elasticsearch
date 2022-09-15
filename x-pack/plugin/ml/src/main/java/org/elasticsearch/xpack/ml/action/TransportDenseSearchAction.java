/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.DenseSearchAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;

import java.util.List;
import java.util.Map;

public class TransportDenseSearchAction extends HandledTransportAction<DenseSearchAction.Request, DenseSearchAction.Response> {

    private final Client client;

    @Inject
    public TransportDenseSearchAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(DenseSearchAction.NAME, transportService, actionFilters, DenseSearchAction.Request::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, DenseSearchAction.Request request, ActionListener<DenseSearchAction.Response> listener) {
        client.execute(InferTrainedModelDeploymentAction.INSTANCE, toInferenceRequest(request), ActionListener.wrap(
            textEmbedding -> {

            },
            listener::onFailure
        ));
    }

    private InferTrainedModelDeploymentAction.Request toInferenceRequest(DenseSearchAction.Request request) {
        final String MAPPED_FIELD = "query_field";
        TextEmbeddingConfigUpdate update = null;
        if (request.getEmbeddingConfig() == null) {
            update = new TextEmbeddingConfigUpdate()
        }


        return new InferTrainedModelDeploymentAction.Request(
            request.getDeploymentId(),
            update,
            List.of(Map.of(MAPPED_FIELD, request.getQueryString())),
            request.getInferenceTimeout()
        );
    }
}
