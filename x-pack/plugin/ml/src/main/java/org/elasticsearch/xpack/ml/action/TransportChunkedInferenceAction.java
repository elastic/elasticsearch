/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.ChunkedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;

import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportChunkedInferenceAction extends HandledTransportAction<
    ChunkedInferenceAction.Request,
    ChunkedInferenceAction.Response> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportChunkedInferenceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService
    ) {
        super(
            ChunkedInferenceAction.NAME,
            transportService,
            actionFilters,
            ChunkedInferenceAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, ChunkedInferenceAction.Request request, ActionListener<ChunkedInferenceAction.Response> listener) {
        var inferModelRequest = translateRequest(request);
        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            InferModelAction.INSTANCE,
            inferModelRequest,
            listener.delegateFailureAndWrap(TransportChunkedInferenceAction::handleResponse)
        );
    }

    static InferModelAction.Request translateRequest(ChunkedInferenceAction.Request request) {
        var localModelRequest = InferModelAction.Request.forTextInput(
            request.getModelId(),
            EmptyConfigUpdate.INSTANCE,
            request.getInputs(),
            true,
            TimeValue.timeValueSeconds(10)
        );
        // inferModelRequest.setPrefixType(request.getPrefixType());
        // inferModelRequest.setHighPriority(request.getHighPriority());
        return localModelRequest;
    }

    static void handleResponse(ActionListener<ChunkedInferenceAction.Response> listener, InferModelAction.Response response) {
        if (response.getInferenceResults().get(0) instanceof ChunkedNlpInferenceResults chunkedResult) {
            // TODO all results
            listener.onResponse(new ChunkedInferenceAction.Response(List.of(chunkedResult)));
        } else {
            listener.onFailure(new ElasticsearchStatusException("Should have been a chunked response", RestStatus.INTERNAL_SERVER_ERROR));
        }
    }
}
