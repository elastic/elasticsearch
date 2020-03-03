/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Request;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Response;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class TransportGetTrainedModelsAction extends HandledTransportAction<Request, Response> {

    private final TrainedModelProvider provider;
    @Inject
    public TransportGetTrainedModelsAction(TransportService transportService,
                                           ActionFilters actionFilters,
                                           TrainedModelProvider trainedModelProvider) {
        super(GetTrainedModelsAction.NAME, transportService, actionFilters, GetTrainedModelsAction.Request::new);
        this.provider = trainedModelProvider;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {

        Response.Builder responseBuilder = Response.builder();

        ActionListener<Tuple<Long, Set<String>>> idExpansionListener = ActionListener.wrap(
            totalAndIds -> {
                responseBuilder.setTotalCount(totalAndIds.v1());

                if (totalAndIds.v2().isEmpty()) {
                    listener.onResponse(responseBuilder.build());
                    return;
                }

                if (request.isIncludeModelDefinition() && totalAndIds.v2().size() > 1) {
                    listener.onFailure(
                        ExceptionsHelper.badRequestException(Messages.INFERENCE_TOO_MANY_DEFINITIONS_REQUESTED)
                    );
                    return;
                }

                if (request.isIncludeModelDefinition()) {
                    provider.getTrainedModel(totalAndIds.v2().iterator().next(), true, ActionListener.wrap(
                        config -> listener.onResponse(responseBuilder.setModels(Collections.singletonList(config)).build()),
                        listener::onFailure
                    ));
                } else {
                    provider.getTrainedModels(totalAndIds.v2(), request.isAllowNoResources(), ActionListener.wrap(
                        configs -> listener.onResponse(responseBuilder.setModels(configs).build()),
                        listener::onFailure
                    ));
                }
            },
            listener::onFailure
        );

        provider.expandIds(request.getResourceId(),
            request.isAllowNoResources(),
            request.getPageParams(),
            new HashSet<>(request.getTags()),
            idExpansionListener);
    }

}
