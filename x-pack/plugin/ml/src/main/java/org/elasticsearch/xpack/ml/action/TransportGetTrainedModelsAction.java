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
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Request;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Response;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportGetTrainedModelsAction extends HandledTransportAction<Request, Response> {

    private final TrainedModelProvider provider;
    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportGetTrainedModelsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        TrainedModelProvider trainedModelProvider
    ) {
        super(GetTrainedModelsAction.NAME, transportService, actionFilters, GetTrainedModelsAction.Request::new);
        this.provider = trainedModelProvider;
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());

        Response.Builder responseBuilder = Response.builder();

        ActionListener<List<TrainedModelConfig>> getModelDefinitionStatusListener = ActionListener.wrap(configs -> {
            if (request.getIncludes().isIncludeDefinitionStatus() == false) {
                listener.onResponse(responseBuilder.setModels(configs).build());
                return;
            }

            assert configs.size() <= 1;
            if (configs.isEmpty()) {
                listener.onResponse(responseBuilder.setModels(configs).build());
                return;
            }

            if (configs.get(0).getModelType() != TrainedModelType.PYTORCH) {
                listener.onFailure(ExceptionsHelper.badRequestException("Definition status is only relevant to PyTorch model types"));
                return;
            }

            TransportStartTrainedModelDeploymentAction.checkFullModelDefinitionIsPresent(
                new OriginSettingClient(client, ML_ORIGIN),
                configs.get(0),
                false,  // missing docs are not an error
                ActionListener.wrap(modelIdAndLength -> {
                    configs.get(0).setFullDefinition(modelIdAndLength.v2() > 0);
                    listener.onResponse(responseBuilder.setModels(configs).build());
                }, listener::onFailure)
            );
        }, listener::onFailure);

        ActionListener<Tuple<Long, Map<String, Set<String>>>> idExpansionListener = ActionListener.wrap(totalAndIds -> {
            responseBuilder.setTotalCount(totalAndIds.v1());

            if (totalAndIds.v2().isEmpty()) {
                listener.onResponse(responseBuilder.build());
                return;
            }

            if (request.getIncludes().isIncludeModelDefinition() && totalAndIds.v2().size() > 1) {
                listener.onFailure(ExceptionsHelper.badRequestException(Messages.INFERENCE_TOO_MANY_DEFINITIONS_REQUESTED));
                return;
            }

            if (request.getIncludes().isIncludeDefinitionStatus() && totalAndIds.v2().size() > 1) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Getting the model download status is not supported when getting more than one model"
                    )
                );
                return;
            }

            if (request.getIncludes().isIncludeModelDefinition()) {
                Map.Entry<String, Set<String>> modelIdAndAliases = totalAndIds.v2().entrySet().iterator().next();
                provider.getTrainedModel(
                    modelIdAndAliases.getKey(),
                    modelIdAndAliases.getValue(),
                    request.getIncludes(),
                    parentTaskId,
                    ActionListener.wrap(
                        config -> getModelDefinitionStatusListener.onResponse(Collections.singletonList(config)),
                        getModelDefinitionStatusListener::onFailure
                    )
                );
            } else {
                provider.getTrainedModels(
                    totalAndIds.v2(),
                    request.getIncludes(),
                    request.isAllowNoResources(),
                    parentTaskId,
                    getModelDefinitionStatusListener
                );
            }
        }, listener::onFailure);
        provider.expandIds(
            request.getResourceId(),
            request.isAllowNoResources(),
            request.getPageParams(),
            new HashSet<>(request.getTags()),
            ModelAliasMetadata.fromState(clusterService.state()),
            parentTaskId,
            Collections.emptySet(),
            idExpansionListener
        );
    }
}
