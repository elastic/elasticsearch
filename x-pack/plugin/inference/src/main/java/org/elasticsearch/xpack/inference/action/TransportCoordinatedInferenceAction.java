
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.inference.UnparsedModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.Optional;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportCoordinatedInferenceAction extends HandledTransportAction<
    CoordinatedInferenceAction.Request,
    CoordinatedInferenceAction.Response> {

    private final Client client;
    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final ClusterService clusterService;

    @Inject
    public TransportCoordinatedInferenceAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry
    ) {
        super(actionName, transportService, actionFilters, CoordinatedInferenceAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(
        Task task,
        CoordinatedInferenceAction.Request request,
        ActionListener<CoordinatedInferenceAction.Response> listener
    ) {
        if (request.hasInferenceConfig() || request.hasObjects()) {
            tryInClusterModel(request, listener);
        } else {
            tryInferenceServiceModel(request,listener);
        }
    }

    private void tryInferenceServiceModel(
        CoordinatedInferenceAction.Request request,
        ActionListener<CoordinatedInferenceAction.Response> listener
    ) {
        ActionListener<ModelRegistry.ModelConfigMap> modelMapListener = ActionListener.wrap(modelConfigMap -> {
            var unparsedModel = UnparsedModel.unparsedModelFromMap(modelConfigMap.config(), modelConfigMap.secrets());
            executeAsyncWithOrigin(
                client,
                INFERENCE_ORIGIN,
                InferenceAction.INSTANCE,
                new InferenceAction.Request(
                    unparsedModel.taskType(),
                    request.getModelId(),
                    request.getInputs().get(0),
                    request.getTaskSettings()
                ),
                ActionListener.wrap(r -> translateInferenceResponse(r.getResult(), listener),
                    e -> handleInferenceServiceModelFailure(e, request, listener))
            );
        }, listener::onFailure);

        modelRegistry.getModel(request.getModelId(), modelMapListener);
    }

    private void tryInClusterModel(CoordinatedInferenceAction.Request request, ActionListener<CoordinatedInferenceAction.Response> listener) {
        var inferModelRequest = request.hasObjects() ? InferModelAction.Request.forIngestDocs(
            request.getModelId(),
            request.getObjectsToInfer(),
            request.getInferenceConfigUpdate(),
            request.getPreviouslyLicensed()
        ) :
            InferModelAction.Request.forTextInput(request.getModelId(), request.getInferenceConfigUpdate(), request.getInputs());


        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            InferModelAction.INSTANCE,
            inferModelRequest,
            ActionListener.wrap(
                r -> { listener.onResponse(new CoordinatedInferenceAction.Response(r.getInferenceResults().get(0))); },
                e -> handleDfaModelError(e, request, listener)
            )
        );
    }

    private void handleInferenceServiceModelFailure(
        Exception error,
        CoordinatedInferenceAction.Request request,
        ActionListener<CoordinatedInferenceAction.Response> listener
    ) {
        if (error instanceof ResourceNotFoundException) {
            tryInClusterModel(request, listener);
        } else {
            listener.onFailure(error);
        }
    }

    private void handleDfaModelError(
        Exception error,
        CoordinatedInferenceAction.Request request,
        ActionListener<CoordinatedInferenceAction.Response> listener
    ) {
        if (error instanceof ResourceNotFoundException) {
            // InferModelAction does not recognise the model, it may be
            // an inference service model in which case explain that the
            // request is of the wrong type
            lookForInferenceServiceModelWithId(
                request.getModelId(),
                ActionListener.wrap(
                    model -> listener.onFailure(
                        new ElasticsearchStatusException(
                            "[{}] wrong request type an inference service model",  // TODO better error
                            RestStatus.BAD_REQUEST,
                            request.getModelId()
                        )
                    ),
                    e -> listener.onFailure(error) // return the original error
                )
            );
        } else {
            listener.onFailure(error);
        }
    }

    private void lookForInferenceServiceModelWithId(String modelId, ActionListener<Optional<ModelRegistry.ModelConfigMap>> listener) {
        modelRegistry.getModel(modelId, ActionListener.wrap(r -> listener.onResponse(Optional.of(r)), e -> Optional.empty()));
    }



    static void translateInferenceResponse(
        InferenceResults inferenceResults,
        ActionListener<CoordinatedInferenceAction.Response> listener
    ) {
        listener.onResponse(new CoordinatedInferenceAction.Response(inferenceResults));
    }
}
