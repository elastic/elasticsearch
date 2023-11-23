
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.inference.UnparsedModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportCoordinatedInferenceAction extends HandledTransportAction<
    CoordinatedInferenceAction.Request,
    InferModelAction.Response> {

    private final Client client;
    private final ModelRegistry modelRegistry;
    private final ClusterService clusterService;

    @Inject
    public TransportCoordinatedInferenceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        ModelRegistry modelRegistry
    ) {
        super(
            CoordinatedInferenceAction.NAME,
            transportService,
            actionFilters,
            CoordinatedInferenceAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.modelRegistry = modelRegistry;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        if (request.hasInferenceConfig() || request.hasObjects()) {
            tryInClusterModel(request, listener);
        } else {
            tryInferenceServiceModel(request, listener);
        }
    }

    private void tryInferenceServiceModel(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        ActionListener<ModelRegistry.ModelConfigMap> modelMapListener = ActionListener.wrap(modelConfigMap -> {
            var unparsedModel = UnparsedModel.unparsedModelFromMap(modelConfigMap.config(), modelConfigMap.secrets());
            executeAsyncWithOrigin(
                client,
                INFERENCE_ORIGIN,
                InferenceAction.INSTANCE,
                new InferenceAction.Request(unparsedModel.taskType(), request.getModelId(), request.getInputs(), request.getTaskSettings()),
                ActionListener.wrap(
                    r -> translateInferenceServiceResponse(r.getResults(), listener),
                    e -> handleInferenceServiceModelFailure(e, request, listener)
                )
            );
        }, listener::onFailure);

        modelRegistry.getUnparsedModelMap(request.getModelId(), modelMapListener);
    }

    private void tryInClusterModel(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        var inferModelRequest = request.hasObjects()
            ? InferModelAction.Request.forIngestDocs(
                request.getModelId(),
                request.getObjectsToInfer(),
                request.getInferenceConfigUpdate(),
                request.getPreviouslyLicensed(),
                request.getInferenceTimeout() // TODO is this timeout right
            )
            : InferModelAction.Request.forTextInput(
                request.getModelId(),
                request.getInferenceConfigUpdate(),
                request.getInputs(),
                true,
                request.getInferenceTimeout()
            );

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            InferModelAction.INSTANCE,
            inferModelRequest,
            ActionListener.wrap(listener::onResponse, e -> handleDfaModelError(e, request, listener))
        );
    }

    private void handleInferenceServiceModelFailure(
        Exception error,
        CoordinatedInferenceAction.Request request,
        ActionListener<InferModelAction.Response> listener
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
        ActionListener<InferModelAction.Response> listener
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
        modelRegistry.getUnparsedModelMap(modelId, ActionListener.wrap(r -> listener.onResponse(Optional.of(r)), listener::onFailure));
    }

    static void translateInferenceServiceResponse(
        List<? extends InferenceResults> inferenceResults,
        ActionListener<InferModelAction.Response> listener
    ) {
        var copyListDueToBoundedCaptureError = new ArrayList<InferenceResults>(inferenceResults);
        listener.onResponse(new InferModelAction.Response(copyListDueToBoundedCaptureError, null, false));
    }
}
