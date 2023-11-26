
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentStateUtils;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
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

    private static final Logger logger = LogManager.getLogger(TransportCoordinatedInferenceAction.class);

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
        if (request.getModelType() == CoordinatedInferenceAction.Request.ModelType.NLP_MODEL) {
            // must be an inference service model or ml hosted model
            forNlp(request, listener);
        } else {
            if (request.hasInferenceConfig() || request.hasObjects()) {
                tryInClusterModel(request, listener);
            } else {
                tryInferenceServiceModel(request, listener);
            }
        }
    }



    private boolean hasTrainedModelAssignment(String modelId, ClusterState state) {
        String concreteModelId = Optional.ofNullable(ModelAliasMetadata.fromState(clusterService.state()).getModelId(request.getId()))
            .orElse(request.getId());

        responseBuilder.setId(concreteModelId);

        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = TrainedModelAssignmentMetadata.fromState(clusterService.state());
        TrainedModelAssignment assignment = trainedModelAssignmentMetadata.getDeploymentAssignment(concreteModelId);
        List<TrainedModelAssignment> assignments;
        if (assignment == null) {
            // look up by model
            assignments = trainedModelAssignmentMetadata.getDeploymentsUsingModel(concreteModelId);
        }
    }


    private void forNlp(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        logger.info("[CoordAction] forNlp [{}]", request.getModelId());
        var clusterState = clusterService.state();
        var assignments = TrainedModelAssignmentStateUtils.modelAssignments(request.getModelId(), clusterState);
        if (assignments == null || assignments.isEmpty()) {
            doInferenceServiceModel(request, listener);
        } else {
            doInClusterModel(request, listener);
        }
    }


    private void doInferenceServiceModel(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        logger.info("[CoordAction] doInferenceServiceModel [{}]", request.getModelId());
        ActionListener<ModelRegistry.ModelConfigMap> modelMapListener = ActionListener.wrap(modelConfigMap -> {
            var unparsedModel = UnparsedModel.unparsedModelFromMap(modelConfigMap.config(), modelConfigMap.secrets());
            executeAsyncWithOrigin(
                client,
                INFERENCE_ORIGIN,
                InferenceAction.INSTANCE,
                new InferenceAction.Request(unparsedModel.taskType(), request.getModelId(), request.getInputs(), request.getTaskSettings()),
                ActionListener.wrap(r -> translateInferenceServiceResponse(r.getResults(), listener), listener::onFailure)
            );
        }, listener::onFailure);

        modelRegistry.getUnparsedModelMap(request.getModelId(), modelMapListener);
    }

    private void tryInferenceServiceModel(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        logger.info("[CoordAction] tryInferenceServiceModel [{}]", request.getModelId());
        ActionListener<ModelRegistry.ModelConfigMap> modelMapListener = ActionListener.wrap(modelConfigMap -> {
            logger.info("[CoordAction] tryInferenceServiceModel got model [{}]", "request.getModelId()");
            var unparsedModel = UnparsedModel.unparsedModelFromMap(modelConfigMap.config(), modelConfigMap.secrets());
            executeAsyncWithOrigin(
                client,
                INFERENCE_ORIGIN,
                InferenceAction.INSTANCE,
                new InferenceAction.Request(unparsedModel.taskType(), request.getModelId(), request.getInputs(), request.getTaskSettings()),
                ActionListener.wrap(
                    r -> translateInferenceServiceResponse(r.getResults(), listener),
                    listener::onFailure
                )
            );

        modelRegistry.getUnparsedModelMap(request.getModelId(), modelMapListener);
    }

    private void doInClusterModel(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        logger.info("[CoordAction] doInClusterModel [{}]", request.getModelId());
        var inferModelRequest = translateRequest(request);

        executeAsyncWithOrigin(client, ML_ORIGIN, InferModelAction.INSTANCE, inferModelRequest, listener);
    }

    private void tryInClusterModel(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        logger.info("[CoordAction] tryInClusterModel [{}]", request.getModelId());
        var inferModelRequest = translateRequest(request);


        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            InferModelAction.INSTANCE,
            inferModelRequest,
            ActionListener.wrap(listener::onResponse, e -> handleDfaModelError(e, request, listener))
        );
    }

    static InferModelAction.Request translateRequest(CoordinatedInferenceAction.Request request) {
        InferenceConfigUpdate inferenceConfigUpdate = request.getInferenceConfigUpdate() == null
            ? EmptyConfigUpdate.INSTANCE
            : request.getInferenceConfigUpdate();

        var inferModelRequest = request.hasObjects()
            ? InferModelAction.Request.forIngestDocs(
                request.getModelId(),
                request.getObjectsToInfer(),
                inferenceConfigUpdate,
                request.getPreviouslyLicensed(),
                request.getInferenceTimeout() // TODO is this timeout right
            )
            : InferModelAction.Request.forTextInput(
                request.getModelId(),
                inferenceConfigUpdate,
                request.getInputs(),
                request.getPreviouslyLicensed(),
                request.getInferenceTimeout()
            );
        inferModelRequest.setPrefixType(request.getPrefixType());
        inferModelRequest.setHighPriority(request.getHighPriority());
        return inferModelRequest;
    }

    private void handleInferenceServiceModelFailure(
        Exception error,
        CoordinatedInferenceAction.Request request,
        ActionListener<InferModelAction.Response> listener
    ) {
        logger.info("[CoordAction] handleInferenceServiceModelFailure [{}]", request.getModelId());
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
        logger.info("[CoordAction] handleDfaModelError [{}]", request.getModelId());
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
        logger.info("[CoordAction] lookForInferenceServiceModelWithId [{}]", modelId);
        modelRegistry.getUnparsedModelMap(modelId, ActionListener.wrap(r -> listener.onResponse(Optional.of(r)), listener::onFailure));
    }

    static void translateInferenceServiceResponse(
        List<? extends InferenceResults> inferenceResults,
        ActionListener<InferModelAction.Response> listener
    ) {
        logger.info("[CoordAction] translateInferenceServiceResponse");
        assert inferenceResults.size() > 0 : "no results";

        try {
            var copyListDueToBoundedCaptureError = new ArrayList<InferenceResults>(inferenceResults);
            var cp = new InferModelAction.Response(copyListDueToBoundedCaptureError, null, false);
            listener.onResponse(cp);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }
}
