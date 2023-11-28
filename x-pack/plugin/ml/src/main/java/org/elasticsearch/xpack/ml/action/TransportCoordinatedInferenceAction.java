
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentUtils;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportCoordinatedInferenceAction extends HandledTransportAction<
    CoordinatedInferenceAction.Request,
    InferModelAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportCoordinatedInferenceAction.class);

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportCoordinatedInferenceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService
    ) {
        super(
            CoordinatedInferenceAction.NAME,
            transportService,
            actionFilters,
            CoordinatedInferenceAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        if (request.getModelType() == CoordinatedInferenceAction.Request.ModelType.NLP_MODEL) {
            // must be an inference service model or ml hosted model
            forNlp(request, listener);
        } else if (request.hasObjects()) {
            // Inference service models do not accept a document map
            doInClusterModel(request, listener);  // if this fails check if IS model and error accordingly
        } else {
            forNlp(request, listener);  // if no model found check it isn't an NLP model

            // boosted tree models need objects
        }
    }

    private void forNlp(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        logger.info("[CoordAction] forNlp [{}]", request.getModelId());
        var clusterState = clusterService.state();
        var assignments = TrainedModelAssignmentUtils.modelAssignments(request.getModelId(), clusterState);
        if (assignments == null || assignments.isEmpty()) {
            doInferenceServiceModel(request, listener);
        } else {
            doInClusterModel(request, listener);
        }
    }

    private void doInferenceServiceModel(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        logger.info("[CoordAction] doInferenceServiceModel [{}]", request.getModelId());
        executeAsyncWithOrigin(
            client,
            INFERENCE_ORIGIN,
            InferenceAction.INSTANCE,
            new InferenceAction.Request(TaskType.ANY, request.getModelId(), request.getInputs(), request.getTaskSettings()),
            ActionListener.wrap(r -> translateInferenceServiceResponse(r.getResults(), listener), listener::onFailure)
        );
    }

    private void doInClusterModel(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        logger.info("[CoordAction] doInClusterModel [{}]", request.getModelId());
        var inferModelRequest = translateRequest(request);

        executeAsyncWithOrigin(client, ML_ORIGIN, InferModelAction.INSTANCE, inferModelRequest, listener);
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
                request.getInferenceTimeout()
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

    // private ActionListener<InferModelAction.Response> wrapModelNotFoundInBoostedTreeModelCheck(ActionListener<InferModelAction.Response>
    // listener) {
    //// return ActionListener.wrap(
    //// listener::onResponse,
    //// e -> {
    //// executeAsyncWithOrigin(
    //// client,
    //// ML_ORIGIN,
    //// GetTrainedModelsAction.INSTANCE,
    //// new InferenceAction.Request(TaskType.ANY, request.getModelId(), request.getInputs(), request.getTaskSettings()),
    //// ActionListener.wrap(r -> translateInferenceServiceResponse(r.getResults(), listener), listener::onFailure)
    //// );
    //// }
    //// );
    // }

    /*
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



    private void lookForInferenceServiceModelWithId(String modelId, ActionListener<PutInferenceModelAction.Response> listener) {
        logger.info("[CoordAction] lookForInferenceServiceModelWithId [{}]", modelId);
        executeAsyncWithOrigin(
            client,
            INFERENCE_ORIGIN,
            GetInferenceModelAction.INSTANCE,
            new GetInferenceModelAction.Request(modelId, TaskType.ANY),
            listener
        );
    }
    */
    static void translateInferenceServiceResponse(
        List<? extends InferenceResults> inferenceResults,
        ActionListener<InferModelAction.Response> listener
    ) {
        logger.info("[CoordAction] translateInferenceServiceResponse");
        assert inferenceResults.size() > 0 : "no results";

        var copyListDueToBoundedCaptureError = new ArrayList<InferenceResults>(inferenceResults);
        var cp = new InferModelAction.Response(copyListDueToBoundedCaptureError, null, false);
        listener.onResponse(cp);
    }
}
