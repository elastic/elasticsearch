
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentUtils;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportCoordinatedInferenceAction extends HandledTransportAction<
    CoordinatedInferenceAction.Request,
    InferModelAction.Response> {

    private static final Map<TrainedModelPrefixStrings.PrefixType, InputType> PREFIX_TYPE_INPUT_TYPE_MAP = Map.of(
        TrainedModelPrefixStrings.PrefixType.INGEST,
        InputType.INTERNAL_INGEST,
        TrainedModelPrefixStrings.PrefixType.SEARCH,
        InputType.INTERNAL_SEARCH
    );

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
        if (request.getRequestModelType() == CoordinatedInferenceAction.Request.RequestModelType.NLP_MODEL) {
            // must be an inference service model or ml hosted model
            forNlp(request, listener);
        } else if (request.hasObjects()) {
            // Inference service models do not accept a document map
            // If this fails check if the model is an inference service
            // model and error accordingly
            doInClusterModel(request, wrapCheckForServiceModelOnMissing(request.getModelId(), listener));
        } else {
            forNlp(request, listener);
        }
    }

    private void forNlp(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        var clusterState = clusterService.state();
        var assignments = TrainedModelAssignmentUtils.modelAssignments(request.getModelId(), clusterState);
        if (assignments == null || assignments.isEmpty()) {
            doInferenceServiceModel(
                request,
                ActionListener.wrap(
                    listener::onResponse,
                    e -> replaceErrorOnMissing(
                        e,
                        () -> new ElasticsearchStatusException(
                            "[" + request.getModelId() + "] is not an inference service model or a deployed ml model",
                            RestStatus.NOT_FOUND
                        ),
                        listener
                    )
                )
            );
        } else {
            doInClusterModel(request, listener);
        }
    }

    private void doInferenceServiceModel(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
        var inputType = convertPrefixToInputType(request.getPrefixType());

        executeAsyncWithOrigin(
            client,
            INFERENCE_ORIGIN,
            InferenceAction.INSTANCE,
            new InferenceAction.Request(
                TaskType.ANY,
                request.getModelId(),
                null,
                null,
                null,
                request.getInputs(),
                request.getTaskSettings(),
                inputType,
                request.getInferenceTimeout(),
                false
            ),
            listener.delegateFailureAndWrap((l, r) -> l.onResponse(translateInferenceServiceResponse(r.getResults())))
        );
    }

    // default for testing
    static InputType convertPrefixToInputType(TrainedModelPrefixStrings.PrefixType prefixType) {
        var inputType = PREFIX_TYPE_INPUT_TYPE_MAP.get(prefixType);

        if (inputType == null) {
            return InputType.INTERNAL_INGEST;
        }

        return inputType;
    }

    private void doInClusterModel(CoordinatedInferenceAction.Request request, ActionListener<InferModelAction.Response> listener) {
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

    private ActionListener<InferModelAction.Response> wrapCheckForServiceModelOnMissing(
        String modelId,
        ActionListener<InferModelAction.Response> listener
    ) {
        return ActionListener.wrap(listener::onResponse, originalError -> {
            if (ExceptionsHelper.unwrapCause(originalError) instanceof ResourceNotFoundException) {
                executeAsyncWithOrigin(
                    client,
                    INFERENCE_ORIGIN,
                    GetInferenceModelAction.INSTANCE,
                    new GetInferenceModelAction.Request(modelId, TaskType.ANY),
                    ActionListener.wrap(
                        model -> listener.onFailure(
                            new ElasticsearchStatusException(
                                "[" + modelId + "] is configured for the _inference API and does not accept documents as input",
                                RestStatus.BAD_REQUEST
                            )
                        ),
                        e -> listener.onFailure(originalError)
                    )
                );
            } else {
                listener.onFailure(originalError);
            }
        });
    }

    private void replaceErrorOnMissing(
        Exception originalError,
        Supplier<ElasticsearchStatusException> replaceOnMissing,
        ActionListener<InferModelAction.Response> listener
    ) {
        if (ExceptionsHelper.unwrapCause(originalError) instanceof ResourceNotFoundException) {
            listener.onFailure(replaceOnMissing.get());
        } else {
            listener.onFailure(originalError);
        }
    }

    static InferModelAction.Response translateInferenceServiceResponse(InferenceServiceResults inferenceResults) {
        var legacyResults = new ArrayList<InferenceResults>(inferenceResults.transformToCoordinationFormat());
        return new InferModelAction.Response(legacyResults, null, false);
    }
}
