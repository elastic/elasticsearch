/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.action;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.mixedbread.request.MixedbreadRerankRequest;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankModel;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankResponseHandler;
import org.elasticsearch.xpack.inference.services.mixedbread.response.MixedbreadRerankResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class MixedbreadActionCreator implements MixedbreadActionVisitor {
    private static final String FAILED_TO_SEND_REQUEST_ERROR_MESSAGE = "Failed to send Mixedbread %s request from inference entity id [%s]";
    private static final String INVALID_REQUEST_TYPE_MESSAGE = "Invalid request type: expected Mixedbread %s request but got %s";

    private static final ResponseHandler RERANK_HANDLER = new MixedbreadRerankResponseHandler("mixedbread rerank", (request, response) -> {
        if ((request instanceof MixedbreadRerankRequest) == false) {
            var errorMessage = format(
                INVALID_REQUEST_TYPE_MESSAGE,
                "RERANK",
                request != null ? request.getClass().getSimpleName() : "null"
            );
            throw new IllegalArgumentException(errorMessage);
        }
        return MixedbreadRerankResponseEntity.fromResponse(response);
    });

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    /**
     * Constructs a new MixedbreadActionCreator with the specified sender and service components.
     *
     * @param sender the sender to use for executing actions
     * @param serviceComponents the service components providing necessary services
     */
    public MixedbreadActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(MixedbreadRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = MixedbreadRerankModel.of(model, taskSettings);
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            inputs -> new MixedbreadRerankRequest(
                inputs.getQuery(),
                inputs.getChunks(),
                inputs.getReturnDocuments(),
                inputs.getTopN(),
                model
            ),
            QueryAndDocsInputs.class
        );
        var errorMessage = buildErrorMessage(TaskType.RERANK, model.getInferenceEntityId());
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

    /**
     * Builds an error message for failed requests.
     *
     * @param requestType the type of request that failed
     * @param inferenceId the inference entity ID associated with the request
     * @return a formatted error message
     */
    public static String buildErrorMessage(TaskType requestType, String inferenceId) {
        return format(FAILED_TO_SEND_REQUEST_ERROR_MESSAGE, requestType.toString(), inferenceId);
    }
}
