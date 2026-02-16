/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.mixedbread.request.rerank.MixedbreadRerankRequest;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankModel;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadResponseHandler;
import org.elasticsearch.xpack.inference.services.mixedbread.response.MixedbreadRerankResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class MixedbreadActionCreator implements MixedbreadActionVisitor {
    private static final String RERANK_ERROR_PREFIX = "Mixedbread rerank";

    private static final ResponseHandler RERANK_HANDLER = new MixedbreadResponseHandler(
        "mixedbread rerank",
        (request, response) -> MixedbreadRerankResponseEntity.fromResponse(response)
    );

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
        var errorMessage = constructFailedToSendRequestMessage(RERANK_ERROR_PREFIX);
        return new SenderExecutableAction(sender, manager, errorMessage);
    }
}
