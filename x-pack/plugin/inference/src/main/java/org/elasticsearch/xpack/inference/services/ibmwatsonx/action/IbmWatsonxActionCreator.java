/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.action;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxRerankRequestManager;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class IbmWatsonxActionCreator implements IbmWatsonxActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public IbmWatsonxActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(IbmWatsonxEmbeddingsModel model, Map<String, Object> taskSettings) {
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("IBM WatsonX embeddings");
        return new SenderExecutableAction(
            sender,
            getEmbeddingsRequestManager(model, serviceComponents.truncator(), serviceComponents.threadPool()),
            failedToSendRequestErrorMessage
        );
    }

    @Override
    public ExecutableAction create(IbmWatsonxRerankModel model, Map<String, Object> taskSettings) {
        var overriddenModel = IbmWatsonxRerankModel.of(model, taskSettings);
        var requestCreator = IbmWatsonxRerankRequestManager.of(overriddenModel, serviceComponents.threadPool());
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("Ibm Watsonx rerank");
        return new SenderExecutableAction(sender, requestCreator, failedToSendRequestErrorMessage);
    }

    protected IbmWatsonxEmbeddingsRequestManager getEmbeddingsRequestManager(
        IbmWatsonxEmbeddingsModel model,
        Truncator truncator,
        ThreadPool threadPool
    ) {
        return new IbmWatsonxEmbeddingsRequestManager(model, truncator, threadPool);
    }
}
