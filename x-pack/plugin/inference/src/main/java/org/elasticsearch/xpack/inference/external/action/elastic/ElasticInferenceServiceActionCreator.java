/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.elastic;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.ElasticInferenceServiceSparseEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsModel;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class ElasticInferenceServiceActionCreator implements ElasticInferenceServiceActionVisitor {

    private final Sender sender;

    private final ServiceComponents serviceComponents;

    public ElasticInferenceServiceActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(ElasticInferenceServiceSparseEmbeddingsModel model) {
        var requestManager = new ElasticInferenceServiceSparseEmbeddingsRequestManager(model, serviceComponents);
        var errorMessage = constructFailedToSendRequestMessage(model.uri(), "Elastic Inference Service sparse embeddings");
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }
}
