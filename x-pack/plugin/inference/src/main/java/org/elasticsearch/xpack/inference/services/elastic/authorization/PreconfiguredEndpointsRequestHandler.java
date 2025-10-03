/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;

import java.util.Objects;

/**
 * This class is responsible for converting the current EIS authorization response structure
 * into Models that
 */
public class PreconfiguredEndpointsRequestHandler {
    private final ElasticInferenceServiceAuthorizationRequestHandler eisAuthorizationRequestHandler;
    private final Sender sender;

    public PreconfiguredEndpointsRequestHandler(
        ElasticInferenceServiceAuthorizationRequestHandler eisAuthorizationRequestHandler,
        Sender sender
    ) {
        this.eisAuthorizationRequestHandler = Objects.requireNonNull(eisAuthorizationRequestHandler);
        this.sender = Objects.requireNonNull(sender);
    }

    public void getPreconfiguredEndpointAsUnparsedModel(String inferenceId, ActionListener<UnparsedModel> listener) {
        SubscribableListener.<ElasticInferenceServiceAuthorizationModel>newForked(authListener -> {
            eisAuthorizationRequestHandler.getAuthorization(authListener, sender);
        })
            .andThenApply(PreconfiguredEndpointsModel::of)
            .andThenApply(preconfiguredEndpointsModel -> preconfiguredEndpointsModel.toUnparsedModel(inferenceId))
            .addListener(listener);
    }
}
