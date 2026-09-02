/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST entry point for {@code POST /_encryption/_reset}. Requires the {@code accept_data_loss=true} query parameter to
 * acknowledge the destructive nature of the operation.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestEncryptionResetAction extends BaseRestHandler {

    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestEncryptionResetAction(Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_encryption/_reset"));
    }

    @Override
    public String getName() {
        return "encryption_reset_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (clusterSupportsFeature.test(ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE) == false) {
            throw new IllegalArgumentException("endpoint not supported on all nodes in the cluster");
        }
        boolean acceptDataLoss = request.paramAsBoolean("accept_data_loss", false);
        var req = new EncryptionResetRequest(RestUtils.getMasterNodeTimeout(request), RestUtils.getAckTimeout(request), acceptDataLoss);
        return channel -> client.execute(TransportEncryptionResetAction.TYPE, req, new RestToXContentListener<>(channel));
    }
}
