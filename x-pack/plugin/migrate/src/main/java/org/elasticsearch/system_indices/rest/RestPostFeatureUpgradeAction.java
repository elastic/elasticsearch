/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.system_indices.action.PostFeatureUpgradeAction;
import org.elasticsearch.system_indices.action.PostFeatureUpgradeRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/**
 * Endpoint for triggering a system feature upgrade
 */
public class RestPostFeatureUpgradeAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "post_feature_upgrade";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_migration/system_features"));
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final PostFeatureUpgradeRequest req = new PostFeatureUpgradeRequest(getMasterNodeTimeout(request));
        return restChannel -> client.execute(PostFeatureUpgradeAction.INSTANCE, req, new RestToXContentListener<>(restChannel));
    }
}
