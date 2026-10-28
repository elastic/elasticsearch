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
import org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusAction;
import org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/**
 * Endpoint for getting the system feature upgrade status
 */
public class RestGetFeatureUpgradeStatusAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_feature_upgrade_status";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_migration/system_features"));
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final GetFeatureUpgradeStatusRequest req = new GetFeatureUpgradeStatusRequest(getMasterNodeTimeout(request));
        return restChannel -> client.execute(GetFeatureUpgradeStatusAction.INSTANCE, req, new RestToXContentListener<>(restChannel));
    }
}
