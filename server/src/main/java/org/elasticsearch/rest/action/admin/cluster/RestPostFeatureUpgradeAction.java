/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeAction;
import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

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

        final PostFeatureUpgradeRequest req = new PostFeatureUpgradeRequest();
        req.masterNodeTimeout(request.paramAsTime("master_timeout", req.masterNodeTimeout()));

        return restChannel -> { client.execute(PostFeatureUpgradeAction.INSTANCE, req, new RestToXContentListener<>(restChannel)); };
    }
}
