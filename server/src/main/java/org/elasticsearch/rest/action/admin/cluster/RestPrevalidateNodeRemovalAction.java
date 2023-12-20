/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestPrevalidateNodeRemovalAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_internal/prevalidate_node_removal"));
    }

    @Override
    public String getName() {
        return "prevalidate_node_removal";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] ids = request.paramAsStringArray("ids", Strings.EMPTY_ARRAY);
        String[] names = request.paramAsStringArray("names", Strings.EMPTY_ARRAY);
        String[] externalIds = request.paramAsStringArray("external_ids", Strings.EMPTY_ARRAY);
        PrevalidateNodeRemovalRequest prevalidationRequest = PrevalidateNodeRemovalRequest.builder()
            .setNames(names)
            .setIds(ids)
            .setExternalIds(externalIds)
            .build();
        prevalidationRequest.masterNodeTimeout(request.paramAsTime("master_timeout", prevalidationRequest.masterNodeTimeout()));
        prevalidationRequest.timeout(request.paramAsTime("timeout", prevalidationRequest.timeout()));
        return channel -> client.execute(
            PrevalidateNodeRemovalAction.INSTANCE,
            prevalidationRequest,
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
