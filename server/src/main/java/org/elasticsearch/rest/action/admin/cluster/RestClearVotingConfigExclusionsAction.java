/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestClearVotingConfigExclusionsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_cluster/voting_config_exclusions"));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    @Override
    public String getName() {
        return "clear_voting_config_exclusions_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final var req = resolveVotingConfigExclusionsRequest(request);
        return channel -> client.execute(TransportClearVotingConfigExclusionsAction.TYPE, req, new RestToXContentListener<>(channel));
    }

    static ClearVotingConfigExclusionsRequest resolveVotingConfigExclusionsRequest(final RestRequest request) {
        final var resolvedRequest = new ClearVotingConfigExclusionsRequest();
        resolvedRequest.masterNodeTimeout(request.paramAsTime("master_timeout", resolvedRequest.masterNodeTimeout()));
        resolvedRequest.setTimeout(resolvedRequest.masterNodeTimeout());
        resolvedRequest.setWaitForRemoval(request.paramAsBoolean("wait_for_removal", resolvedRequest.getWaitForRemoval()));
        return resolvedRequest;
    }

}
