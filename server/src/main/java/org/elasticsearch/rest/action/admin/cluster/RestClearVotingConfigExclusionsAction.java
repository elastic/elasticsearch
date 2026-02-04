/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.EmptyResponseListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;
import static org.elasticsearch.rest.action.EmptyResponseListener.PLAIN_TEXT_EMPTY_RESPONSE_CAPABILITY_NAME;

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
    public Set<String> supportedCapabilities() {
        return Set.of(PLAIN_TEXT_EMPTY_RESPONSE_CAPABILITY_NAME);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final var req = resolveVotingConfigExclusionsRequest(request);
        return channel -> client.execute(TransportClearVotingConfigExclusionsAction.TYPE, req, new EmptyResponseListener(channel));
    }

    static ClearVotingConfigExclusionsRequest resolveVotingConfigExclusionsRequest(final RestRequest request) {
        final var resolvedRequest = new ClearVotingConfigExclusionsRequest(getMasterNodeTimeout(request));
        resolvedRequest.setTimeout(resolvedRequest.masterNodeTimeout());
        resolvedRequest.setWaitForRemoval(request.paramAsBoolean("wait_for_removal", resolvedRequest.getWaitForRemoval()));
        return resolvedRequest;
    }

}
