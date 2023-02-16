/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class RestUpdateDesiredNodesAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "update_desired_nodes";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.PUT, "_internal/desired_nodes/{history_id}/{version}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String historyId = request.param("history_id");
        final long version = request.paramAsLong("version", Long.MIN_VALUE);
        boolean dryRun = request.paramAsBoolean("dry_run", false);

        final UpdateDesiredNodesRequest updateDesiredNodesRequest;
        try (XContentParser parser = request.contentParser()) {
            updateDesiredNodesRequest = UpdateDesiredNodesRequest.fromXContent(historyId, version, dryRun, parser);
        }

        updateDesiredNodesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", updateDesiredNodesRequest.masterNodeTimeout()));
        return restChannel -> client.execute(
            UpdateDesiredNodesAction.INSTANCE,
            updateDesiredNodesRequest,
            new RestToXContentListener<>(restChannel)
        );
    }
}
