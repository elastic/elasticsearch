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
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

public class RestUpdateDesiredNodesAction extends BaseRestHandler {

    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestUpdateDesiredNodesAction.class);
    private static final String VERSION_DEPRECATION_MESSAGE =
        "[version removal] Specifying node_version in desired nodes requests is deprecated.";
    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestUpdateDesiredNodesAction(Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

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

        if (clusterSupportsFeature.test(DesiredNode.DESIRED_NODE_VERSION_DEPRECATED)) {
            if (updateDesiredNodesRequest.getNodes().stream().anyMatch(DesiredNode::hasVersion)) {
                deprecationLogger.compatibleCritical("desired_nodes_version", VERSION_DEPRECATION_MESSAGE);
            }
        } else {
            if (updateDesiredNodesRequest.getNodes().stream().anyMatch(n -> n.hasVersion() == false)) {
                throw new XContentParseException("[node_version] field is required and must have a valid value");
            }
        }

        updateDesiredNodesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", updateDesiredNodesRequest.masterNodeTimeout()));
        return restChannel -> client.execute(
            UpdateDesiredNodesAction.INSTANCE,
            updateDesiredNodesRequest,
            new RestToXContentListener<>(restChannel)
        );
    }
}
