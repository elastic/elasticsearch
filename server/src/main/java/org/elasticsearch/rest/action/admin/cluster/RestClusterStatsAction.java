/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.REST_TIMEOUT_PARAM;
import static org.elasticsearch.rest.RestUtils.getTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestClusterStatsAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_CAPABILITIES = Set.of(
        "human-readable-total-docs-size",
        "verbose-dense-vector-mapping-stats",
        "ccs-stats",
        "retrievers-usage-stats",
        "esql-stats"
    );
    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of("include_remotes", "nodeId", REST_TIMEOUT_PARAM);

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cluster/stats"), new Route(GET, "/_cluster/stats/nodes/{nodeId}"));
    }

    @Override
    public String getName() {
        return "cluster_stats_action";
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_QUERY_PARAMETERS;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterStatsRequest clusterStatsRequest = new ClusterStatsRequest(
            request.paramAsBoolean("include_remotes", false),
            request.paramAsStringArray("nodeId", null)
        );
        clusterStatsRequest.setTimeout(getTimeout(request));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .cluster()
            .clusterStats(clusterStatsRequest, new NodesResponseRestListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return SUPPORTED_CAPABILITIES;
    }
}
