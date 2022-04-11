/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.coordination.ClusterFormationInfoRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestClusterFormationInfoAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_internal/cluster_formation_info"));
    }

    @Override
    public String getName() {
        return "cluster_formation_info_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterFormationInfoRequest formationInfoRequest = new ClusterFormationInfoRequest(request.paramAsStringArray("nodeIds", null));
        formationInfoRequest.timeout(request.param("timeout"));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .cluster()
            .clusterFormationInfo(formationInfoRequest, new NodesResponseRestListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
