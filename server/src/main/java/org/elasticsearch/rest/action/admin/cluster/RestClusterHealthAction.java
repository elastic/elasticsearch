/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.ClusterStatsLevel;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestClusterHealthAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cluster/health"), new Route(GET, "/_cluster/health/{index}"));
    }

    @Override
    public String getName() {
        return "cluster_health_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ClusterHealthRequest clusterHealthRequest = fromRequest(request);
        return channel -> client.admin().cluster().health(clusterHealthRequest, new RestStatusToXContentListener<>(channel));
    }

    public static ClusterHealthRequest fromRequest(final RestRequest request) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest(indices);
        clusterHealthRequest.indicesOptions(IndicesOptions.fromRequest(request, clusterHealthRequest.indicesOptions()));
        clusterHealthRequest.local(request.paramAsBoolean("local", clusterHealthRequest.local()));
        clusterHealthRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterHealthRequest.masterNodeTimeout()));
        clusterHealthRequest.timeout(request.paramAsTime("timeout", clusterHealthRequest.timeout()));
        String waitForStatus = request.param("wait_for_status");
        if (waitForStatus != null) {
            clusterHealthRequest.waitForStatus(ClusterHealthStatus.valueOf(waitForStatus.toUpperCase(Locale.ROOT)));
        }
        clusterHealthRequest.waitForNoRelocatingShards(
            request.paramAsBoolean("wait_for_no_relocating_shards", clusterHealthRequest.waitForNoRelocatingShards())
        );
        clusterHealthRequest.waitForNoInitializingShards(
            request.paramAsBoolean("wait_for_no_initializing_shards", clusterHealthRequest.waitForNoInitializingShards())
        );
        if (request.hasParam("wait_for_relocating_shards")) {
            // wait_for_relocating_shards has been removed in favor of wait_for_no_relocating_shards
            throw new IllegalArgumentException(
                "wait_for_relocating_shards has been removed, use wait_for_no_relocating_shards [true/false] instead"
            );
        }
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            clusterHealthRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        clusterHealthRequest.waitForNodes(request.param("wait_for_nodes", clusterHealthRequest.waitForNodes()));
        if (request.param("wait_for_events") != null) {
            clusterHealthRequest.waitForEvents(Priority.valueOf(request.param("wait_for_events").toUpperCase(Locale.ROOT)));
        }
        // level parameter validation
        ClusterStatsLevel.of(request, ClusterStatsLevel.CLUSTER);
        return clusterHealthRequest;
    }

    private static final Set<String> RESPONSE_PARAMS = Collections.singleton("level");

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}
