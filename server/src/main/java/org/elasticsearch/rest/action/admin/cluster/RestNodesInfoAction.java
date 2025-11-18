/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoMetrics;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestNodesInfoAction extends BaseRestHandler {
    static final Set<String> ALLOWED_METRICS = NodesInfoMetrics.Metric.allMetrics();

    private final SettingsFilter settingsFilter;

    public RestNodesInfoAction(SettingsFilter settingsFilter) {
        this.settingsFilter = settingsFilter;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_nodes"),
            // this endpoint is used for metrics, not for node IDs, like /_nodes/fs
            new Route(GET, "/_nodes/{nodeId}"),
            new Route(GET, "/_nodes/{nodeId}/{metrics}"),
            // added this endpoint to be aligned with stats
            new Route(GET, "/_nodes/{nodeId}/info/{metrics}")
        );
    }

    @Override
    public String getName() {
        return "nodes_info_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final NodesInfoRequest nodesInfoRequest = prepareRequest(request);
        settingsFilter.addFilterSettingParams(request);

        return channel -> client.admin().cluster().nodesInfo(nodesInfoRequest, new NodesResponseRestListener<>(channel));
    }

    static NodesInfoRequest prepareRequest(final RestRequest request) {
        String[] nodeIds;
        Set<String> metrics;

        // special case like /_nodes/os (in this case os are metrics and not the nodeId)
        // still, /_nodes/_local (or any other node id) should work and be treated as usual
        // this means one must differentiate between allowed metrics and arbitrary node ids in the same place
        if (request.hasParam("nodeId") && request.hasParam("metrics") == false) {
            String nodeId = request.param("nodeId", "_all");
            Set<String> metricsOrNodeIds = Strings.tokenizeByCommaToSet(nodeId);
            boolean isMetricsOnly = ALLOWED_METRICS.containsAll(metricsOrNodeIds);
            if (isMetricsOnly) {
                nodeIds = new String[] { "_all" };
                metrics = metricsOrNodeIds;
            } else {
                nodeIds = Strings.tokenizeToStringArray(nodeId, ",");
                metrics = Sets.newHashSet("_all");
            }
        } else {
            nodeIds = Strings.tokenizeToStringArray(request.param("nodeId", "_all"), ",");
            metrics = Strings.tokenizeByCommaToSet(request.param("metrics", "_all"));
        }

        final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(nodeIds);
        nodesInfoRequest.setTimeout(getTimeout(request));
        // shortcut, don't do checks if only all is specified
        if (metrics.size() == 1 && metrics.contains("_all")) {
            nodesInfoRequest.all();
        } else {
            nodesInfoRequest.clear();
            // disregard unknown metrics; TODO eschew this lenience?
            metrics.retainAll(ALLOWED_METRICS);
            nodesInfoRequest.addMetrics(metrics.toArray(String[]::new));
        }
        return nodesInfoRequest;
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
