/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.info;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.rest.RestRequest;

import java.util.List;

public class RestHttpInfoAction extends AbstractInfoAction {

    public static final NodesStatsRequest NODES_STATS_REQUEST = new NodesStatsRequest().clear()
        .addMetric(NodesStatsRequest.Metric.HTTP.metricName());

    @Override
    public String getName() {
        return "http_info_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_info/http"));
    }

    @Override
    public NodesStatsRequest buildNodeStatsRequest() {
        return NODES_STATS_REQUEST;
    }

    @Override
    public ChunkedToXContent xContentChunks(NodesStatsResponse nodesStatsResponse) {
        return nodesStatsResponse.getNodes().stream().map(NodeStats::getHttp).reduce(HttpStats.IDENTITY, HttpStats::merge);
    }
}
