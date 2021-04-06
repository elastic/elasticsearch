/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestClusterSearchShardsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_search_shards"),
            new Route(POST, "/_search_shards"),
            new Route(GET, "/{index}/_search_shards"),
            new Route(POST, "/{index}/_search_shards"));
    }

    @Override
    public String getName() {
        return "cluster_search_shards_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterSearchShardsRequest clusterSearchShardsRequest = Requests.clusterSearchShardsRequest(indices);
        clusterSearchShardsRequest.local(request.paramAsBoolean("local", clusterSearchShardsRequest.local()));
        clusterSearchShardsRequest.routing(request.param("routing"));
        clusterSearchShardsRequest.preference(request.param("preference"));
        clusterSearchShardsRequest.indicesOptions(IndicesOptions.fromRequest(request, clusterSearchShardsRequest.indicesOptions()));
        return channel -> client.admin().cluster().searchShards(clusterSearchShardsRequest, new RestToXContentListener<>(channel));
    }
}
