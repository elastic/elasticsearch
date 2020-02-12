/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestClusterStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_cluster/stats"),
            new Route(GET, "/_cluster/stats/nodes/{nodeId}")));
    }

    @Override
    public String getName() {
        return "cluster_stats_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterStatsRequest clusterStatsRequest = new ClusterStatsRequest().nodesIds(request.paramAsStringArray("nodeId", null));
        clusterStatsRequest.timeout(request.param("timeout"));
        return channel -> client.admin().cluster().clusterStats(clusterStatsRequest, new NodesResponseRestListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
