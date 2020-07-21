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

import org.elasticsearch.action.admin.cluster.reelect.ClusterReelectAction;
import org.elasticsearch.action.admin.cluster.reelect.ClusterReelectRequest;
import org.elasticsearch.action.admin.cluster.reelect.ClusterReelectResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;


public class RestClusterReelectAction extends BaseRestHandler {

    @Override
    public List<Route> routes() { return List.of(new Route(POST, "/_cluster/reelect")); }

    @Override
    public String getName() { return "cluster_reelect_action"; }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ClusterReelectRequest clusterReelectRequest = new ClusterReelectRequest();
        clusterReelectRequest.timeout(request.paramAsTime("timeout", clusterReelectRequest.timeout()));
        clusterReelectRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterReelectRequest.masterNodeTimeout()));
        return channel -> client.execute(ClusterReelectAction.INSTANCE, clusterReelectRequest,
            new RestToXContentListener<ClusterReelectResponse>(channel));
    }
}
