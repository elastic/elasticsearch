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

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Table;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestMasterAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat/master"));
    }

    @Override
    public String getName() {
        return "cat_master_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/master\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestResponseListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(final ClusterStateResponse clusterStateResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, clusterStateResponse), channel);
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders()
                .addCell("id", "desc:node id")
                .addCell("host", "alias:h;desc:host name")
                .addCell("ip", "desc:ip address ")
                .addCell("node", "alias:n;desc:node name")
                .endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, ClusterStateResponse state) {
        Table table = getTableWithHeader(request);
        DiscoveryNodes nodes = state.getState().nodes();

        table.startRow();
        DiscoveryNode master = nodes.get(nodes.getMasterNodeId());
        if (master == null) {
            table.addCell("-");
            table.addCell("-");
            table.addCell("-");
            table.addCell("-");
        } else {
            table.addCell(master.getId());
            table.addCell(master.getHostName());
            table.addCell(master.getHostAddress());
            table.addCell(master.getName());
        }
        table.endRow();

        return table;
    }
}
