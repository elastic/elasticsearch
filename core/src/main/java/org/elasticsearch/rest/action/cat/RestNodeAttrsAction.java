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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestActionListener;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestNodeAttrsAction extends AbstractCatAction {

    @Inject
    public RestNodeAttrsAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_cat/nodeattrs", this);
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/nodeattrs\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
                nodesInfoRequest.clear().jvm(false).os(false).process(true);
                client.admin().cluster().nodesInfo(nodesInfoRequest, new RestActionListener<NodesInfoResponse>(channel) {
                    @Override
                    public void processResponse(final NodesInfoResponse nodesInfoResponse) {
                        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
                        nodesStatsRequest.clear().jvm(false).os(false).fs(false).indices(false).process(false);
                        client.admin().cluster().nodesStats(nodesStatsRequest, new RestResponseListener<NodesStatsResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(NodesStatsResponse nodesStatsResponse) throws Exception {
                                return RestTable.buildResponse(buildTable(request, clusterStateResponse, nodesInfoResponse, nodesStatsResponse), channel);
                            }
                        });
                    }
                });
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("node", "default:true;alias:name;desc:node name");
        table.addCell("id",   "default:false;alias:id,nodeId;desc:unique node id");
        table.addCell("pid",  "default:false;alias:p;desc:process id");
        table.addCell("host", "alias:h;desc:host name");
        table.addCell("ip",   "alias:i;desc:ip address");
        table.addCell("port", "default:false;alias:po;desc:bound transport port");
        table.addCell("attr", "default:true;alias:attr.name;desc:attribute description");
        table.addCell("value","default:true;alias:attr.value;desc:attribute value");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo, NodesStatsResponse nodesStats) {
        boolean fullId = req.paramAsBoolean("full_id", false);

        DiscoveryNodes nodes = state.getState().nodes();
        Table table = getTableWithHeader(req);

        for (DiscoveryNode node : nodes) {
            NodeInfo info = nodesInfo.getNodesMap().get(node.id());
            for(ObjectObjectCursor<String, String> att : node.attributes()) {
                buildRow(fullId, table, node, info, att.key, att.value);
            }
            if (info.getServiceAttributes() != null) {
                for (Map.Entry<String, String> entry : info.getServiceAttributes().entrySet()) {
                    buildRow(fullId, table, node, info, entry.getKey(), entry.getValue());
                }
            }
        }

        return table;
    }

    private final void buildRow(boolean fullId, Table table, DiscoveryNode node, NodeInfo info, String key, String value) {
        table.startRow();
        table.addCell(node.name());
        table.addCell(fullId ? node.id() : Strings.substring(node.getId(), 0, 4));
        table.addCell(info == null ? null : info.getProcess().getId());
        table.addCell(node.getHostName());
        table.addCell(node.getHostAddress());
        if (node.address() instanceof InetSocketTransportAddress) {
            table.addCell(((InetSocketTransportAddress) node.address()).address().getPort());
        } else {
            table.addCell("-");
        }
        table.addCell(key);
        table.addCell(value);
        table.endRow();
    }
}
