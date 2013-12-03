/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestNodesAction extends AbstractCatAction {

    @Inject
    public RestNodesAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/nodes", this);
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat/nodes\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.filterMetaData(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(final ClusterStateResponse clusterStateResponse) {
                NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
                nodesInfoRequest.clear().jvm(true).os(true).process(true);
                client.admin().cluster().nodesInfo(nodesInfoRequest, new ActionListener<NodesInfoResponse>() {
                    @Override
                    public void onResponse(final NodesInfoResponse nodesInfoResponse) {
                        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
                        nodesStatsRequest.clear().jvm(true).os(true).fs(true);
                        client.admin().cluster().nodesStats(nodesStatsRequest, new ActionListener<NodesStatsResponse>() {
                            @Override
                            public void onResponse(NodesStatsResponse nodesStatsResponse) {
                                try {
                                    channel.sendResponse(RestTable.buildResponse(buildTable(request, clusterStateResponse, nodesInfoResponse, nodesStatsResponse), request, channel));
                                } catch (Throwable e) {
                                    onFailure(e);
                                }
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                try {
                                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                                } catch (IOException e1) {
                                    logger.error("Failed to send failure response", e1);
                                }
                            }
                        });
                    }
                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            channel.sendResponse(new XContentThrowableRestResponse(request, e));
                        } catch (IOException e1) {
                            logger.error("Failed to send failure response", e1);
                        }
                    }
                });
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    @Override
    Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("nodeId");
        table.addCell("pid");
        table.addCell("ip");
        table.addCell("port");

        table.addCell("es");
        table.addCell("jdk");
        table.addCell("diskAvail", "text-align:right;");
        table.addCell("heapPercent", "text-align:right;");
        table.addCell("heapMax", "text-align:right;");
        table.addCell("ramPercent", "text-align:right;");
        table.addCell("ramMax", "text-align:right;");

        table.addCell("load", "text-align:right;");
        table.addCell("uptime", "text-align:right;");
        table.addCell("data/client");
        table.addCell("master");
        table.addCell("name");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo, NodesStatsResponse nodesStats) {
        boolean fullId = req.paramAsBoolean("full_id", false);

        String masterId = state.getState().nodes().masterNodeId();
        Table table = getTableWithHeader(req);

        for (DiscoveryNode node : state.getState().nodes()) {
            NodeInfo info = nodesInfo.getNodesMap().get(node.id());
            NodeStats stats = nodesStats.getNodesMap().get(node.id());
            long availableDisk = -1;
            long heapUsed = -1;
            long heapMax = -1;
            float heapRatio = -1.0f;

            if (null != stats && null != info) {
                heapUsed = stats.getJvm().mem().heapUsed().bytes();
                heapMax = info.getJvm().mem().heapMax().bytes();

                if (heapMax > 0) {
                    heapRatio = heapUsed / (heapMax * 1.0f);
                }

                if (!(stats.getFs() == null)) {
                    availableDisk = 0;
                    Iterator<FsStats.Info> it = stats.getFs().iterator();
                    while (it.hasNext()) {
                        availableDisk += it.next().getAvailable().bytes();
                    }
                }
            }

            table.startRow();

            table.addCell(fullId ? node.id() : node.id().substring(0, 4));
            table.addCell(info == null ? null : info.getProcess().id());
            table.addCell(((InetSocketTransportAddress) node.address()).address().getAddress().getHostAddress());
            table.addCell(((InetSocketTransportAddress) node.address()).address().getPort());
            table.addCell(info == null ? null : info.getVersion().number());
            table.addCell(info == null ? null : info.getJvm().version());
            table.addCell(availableDisk < 0 ? null : ByteSizeValue.parseBytesSizeValue(new Long(availableDisk).toString()));
            table.addCell(heapRatio < 0 ? null : String.format(Locale.ROOT, "%.1f", heapRatio*100.0));
            table.addCell(heapMax < 0 ? null : new ByteSizeValue(heapMax));
            table.addCell(stats == null ? null : stats.getOs().mem() == null ? null : stats.getOs().mem().usedPercent());
            table.addCell(info == null ? null : info.getOs().mem() == null ? null : info.getOs().mem().total()); // sigar fails to load in IntelliJ
            table.addCell(stats == null ? null : stats.getOs() == null ? null : stats.getOs().getLoadAverage().length < 1 ? null : stats.getOs().getLoadAverage()[0]);
            table.addCell(stats == null ? null : stats.getJvm().uptime());
            table.addCell(node.clientNode() ? "c" : node.dataNode() ? "d" : null);
            table.addCell(masterId.equals(node.id()) ? "*" : node.masterNode() ? "m" : "-");
            table.addCell(node.name());

            table.endRow();
        }

        return table;
    }
}
