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
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.table.Row;
import org.elasticsearch.common.table.Table;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestMasterAction extends BaseRestHandler {
    @Inject
    public RestMasterAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/master", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final boolean verbose = request.paramAsBoolean("verbose", false);
        final StringBuilder out = new StringBuilder();

        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();                                                                                                     clusterStateRequest.listenerThreaded(false);
        clusterStateRequest.filterMetaData(true);
        clusterStateRequest.local(false);

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(final ClusterStateResponse clusterStateResponse) {
                try {
                    RestStatus status = RestStatus.OK;
                    Table tab = new Table();
                    tab.addRow(new Row()
                            .addCell("id")
                            .addCell("transport addr")
                            .addCell("name"), true);
                    tab.addRow(new Row()
                            .addCell(clusterStateResponse.getState().nodes().masterNode().id())
                            .addCell(((InetSocketTransportAddress)clusterStateResponse.getState().nodes()
                                     .masterNode().address()).address().getHostString())
                            .addCell(clusterStateResponse.getState().nodes().masterNode().name()));

                    channel.sendResponse(new StringRestResponse(status, tab.render(verbose)));
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
}
