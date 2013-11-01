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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.table.TimestampedTable;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestHealthAction extends BaseRestHandler {

    @Inject
    public RestHealthAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/health", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest();
        final boolean timeStamp = request.paramAsBoolean("ts", true);

        client.admin().cluster().health(clusterHealthRequest, new ActionListener<ClusterHealthResponse>() {
            @Override
            public void onResponse(final ClusterHealthResponse health) {
                try {
                    channel.sendResponse(RestTable.buildResponse(buildTable(health, timeStamp), request, channel));
                } catch (Throwable t) {
                    onFailure(t);
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

    private Table buildTable (final ClusterHealthResponse health, boolean timeStamp) {
        Table t;

        if (timeStamp) {
            t = new TimestampedTable();
        } else {
            t = new Table();
        }

        if (null != health) {
            t.startHeaders();
            t.addCell("cluster");
            t.addCell("status");
            t.addCell("nodeTotal", "text-align:right;");
            t.addCell("nodeData", "text-align:right;");
            t.addCell("shards", "text-align:right;");
            t.addCell("pri", "text-align:right;");
            t.addCell("relo", "text-align:right;");
            t.addCell("init", "text-align:right;");
            t.addCell("unassign", "text-align:right;");
            t.endHeaders();

            t.startRow();
            t.addCell(health.getClusterName());
            t.addCell(health.getStatus().name().toLowerCase(Locale.ROOT));
            t.addCell(health.getNumberOfNodes());
            t.addCell(health.getNumberOfDataNodes());
            t.addCell(health.getActiveShards());
            t.addCell(health.getActivePrimaryShards());
            t.addCell(health.getRelocatingShards());
            t.addCell(health.getInitializingShards());
            t.addCell(health.getUnassignedShards());
            t.endRow();
        }
        return t;
    }
}
