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

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestHealthAction extends AbstractCatAction {

    @Inject
    public RestHealthAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_cat/health", this);
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/health\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel, final Client client) {
        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest();

        client.admin().cluster().health(clusterHealthRequest, new RestResponseListener<ClusterHealthResponse>(channel) {
            @Override
            public RestResponse buildResponse(final ClusterHealthResponse health) throws Exception {
                return RestTable.buildResponse(buildTable(health, request), channel);
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table t = new Table();
        t.startHeaders();
        t.addCell("epoch", "alias:t,time;desc:seconds since 1970-01-01 00:00:00");
        t.addCell("timestamp", "alias:ts,hms,hhmmss;desc:time in HH:MM:SS");
        t.addCell("cluster", "alias:cl;desc:cluster name");
        t.addCell("status", "alias:st;desc:health status");
        t.addCell("node.total", "alias:nt,nodeTotal;text-align:right;desc:total number of nodes");
        t.addCell("node.data", "alias:nd,nodeData;text-align:right;desc:number of nodes that can store data");
        t.addCell("shards", "alias:t,sh,shards.total,shardsTotal;text-align:right;desc:total number of shards");
        t.addCell("pri", "alias:p,shards.primary,shardsPrimary;text-align:right;desc:number of primary shards");
        t.addCell("relo", "alias:r,shards.relocating,shardsRelocating;text-align:right;desc:number of relocating nodes");
        t.addCell("init", "alias:i,shards.initializing,shardsInitializing;text-align:right;desc:number of initializing nodes");
        t.addCell("unassign", "alias:u,shards.unassigned,shardsUnassigned;text-align:right;desc:number of unassigned shards");
        t.addCell("pending_tasks", "alias:pt,pendingTasks;text-align:right;desc:number of pending tasks");
        t.addCell("max_task_wait_time", "alias:mtwt,maxTaskWaitTime;text-align:right;desc:wait time of longest task pending");
        t.addCell("active_shards_percent", "alias:asp,activeShardsPercent;text-align:right;desc:active number of shards in percent");
        t.endHeaders();

        return t;
    }

    private DateTimeFormatter dateFormat = DateTimeFormat.forPattern("HH:mm:ss");

    private Table buildTable(final ClusterHealthResponse health, final RestRequest request) {
        long time = System.currentTimeMillis();
        Table t = getTableWithHeader(request);
        t.startRow();
        t.addCell(TimeUnit.SECONDS.convert(time, TimeUnit.MILLISECONDS));
        t.addCell(dateFormat.print(time));
        t.addCell(health.getClusterName());
        t.addCell(health.getStatus().name().toLowerCase(Locale.ROOT));
        t.addCell(health.getNumberOfNodes());
        t.addCell(health.getNumberOfDataNodes());
        t.addCell(health.getActiveShards());
        t.addCell(health.getActivePrimaryShards());
        t.addCell(health.getRelocatingShards());
        t.addCell(health.getInitializingShards());
        t.addCell(health.getUnassignedShards());
        t.addCell(health.getNumberOfPendingTasks());
        t.addCell(health.getTaskMaxWaitingTime().millis() == 0 ? "-" : health.getTaskMaxWaitingTime());
        t.addCell(String.format(Locale.ROOT, "%1.1f%%", health.getActiveShardsPercent()));
        t.endRow();
        return t;
    }
}
