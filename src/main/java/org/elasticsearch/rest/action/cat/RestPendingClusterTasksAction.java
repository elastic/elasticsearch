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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestPendingClusterTasksAction extends AbstractCatAction {
    @Inject
    public RestPendingClusterTasksAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/pending_tasks", this);
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat/pending_tasks\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel) {
        PendingClusterTasksRequest pendingClusterTasksRequest = new PendingClusterTasksRequest();
        pendingClusterTasksRequest.masterNodeTimeout(request.paramAsTime("master_timeout", pendingClusterTasksRequest.masterNodeTimeout()));
        pendingClusterTasksRequest.local(request.paramAsBoolean("local", pendingClusterTasksRequest.local()));
        client.admin().cluster().pendingClusterTasks(pendingClusterTasksRequest, new ActionListener<PendingClusterTasksResponse>() {
            @Override
            public void onResponse(PendingClusterTasksResponse pendingClusterTasks) {
                try {
                    Table tab = buildTable(request, pendingClusterTasks);
                    channel.sendResponse(RestTable.buildResponse(tab, request, channel));
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
    Table getTableWithHeader(final RestRequest request) {
        Table t = new Table();
        t.startHeaders();
        t.addCell("insertOrder", "text-align:right;desc:task insertion order");
        t.addCell("timeInQueue", "text-align:right;desc:how long task has been in queue");
        t.addCell("priority", "desc:task priority");
        t.addCell("source", "desc:task source");
        t.endHeaders();
        return t;
    }

    private Table buildTable(RestRequest request, PendingClusterTasksResponse tasks) {
        Table t = getTableWithHeader(request);

        for (PendingClusterTask task : tasks) {
            t.startRow();
            t.addCell(task.getInsertOrder());
            t.addCell(task.getTimeInQueue());
            t.addCell(task.getPriority());
            t.addCell(task.getSource());
            t.endRow();
        }

        return t;
    }
}
