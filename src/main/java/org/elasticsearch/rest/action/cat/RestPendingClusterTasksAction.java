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

import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestPendingClusterTasksAction extends AbstractCatAction {
    @Inject
    public RestPendingClusterTasksAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_cat/pending_tasks", this);
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat/pending_tasks\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel, final Client client) {
        PendingClusterTasksRequest pendingClusterTasksRequest = new PendingClusterTasksRequest();
        pendingClusterTasksRequest.masterNodeTimeout(request.paramAsTime("master_timeout", pendingClusterTasksRequest.masterNodeTimeout()));
        pendingClusterTasksRequest.local(request.paramAsBoolean("local", pendingClusterTasksRequest.local()));
        client.admin().cluster().pendingClusterTasks(pendingClusterTasksRequest, new RestResponseListener<PendingClusterTasksResponse>(channel) {
            @Override
            public RestResponse buildResponse(PendingClusterTasksResponse pendingClusterTasks) throws Exception {
                Table tab = buildTable(request, pendingClusterTasks);
                return RestTable.buildResponse(tab, channel);
            }
        });
    }

    @Override
    Table getTableWithHeader(final RestRequest request) {
        Table t = new Table();
        t.startHeaders();
        t.addCell("insertOrder", "alias:o;text-align:right;desc:task insertion order");
        t.addCell("timeInQueue", "alias:t;text-align:right;desc:how long task has been in queue");
        t.addCell("priority", "alias:p;desc:task priority");
        t.addCell("source", "alias:s;desc:task source");
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
