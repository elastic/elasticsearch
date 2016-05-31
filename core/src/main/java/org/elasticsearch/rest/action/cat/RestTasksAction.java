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

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;
import org.elasticsearch.tasks.TaskInfo;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.action.admin.cluster.node.tasks.RestListTasksAction.generateListTasksRequest;

public class RestTasksAction extends AbstractCatAction {
    private final ClusterService clusterService;

    @Inject
    public RestTasksAction(Settings settings, RestController controller, Client client, ClusterService clusterService) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_cat/tasks", this);
        this.clusterService = clusterService;
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/tasks\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel, final Client client) {
        client.admin().cluster().listTasks(generateListTasksRequest(request), new RestResponseListener<ListTasksResponse>(channel) {
            @Override
            public RestResponse buildResponse(ListTasksResponse listTasksResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, listTasksResponse), channel);
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        boolean detailed = request.paramAsBoolean("detailed", false);
        Table table = new Table();
        table.startHeaders();

        // Task main info
        table.addCell("id", "default:false;desc:id of the task with the node");
        table.addCell("action", "alias:ac;desc:task action");
        table.addCell("task_id", "alias:ti;desc:unique task id");
        table.addCell("parent_task_id", "alias:pti;desc:parent task id");
        table.addCell("type", "alias:ty;desc:task type");
        table.addCell("start_time", "alias:start;desc:start time in ms");
        table.addCell("timestamp", "alias:ts,hms,hhmmss;desc:start time in HH:MM:SS");
        table.addCell("running_time_ns", "default:false;alias:time;desc:running time ns");
        table.addCell("running_time", "default:true;alias:time;desc:running time");

        // Node info
        table.addCell("node_id", "default:false;alias:ni;desc:unique node id");
        table.addCell("ip", "default:true;alias:i;desc:ip address");
        table.addCell("port", "default:false;alias:po;desc:bound transport port");
        table.addCell("node", "default:true;alias:n;desc:node name");
        table.addCell("version", "default:false;alias:v;desc:es version");

        // Task detailed info
        if (detailed) {
            table.addCell("description", "default:false;alias:desc;desc:task action");
        }
        table.endHeaders();
        return table;
    }

    private DateTimeFormatter dateFormat = DateTimeFormat.forPattern("HH:mm:ss");

    private void buildRow(Table table, boolean fullId, boolean detailed, DiscoveryNodes discoveryNodes, TaskInfo taskInfo) {
        table.startRow();
        String nodeId = taskInfo.getTaskId().getNodeId();
        DiscoveryNode node = discoveryNodes.get(nodeId);

        table.addCell(taskInfo.getId());
        table.addCell(taskInfo.getAction());
        table.addCell(taskInfo.getTaskId().toString());
        if (taskInfo.getParentTaskId().isSet()) {
            table.addCell(taskInfo.getParentTaskId().toString());
        } else {
            table.addCell("-");
        }
        table.addCell(taskInfo.getType());
        table.addCell(taskInfo.getStartTime());
        table.addCell(dateFormat.print(taskInfo.getStartTime()));
        table.addCell(taskInfo.getRunningTimeNanos());
        table.addCell(TimeValue.timeValueNanos(taskInfo.getRunningTimeNanos()).toString());

        // Node information. Note that the node may be null because it has left the cluster between when we got this response and now.
        table.addCell(fullId ? nodeId : Strings.substring(nodeId, 0, 4));
        table.addCell(node == null ? "-" : node.getHostAddress());
        if (node != null && node.getAddress() instanceof InetSocketTransportAddress) {
            table.addCell(((InetSocketTransportAddress) node.getAddress()).address().getPort());
        } else {
            table.addCell("-");
        }
        table.addCell(node == null ? "-" : node.getName());
        table.addCell(node == null ? "-" : node.getVersion().toString());

        if (detailed) {
            table.addCell(taskInfo.getDescription());
        }
        table.endRow();
    }

    private void buildGroups(Table table, boolean detailed, boolean fullId, List<TaskGroup> taskGroups) {
        DiscoveryNodes discoveryNodes = clusterService.state().nodes();
        List<TaskGroup> sortedGroups = new ArrayList<>(taskGroups);
        sortedGroups.sort((o1, o2) -> Long.compare(o1.getTaskInfo().getStartTime(), o2.getTaskInfo().getStartTime()));
        for (TaskGroup taskGroup : sortedGroups) {
            buildRow(table, fullId, detailed, discoveryNodes, taskGroup.getTaskInfo());
            buildGroups(table, fullId, detailed, taskGroup.getChildTasks());
        }
    }

    private Table buildTable(RestRequest request, ListTasksResponse listTasksResponse) {
        boolean fullId = request.paramAsBoolean("full_id", false);
        boolean detailed = request.paramAsBoolean("detailed", false);
        Table table = getTableWithHeader(request);
        buildGroups(table, fullId, detailed, listTasksResponse.getTaskGroups());
        return table;
    }
}
