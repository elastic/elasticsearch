/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.set.Sets.addToCopy;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.action.admin.cluster.RestListTasksAction.generateListTasksRequest;

@ServerlessScope(Scope.INTERNAL)
public class RestTasksAction extends AbstractCatAction {

    private static final Set<String> RESPONSE_PARAMS = addToCopy(AbstractCatAction.RESPONSE_PARAMS, "detailed");

    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestTasksAction(Supplier<DiscoveryNodes> nodesInCluster) {
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/tasks"));
    }

    @Override
    public String getName() {
        return "cat_tasks_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/tasks\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final ListTasksRequest listTasksRequest = generateListTasksRequest(request);
        return channel -> client.admin().cluster().listTasks(listTasksRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(ListTasksResponse listTasksResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, listTasksResponse), channel);
            }
        });
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
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
        table.addCell("x_opaque_id", "default:false;alias:x;desc:X-Opaque-ID header");

        // Task detailed info
        if (detailed) {
            table.addCell("description", "default:true;alias:desc;desc:task action");
        }
        table.endHeaders();
        return table;
    }

    private static final DateFormatter FORMATTER = DateFormatter.forPattern("HH:mm:ss").withZone(ZoneOffset.UTC);

    private static void buildRow(Table table, boolean fullId, boolean detailed, DiscoveryNodes discoveryNodes, TaskInfo taskInfo) {
        table.startRow();
        String nodeId = taskInfo.taskId().getNodeId();
        DiscoveryNode node = discoveryNodes.get(nodeId);

        table.addCell(taskInfo.id());
        table.addCell(taskInfo.action());
        table.addCell(taskInfo.taskId().toString());
        if (taskInfo.parentTaskId().isSet()) {
            table.addCell(taskInfo.parentTaskId().toString());
        } else {
            table.addCell("-");
        }
        table.addCell(taskInfo.type());
        table.addCell(taskInfo.startTime());
        table.addCell(FORMATTER.format(Instant.ofEpochMilli(taskInfo.startTime())));
        table.addCell(taskInfo.runningTimeNanos());
        table.addCell(TimeValue.timeValueNanos(taskInfo.runningTimeNanos()));

        // Node information. Note that the node may be null because it has left the cluster between when we got this response and now.
        table.addCell(fullId ? nodeId : Strings.substring(nodeId, 0, 4));
        table.addCell(node == null ? "-" : node.getHostAddress());
        table.addCell(node.getAddress().address().getPort());
        table.addCell(node == null ? "-" : node.getName());
        table.addCell(node == null ? "-" : node.getVersion().toString());
        table.addCell(taskInfo.headers().getOrDefault(Task.X_OPAQUE_ID_HTTP_HEADER, "-"));

        if (detailed) {
            table.addCell(taskInfo.description());
        }
        table.endRow();
    }

    private void buildGroups(Table table, boolean fullId, boolean detailed, List<TaskGroup> taskGroups) {
        DiscoveryNodes discoveryNodes = nodesInCluster.get();
        List<TaskGroup> sortedGroups = new ArrayList<>(taskGroups);
        sortedGroups.sort(Comparator.comparingLong(o -> o.taskInfo().startTime()));
        for (TaskGroup taskGroup : sortedGroups) {
            buildRow(table, fullId, detailed, discoveryNodes, taskGroup.taskInfo());
            buildGroups(table, fullId, detailed, taskGroup.childTasks());
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
