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

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Returns the list of tasks currently running on the nodes
 */
public class ListTasksResponse extends BaseTasksResponse implements ToXContent {

    private List<TaskInfo> tasks;

    private Map<String, List<TaskInfo>> perNodeTasks;

    private List<TaskGroup> groups;

    private DiscoveryNodes discoveryNodes;

    public ListTasksResponse() {
    }

    public ListTasksResponse(List<TaskInfo> tasks, List<TaskOperationFailure> taskFailures,
            List<? extends FailedNodeException> nodeFailures) {
        super(taskFailures, nodeFailures);
        this.tasks = tasks == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(tasks));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tasks = Collections.unmodifiableList(in.readList(TaskInfo::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(tasks);
    }

    /**
     * Returns the list of tasks by node
     */
    public Map<String, List<TaskInfo>> getPerNodeTasks() {
        if (perNodeTasks == null) {
            perNodeTasks = tasks.stream().collect(Collectors.groupingBy(t -> t.getTaskId().getNodeId()));
        }
        return perNodeTasks;
    }

    public List<TaskGroup> getTaskGroups() {
        if (groups == null) {
            buildTaskGroups();
        }
        return groups;
    }

    private void buildTaskGroups() {
        Map<TaskId, TaskGroup.Builder> taskGroups = new HashMap<>();
        List<TaskGroup.Builder> topLevelTasks = new ArrayList<>();
        // First populate all tasks
        for (TaskInfo taskInfo : this.tasks) {
            taskGroups.put(taskInfo.getTaskId(), TaskGroup.builder(taskInfo));
        }

        // Now go through all task group builders and add children to their parents
        for (TaskGroup.Builder taskGroup : taskGroups.values()) {
            TaskId parentTaskId = taskGroup.getTaskInfo().getParentTaskId();
            if (parentTaskId.isSet()) {
                TaskGroup.Builder parentTask = taskGroups.get(parentTaskId);
                if (parentTask != null) {
                    // we found parent in the list of tasks - add it to the parent list
                    parentTask.addGroup(taskGroup);
                } else {
                    // we got zombie or the parent was filtered out - add it to the the top task list
                    topLevelTasks.add(taskGroup);
                }
            } else {
                // top level task - add it to the top task list
                topLevelTasks.add(taskGroup);
            }
        }
        this.groups = Collections.unmodifiableList(topLevelTasks.stream().map(TaskGroup.Builder::build).collect(Collectors.toList()));
    }

    public List<TaskInfo> getTasks() {
        return tasks;
    }

    /**
     * Set a reference to the {@linkplain DiscoveryNodes}. Used for calling {@link #toXContent(XContentBuilder, ToXContent.Params)} with
     * {@code group_by=nodes}.
     */
    public void setDiscoveryNodes(DiscoveryNodes discoveryNodes) {
        this.discoveryNodes = discoveryNodes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (getTaskFailures() != null && getTaskFailures().size() > 0) {
            builder.startArray("task_failures");
            for (TaskOperationFailure ex : getTaskFailures()){
                builder.startObject();
                builder.value(ex);
                builder.endObject();
            }
            builder.endArray();
        }

        if (getNodeFailures() != null && getNodeFailures().size() > 0) {
            builder.startArray("node_failures");
            for (FailedNodeException ex : getNodeFailures()) {
                builder.startObject();
                ex.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
        String groupBy = params.param("group_by", "nodes");
        if ("nodes".equals(groupBy)) {
            if (discoveryNodes == null) {
                throw new IllegalStateException("discoveryNodes must be set before calling toXContent with group_by=nodes");
            }
            builder.startObject("nodes");
            for (Map.Entry<String, List<TaskInfo>> entry : getPerNodeTasks().entrySet()) {
                DiscoveryNode node = discoveryNodes.get(entry.getKey());
                builder.startObject(entry.getKey());
                if (node != null) {
                    // If the node is no longer part of the cluster, oh well, we'll just skip it's useful information.
                    builder.field("name", node.getName());
                    builder.field("transport_address", node.getAddress().toString());
                    builder.field("host", node.getHostName());
                    builder.field("ip", node.getAddress());

                    builder.startArray("roles");
                    for (DiscoveryNode.Role role : node.getRoles()) {
                        builder.value(role.getRoleName());
                    }
                    builder.endArray();

                    if (!node.getAttributes().isEmpty()) {
                        builder.startObject("attributes");
                        for (Map.Entry<String, String> attrEntry : node.getAttributes().entrySet()) {
                            builder.field(attrEntry.getKey(), attrEntry.getValue());
                        }
                        builder.endObject();
                    }
                }
                builder.startObject("tasks");
                for(TaskInfo task : entry.getValue()) {
                    builder.field(task.getTaskId().toString());
                    task.toXContent(builder, params);
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endObject();
        } else if ("parents".equals(groupBy)) {
            builder.startObject("tasks");
            for (TaskGroup group : getTaskGroups()) {
                builder.field(group.getTaskInfo().getTaskId().toString());
                group.toXContent(builder, params);
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
