/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Returns the list of tasks currently running on the nodes
 */
public class ListTasksResponse extends BaseTasksResponse implements ToXContentObject {
    private static final String TASKS = "tasks";

    private final List<TaskInfo> tasks;

    private Map<String, List<TaskInfo>> perNodeTasks;

    private List<TaskGroup> groups;

    public ListTasksResponse(List<TaskInfo> tasks, List<TaskOperationFailure> taskFailures,
            List<? extends ElasticsearchException> nodeFailures) {
        super(taskFailures, nodeFailures);
        this.tasks = tasks == null ? Collections.emptyList() : List.copyOf(tasks);
    }

    public ListTasksResponse(StreamInput in) throws IOException {
        super(in);
        tasks = Collections.unmodifiableList(in.readList(TaskInfo::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(tasks);
    }

    protected static <T> ConstructingObjectParser<T, Void> setupParser(String name,
                                                                       TriFunction<
                                                                           List<TaskInfo>,
                                                                           List<TaskOperationFailure>,
                                                                           List<ElasticsearchException>,
                                                                           T> ctor) {
        ConstructingObjectParser<T, Void> parser = new ConstructingObjectParser<>(name, true,
            constructingObjects -> {
                int i = 0;
                @SuppressWarnings("unchecked")
                List<TaskInfo> tasks = (List<TaskInfo>) constructingObjects[i++];
                @SuppressWarnings("unchecked")
                List<TaskOperationFailure> tasksFailures = (List<TaskOperationFailure>) constructingObjects[i++];
                @SuppressWarnings("unchecked")
                List<ElasticsearchException> nodeFailures = (List<ElasticsearchException>) constructingObjects[i];
                return ctor.apply(tasks,tasksFailures, nodeFailures);
            });
        parser.declareObjectArray(optionalConstructorArg(), TaskInfo.PARSER, new ParseField(TASKS));
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> TaskOperationFailure.fromXContent(p), new ParseField(TASK_FAILURES));
        parser.declareObjectArray(optionalConstructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p), new ParseField(NODE_FAILURES));
        return parser;
    }

    private static final ConstructingObjectParser<ListTasksResponse, Void> PARSER =
        setupParser("list_tasks_response", ListTasksResponse::new);

    /**
     * Returns the list of tasks by node
     */
    public Map<String, List<TaskInfo>> getPerNodeTasks() {
        if (perNodeTasks == null) {
            perNodeTasks = tasks.stream().collect(Collectors.groupingBy(t -> t.getTaskId().getNodeId()));
        }
        return perNodeTasks;
    }

    /**
     * Get the tasks found by this request grouped by parent tasks.
     */
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
                    // we got zombie or the parent was filtered out - add it to the top task list
                    topLevelTasks.add(taskGroup);
                }
            } else {
                // top level task - add it to the top task list
                topLevelTasks.add(taskGroup);
            }
        }
        this.groups = Collections.unmodifiableList(topLevelTasks.stream().map(TaskGroup.Builder::build).collect(Collectors.toList()));
    }

    /**
     * Get the tasks found by this request.
     */
    public List<TaskInfo> getTasks() {
        return tasks;
    }

    /**
     * Convert this task response to XContent grouping by executing nodes.
     */
    public XContentBuilder toXContentGroupedByNode(XContentBuilder builder, Params params, DiscoveryNodes discoveryNodes)
            throws IOException {
        toXContentCommon(builder, params);
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
                for (DiscoveryNodeRole role : node.getRoles()) {
                    builder.value(role.roleName());
                }
                builder.endArray();

                if (node.getAttributes().isEmpty() == false) {
                    builder.startObject("attributes");
                    for (Map.Entry<String, String> attrEntry : node.getAttributes().entrySet()) {
                        builder.field(attrEntry.getKey(), attrEntry.getValue());
                    }
                    builder.endObject();
                }
            }
            builder.startObject(TASKS);
            for(TaskInfo task : entry.getValue()) {
                builder.startObject(task.getTaskId().toString());
                task.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    /**
     * Convert this response to XContent grouping by parent tasks.
     */
    public XContentBuilder toXContentGroupedByParents(XContentBuilder builder, Params params) throws IOException {
        toXContentCommon(builder, params);
        builder.startObject(TASKS);
        for (TaskGroup group : getTaskGroups()) {
            builder.field(group.getTaskInfo().getTaskId().toString());
            group.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Presents a flat list of tasks
     */
    public XContentBuilder toXContentGroupedByNone(XContentBuilder builder, Params params) throws IOException {
        toXContentCommon(builder, params);
        builder.startArray(TASKS);
        for (TaskInfo taskInfo : getTasks()) {
            builder.startObject();
            taskInfo.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentGroupedByNone(builder, params);
        builder.endObject();
        return builder;
    }

    public static ListTasksResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
