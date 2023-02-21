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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Returns the list of tasks currently running on the nodes
 */
public class ListTasksResponse extends BaseTasksResponse {
    private static final String TASKS = "tasks";

    private final List<TaskInfo> tasks;

    private Map<String, List<TaskInfo>> perNodeTasks;

    private List<TaskGroup> groups;

    public ListTasksResponse(
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskFailures,
        List<? extends ElasticsearchException> nodeFailures
    ) {
        super(taskFailures, nodeFailures);
        this.tasks = tasks == null ? List.of() : List.copyOf(tasks);
    }

    public ListTasksResponse(StreamInput in) throws IOException {
        super(in);
        tasks = in.readImmutableList(TaskInfo::from);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(tasks);
    }

    protected static <T> ConstructingObjectParser<T, Void> setupParser(
        String name,
        TriFunction<List<TaskInfo>, List<TaskOperationFailure>, List<ElasticsearchException>, T> ctor
    ) {
        ConstructingObjectParser<T, Void> parser = new ConstructingObjectParser<>(name, true, constructingObjects -> {
            int i = 0;
            @SuppressWarnings("unchecked")
            List<TaskInfo> tasks = (List<TaskInfo>) constructingObjects[i++];
            @SuppressWarnings("unchecked")
            List<TaskOperationFailure> tasksFailures = (List<TaskOperationFailure>) constructingObjects[i++];
            @SuppressWarnings("unchecked")
            List<ElasticsearchException> nodeFailures = (List<ElasticsearchException>) constructingObjects[i];
            return ctor.apply(tasks, tasksFailures, nodeFailures);
        });
        parser.declareObjectArray(optionalConstructorArg(), TaskInfo.PARSER, new ParseField(TASKS));
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> TaskOperationFailure.fromXContent(p), new ParseField(TASK_FAILURES));
        parser.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            new ParseField(NODE_FAILURES)
        );
        return parser;
    }

    private static final ConstructingObjectParser<ListTasksResponse, Void> PARSER = setupParser(
        "list_tasks_response",
        ListTasksResponse::new
    );

    /**
     * Returns the list of tasks by node
     */
    public Map<String, List<TaskInfo>> getPerNodeTasks() {
        if (perNodeTasks == null) {
            perNodeTasks = tasks.stream().collect(Collectors.groupingBy(t -> t.taskId().getNodeId()));
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
            taskGroups.put(taskInfo.taskId(), TaskGroup.builder(taskInfo));
        }

        // Now go through all task group builders and add children to their parents
        for (TaskGroup.Builder taskGroup : taskGroups.values()) {
            TaskId parentTaskId = taskGroup.getTaskInfo().parentTaskId();
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
        this.groups = topLevelTasks.stream().map(TaskGroup.Builder::build).toList();
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
    public ChunkedToXContentObject groupedByNode(Supplier<DiscoveryNodes> nodesInCluster) {
        return ignored -> {
            final var discoveryNodes = nodesInCluster.get();
            return Iterators.concat(Iterators.single((builder, params) -> {
                builder.startObject();
                toXContentCommon(builder, params);
                builder.startObject("nodes");
                return builder;
            }), Iterators.flatMap(getPerNodeTasks().entrySet().iterator(), entry -> {
                DiscoveryNode node = discoveryNodes.get(entry.getKey());
                return Iterators.concat(Iterators.single((builder, params) -> {
                    builder.startObject(entry.getKey());
                    if (node != null) {
                        // If the node is no longer part of the cluster, oh well, we'll just skip its useful information.
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
                    return builder;
                }), Iterators.flatMap(entry.getValue().iterator(), task -> Iterators.<ToXContent>single((builder, params) -> {
                    builder.startObject(task.taskId().toString());
                    task.toXContent(builder, params);
                    builder.endObject();
                    return builder;
                })), Iterators.<ToXContent>single((builder, params) -> {
                    builder.endObject();
                    builder.endObject();
                    return builder;
                }));
            }), Iterators.<ToXContent>single((builder, params) -> {
                builder.endObject();
                builder.endObject();
                return builder;
            }));
        };
    }

    /**
     * Convert this response to XContent grouping by parent tasks.
     */
    public ChunkedToXContentObject groupedByParent() {
        return ignored -> Iterators.concat(Iterators.single((builder, params) -> {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.startObject(TASKS);
            return builder;
        }), getTaskGroups().stream().<ToXContent>map(group -> (builder, params) -> {
            builder.field(group.taskInfo().taskId().toString());
            group.toXContent(builder, params);
            return builder;
        }).iterator(), Iterators.single((builder, params) -> {
            builder.endObject();
            builder.endObject();
            return builder;
        }));
    }

    /**
     * Presents a flat list of tasks
     */
    public ChunkedToXContentObject groupedByNone() {
        return ignored -> Iterators.concat(Iterators.single((builder, params) -> {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.startArray(TASKS);
            return builder;
        }), getTasks().stream().<ToXContent>map(taskInfo -> (builder, params) -> {
            builder.startObject();
            taskInfo.toXContent(builder, params);
            builder.endObject();
            return builder;
        }).iterator(), Iterators.single((builder, params) -> {
            builder.endArray();
            builder.endObject();
            return builder;
        }));
    }

    public static ListTasksResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(ChunkedToXContent.wrapAsToXContent(groupedByNone()), true, true);
    }

}
