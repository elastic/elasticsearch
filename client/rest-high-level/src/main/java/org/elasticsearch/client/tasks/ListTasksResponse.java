/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class ListTasksResponse {

    protected final List<TaskOperationFailure> taskFailures = new ArrayList<>();
    protected final List<ElasticsearchException> nodeFailures = new ArrayList<>();
    protected final List<NodeData> nodesInfoData = new ArrayList<>();
    protected final List<TaskInfo> tasks = new ArrayList<>();
    protected final List<TaskGroup> taskGroups = new ArrayList<>();

    ListTasksResponse(List<NodeData> nodesInfoData,
                        List<TaskOperationFailure> taskFailures,
                        List<ElasticsearchException> nodeFailures) {
        if (taskFailures != null) {
            this.taskFailures.addAll(taskFailures);
        }
        if (nodeFailures != null) {
            this.nodeFailures.addAll(nodeFailures);
        }
        if (nodesInfoData != null) {
            this.nodesInfoData.addAll(nodesInfoData);
        }
        this.tasks.addAll(this
            .nodesInfoData
            .stream()
            .flatMap(nodeData -> nodeData.getTasks().stream())
            .collect(toList())
        );
        this.taskGroups.addAll(buildTaskGroups());
    }

    private List<TaskGroup> buildTaskGroups() {
        Map<TaskId, TaskGroup.Builder> taskGroups = new HashMap<>();
        List<TaskGroup.Builder> topLevelTasks = new ArrayList<>();
        // First populate all tasks
        for (TaskInfo taskInfo : this.tasks) {
            taskGroups.put(taskInfo.getTaskId(), TaskGroup.builder(taskInfo));
        }

        // Now go through all task group builders and add children to their parents
        for (TaskGroup.Builder taskGroup : taskGroups.values()) {
            TaskId parentTaskId = taskGroup.getTaskInfo().getParentTaskId();
            if (parentTaskId != null) {
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
        return topLevelTasks.stream().map(TaskGroup.Builder::build).collect(Collectors.toUnmodifiableList());
    }

    public List<TaskInfo> getTasks() {
        return tasks;
    }

    public Map<String, List<TaskInfo>> getPerNodeTasks() {
        return getTasks()
            .stream()
            .collect(groupingBy(TaskInfo::getNodeId));
    }

    public List<TaskOperationFailure> getTaskFailures() {
        return taskFailures;
    }

    public List<ElasticsearchException> getNodeFailures() {
        return nodeFailures;
    }

    public List<TaskGroup> getTaskGroups() {
        return taskGroups;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof ListTasksResponse) == false) return false;
        ListTasksResponse response = (ListTasksResponse) o;
        return nodesInfoData.equals(response.nodesInfoData) &&
            Objects.equals
                (getTaskFailures(), response.getTaskFailures()) &&
            Objects.equals(getNodeFailures(), response.getNodeFailures()) &&
            Objects.equals(getTasks(), response.getTasks()) &&
            Objects.equals(getTaskGroups(), response.getTaskGroups());
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodesInfoData, getTaskFailures(), getNodeFailures(), getTasks(), getTaskGroups());
    }

    @Override
    public String toString() {
        return "CancelTasksResponse{" +
            "nodesInfoData=" + nodesInfoData +
            ", taskFailures=" + taskFailures +
            ", nodeFailures=" + nodeFailures +
            ", tasks=" + tasks +
            ", taskGroups=" + taskGroups +
            '}';
    }
}
