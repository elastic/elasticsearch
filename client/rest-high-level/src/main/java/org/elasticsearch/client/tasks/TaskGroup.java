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
package org.elasticsearch.client.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Client side counterpart of server side version.
 *
 * {@link org.elasticsearch.action.admin.cluster.node.tasks.list.TaskGroup}
 */
public class TaskGroup {

    private final TaskInfo task;

    @Override
    public String toString() {
        return "TaskGroup{" +
            "task=" + task +
            ", childTasks=" + childTasks +
            '}';
    }

    private final List<TaskGroup> childTasks = new ArrayList<>();

    public TaskGroup(TaskInfo task, List<TaskGroup> childTasks) {
        this.task = task;
        this.childTasks.addAll(childTasks);
    }

    public static TaskGroup.Builder builder(TaskInfo taskInfo) {
        return new TaskGroup.Builder(taskInfo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskGroup)) return false;
        TaskGroup taskGroup = (TaskGroup) o;
        return Objects.equals(task, taskGroup.task) &&
            Objects.equals(getChildTasks(), taskGroup.getChildTasks());
    }

    @Override
    public int hashCode() {
        return Objects.hash(task, getChildTasks());
    }

    public static class Builder {
        private TaskInfo taskInfo;
        private List<TaskGroup.Builder> childTasks;

        private Builder(TaskInfo taskInfo) {
            this.taskInfo = taskInfo;
            childTasks = new ArrayList<>();
        }

        public void addGroup(TaskGroup.Builder builder) {
            childTasks.add(builder);
        }

        public TaskInfo getTaskInfo() {
            return taskInfo;
        }

        public TaskGroup build() {
            return new TaskGroup(
                taskInfo,
                childTasks.stream().map(TaskGroup.Builder::build).collect(Collectors.toList())
            );
        }
    }

    public TaskInfo getTaskInfo() {
        return task;
    }

    public List<TaskGroup> getChildTasks() {
        return childTasks;
    }
}

