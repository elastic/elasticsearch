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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Information about a currently running task and all its subtasks.
 */
public class TaskGroup implements ToXContent {

    private final TaskInfo task;

    private final List<TaskGroup> childTasks;


    public TaskGroup(TaskInfo task, List<TaskGroup> childTasks) {
        this.task = task;
        this.childTasks = Collections.unmodifiableList(new ArrayList<>(childTasks));
    }

    public static Builder builder(TaskInfo taskInfo) {
        return new Builder(taskInfo);
    }

    public static class Builder {
        private TaskInfo taskInfo;
        private List<Builder> childTasks;

        private Builder(TaskInfo taskInfo) {
            this.taskInfo = taskInfo;
            childTasks = new ArrayList<>();
        }

        public void addGroup(Builder builder) {
            childTasks.add(builder);
        }

        public TaskInfo getTaskInfo() {
            return taskInfo;
        }

        public TaskGroup build() {
            return new TaskGroup(taskInfo, childTasks.stream().map(Builder::build).collect(Collectors.toList()));
        }
    }

    public TaskInfo getTaskInfo() {
        return task;
    }

    public List<TaskGroup> getChildTasks() {
        return childTasks;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        task.innerToXContent(builder, params);
        if (childTasks.isEmpty() == false) {
            builder.startArray("children");
            for (TaskGroup taskGroup : childTasks) {
                taskGroup.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder.endObject();
    }
}
