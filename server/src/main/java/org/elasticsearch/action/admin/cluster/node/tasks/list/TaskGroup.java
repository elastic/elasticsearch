/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Information about a currently running task and all its subtasks.
 */
public record TaskGroup(TaskInfo task, List<TaskGroup> childTasks) implements ToXContentObject {

    public TaskGroup(TaskInfo task, List<TaskGroup> childTasks) {
        this.task = task;
        this.childTasks = List.copyOf(childTasks);
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
            return new TaskGroup(taskInfo, childTasks.stream().map(Builder::build).toList());
        }
    }

    public TaskInfo taskInfo() {
        return task;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        task.toXContent(builder, params);
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
