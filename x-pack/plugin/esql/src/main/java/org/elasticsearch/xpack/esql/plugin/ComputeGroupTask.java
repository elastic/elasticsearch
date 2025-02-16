/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.function.Supplier;

class ComputeGroupTask {
    public static CancellableTask createGroupTask(TransportService transportService, Task parentTask, Supplier<String> description) {
        final TaskManager taskManager = transportService.getTaskManager();
        return (CancellableTask) taskManager.register(
            "transport",
            "esql_compute_group",
            new ComputeGroupTaskRequest(parentTask.taskInfo(transportService.getLocalNode().getId(), false).taskId(), description)
        );
    }

    private static class ComputeGroupTaskRequest extends TransportRequest {
        private final Supplier<String> parentDescription;

        ComputeGroupTaskRequest(TaskId parentTask, Supplier<String> description) {
            this.parentDescription = description;
            setParentTask(parentTask);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            assert parentTaskId.isSet();
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return "group [" + parentDescription.get() + "]";
        }
    }
}
