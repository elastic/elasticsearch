/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import java.util.Map;

/**
 * An interface for a request that can be used to register a task manager task
 */
public interface TaskAwareRequest {
    /**
     * Set a reference to task that caused this task to be run.
     */
    default void setParentTask(String parentTaskNode, long parentTaskId) {
        setParentTask(new TaskId(parentTaskNode, parentTaskId));
    }

    /**
     * Set a reference to task that created this request.
     */
    void setParentTask(TaskId taskId);

    /**
     * Gets the request ID. Defaults to -1, meaning "no request ID is set".
     */
    default long getRequestId() {
        return -1;
    }

    /**
     * Set the request ID related to this task.
     */
    void setRequestId(long requestId);

    /**
     * Get a reference to the task that created this request. Implementers should default to
     * {@link TaskId#EMPTY_TASK_ID}, meaning "there is no parent".
     */
    TaskId getParentTask();

    /**
     * Returns the task object that should be used to keep track of the processing of the request.
     */
    default Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new Task(id, type, action, getDescription(), parentTaskId, headers);
    }

    /**
     * Returns the task object that should be used to keep track of the processing of the request, with an extra local node ID.
     */
    default Task createTask(TaskId taskId, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return createTask(taskId.getId(), type, action, parentTaskId, headers);
    }

    /**
     * Returns optional description of the request to be displayed by the task manager
     */
    default String getDescription() {
        return "";
    }
}
