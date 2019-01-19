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
     * Returns optional description of the request to be displayed by the task manager
     */
    default String getDescription() {
        return "";
    }
}
