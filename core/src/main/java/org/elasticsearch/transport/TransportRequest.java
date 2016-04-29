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

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

/**
 */
public abstract class TransportRequest extends TransportMessage {
    public static class Empty extends TransportRequest {
        public static final Empty INSTANCE = new Empty();
    }

    /**
     * Parent of this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "no parent".
     */
    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    public TransportRequest() {
    }

    /**
     * Set a reference to task that caused this task to be run.
     */
    public void setParentTask(String parentTaskNode, long parentTaskId) {
        setParentTask(new TaskId(parentTaskNode, parentTaskId));
    }

    /**
     * Set a reference to task that created this request.
     */
    public void setParentTask(TaskId taskId) {
        this.parentTaskId = taskId;
    }

    /**
     * Get a reference to the task that created this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "there is no parent".
     */
    public TaskId getParentTask() {
        return parentTaskId;
    }

    /**
     * Returns the task object that should be used to keep track of the processing of the request.
     *
     * A request can override this method and return null to avoid being tracked by the task manager.
     */
    public Task createTask(long id, String type, String action, TaskId parentTaskId) {
        return new Task(id, type, action, getDescription(), parentTaskId);
    }

    /**
     * Returns optional description of the request to be displayed by the task manager
     */
    public String getDescription() {
        return "";
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        parentTaskId = TaskId.readFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        parentTaskId.writeTo(out);
    }
}
