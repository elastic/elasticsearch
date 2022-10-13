/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

public abstract class TransportRequest extends TransportMessage implements TaskAwareRequest {
    public static class Empty extends TransportRequest {
        public static final Empty INSTANCE = new Empty();

        public Empty() {}

        public Empty(StreamInput in) throws IOException {
            super(in);
        }
    }

    /**
     * Parent of this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "no parent".
     */
    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    public TransportRequest() {}

    public TransportRequest(StreamInput in) throws IOException {
        parentTaskId = TaskId.readFromStream(in);
    }

    /**
     * Set a reference to task that created this request.
     */
    @Override
    public void setParentTask(TaskId taskId) {
        this.parentTaskId = taskId;
    }

    /**
     * Get a reference to the task that created this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "there is no parent".
     */
    @Override
    public TaskId getParentTask() {
        return parentTaskId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        parentTaskId.writeTo(out);
    }

    @Override
    public String toString() {
        return getClass().getName() + "/" + getParentTask();
    }
}
