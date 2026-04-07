/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Objects;

public class ResumeBulkByScrollResponse extends ActionResponse {

    private final TaskId taskId;

    public ResumeBulkByScrollResponse(TaskId taskId) {
        this.taskId = Objects.requireNonNull(taskId, "taskId");
    }

    public ResumeBulkByScrollResponse(StreamInput in) throws IOException {
        this.taskId = TaskId.readFromStream(in);
    }

    public TaskId getTaskId() {
        return taskId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskId.writeTo(out);
    }

}
