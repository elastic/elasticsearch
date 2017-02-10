/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Response upon a successful start or an persistent action
 */
public class PersistentActionResponse extends ActionResponse {
    private long taskId;

    public PersistentActionResponse() {
        super();
    }

    public PersistentActionResponse(long taskId) {
        this.taskId = taskId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        taskId = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(taskId);
    }

    public long getTaskId() {
        return taskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersistentActionResponse that = (PersistentActionResponse) o;
        return taskId == that.taskId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId);
    }
}
