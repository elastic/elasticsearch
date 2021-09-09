/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;

import java.io.IOException;
import java.util.Objects;

/**
 * Response upon a successful start or an persistent task
 */
public class PersistentTaskResponse extends ActionResponse {
    private PersistentTask<?> task;

    public PersistentTaskResponse(StreamInput in) throws IOException {
        super(in);
        task = in.readOptionalWriteable(PersistentTask::new);
    }

    public PersistentTaskResponse(PersistentTask<?> task) {
        this.task = task;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(task);
    }

    public PersistentTask<?> getTask() {
        return task;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersistentTaskResponse that = (PersistentTaskResponse) o;
        return Objects.equals(task, that.task);
    }

    @Override
    public int hashCode() {
        return Objects.hash(task);
    }
}
