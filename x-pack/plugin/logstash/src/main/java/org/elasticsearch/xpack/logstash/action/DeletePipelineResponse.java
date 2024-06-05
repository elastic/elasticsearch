/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class DeletePipelineResponse extends ActionResponse {

    private final boolean deleted;

    public DeletePipelineResponse(boolean deleted) {
        this.deleted = deleted;
    }

    public DeletePipelineResponse(StreamInput in) throws IOException {
        super(in);
        this.deleted = in.readBoolean();
    }

    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(deleted);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeletePipelineResponse that = (DeletePipelineResponse) o;
        return deleted == that.deleted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(deleted);
    }
}
