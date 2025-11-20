/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;

/**
 * Response for storing a model in the model registry using the bulk API.
 */
public record ModelStoreResponse(String inferenceId, RestStatus status, @Nullable Exception failureCause) implements Writeable {

    public ModelStoreResponse(StreamInput in) throws IOException {
        this(in.readString(), RestStatus.readFrom(in), in.readException());
    }

    public boolean failed() {
        return failureCause != null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(inferenceId);
        RestStatus.writeTo(out, status);
        out.writeException(failureCause);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ModelStoreResponse that = (ModelStoreResponse) o;
        return status == that.status && Objects.equals(inferenceId, that.inferenceId)
        // Exception does not have hashCode() or equals() so assume errors are equal iff class and message are equal
            && Objects.equals(
                failureCause == null ? null : failureCause.getMessage(),
                that.failureCause == null ? null : that.failureCause.getMessage()
            )
            && Objects.equals(
                failureCause == null ? null : failureCause.getClass(),
                that.failureCause == null ? null : that.failureCause.getClass()
            );
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            inferenceId,
            status,
            // Exception does not have hashCode() or equals() so assume errors are equal iff class and message are equal
            failureCause == null ? null : failureCause.getMessage(),
            failureCause == null ? null : failureCause.getClass()
        );
    }
}
