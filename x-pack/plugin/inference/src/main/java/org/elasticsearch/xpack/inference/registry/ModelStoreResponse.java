/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public record ModelStoreResponse(String inferenceId, RestStatus status, @Nullable Exception failureCause) implements Writeable {

    public ModelStoreResponse(StreamInput in) throws IOException {
        this(
            in.readString(),
            RestStatus.readFrom(in),
            in.readException()
        );
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
}
