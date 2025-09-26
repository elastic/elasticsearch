/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public record FullyQualifiedInferenceId(String clusterAlias, String inferenceId) implements Writeable {
    public FullyQualifiedInferenceId(String clusterAlias, String inferenceId) {
        this.clusterAlias = Objects.requireNonNull(clusterAlias);
        this.inferenceId = Objects.requireNonNull(inferenceId);
    }

    public FullyQualifiedInferenceId(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clusterAlias);
        out.writeString(inferenceId);
    }

    @Override
    public String toString() {
        return "{clusterAlias=" + clusterAlias + ", inferenceId=" + inferenceId + "}";
    }
}
