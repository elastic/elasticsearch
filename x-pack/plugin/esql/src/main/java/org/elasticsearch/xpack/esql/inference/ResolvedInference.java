/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;

import java.io.IOException;

public record ResolvedInference(String inferenceId, TaskType taskType) implements Writeable {

    public ResolvedInference(StreamInput in) throws IOException {
        this(in.readString(), TaskType.valueOf(in.readString()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(inferenceId);
        out.writeString(taskType.name());
    }
}
