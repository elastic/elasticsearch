/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.autoscaling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record MlAutoscalingStats(
    int existingTotalNodes,
    long existingPerNodeMemoryBytes,
    long existingTotalModelMemoryBytes,
    int existingTotalProcessors,
    int minNodes,
    long extraPerNodeMemoryBytes,
    int extraPerNodeNodeProcessors,
    long extraModelMemoryInBytes,
    int extraProcessors,
    long removeNodeMemoryInBytes,
    long perNodeMemoryOverheadInBytes
) implements Writeable {

    public MlAutoscalingStats(StreamInput in) throws IOException {
        this(
            in.readVInt(), // existingTotalNodes
            in.readVLong(),  // existingPerNodeMemoryBytes
            in.readVLong(), // modelMemoryInBytes
            in.readVInt(), // existingTotalProcessors
            in.readVInt(), // minNodes
            in.readVLong(), // extraPerNodeMemoryBytes
            in.readVInt(), // extraPerNodeNodeProcessors
            in.readVLong(), // extraModelMemoryInBytes
            in.readVInt(), // extraProcessors
            in.readVLong(), // removeNodeMemoryInBytes
            in.readVLong() // perNodeMemoryOverheadInBytes
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(existingTotalNodes);
        out.writeVLong(existingPerNodeMemoryBytes);
        out.writeVLong(existingTotalModelMemoryBytes);
        out.writeVLong(existingTotalProcessors);
        out.writeVInt(minNodes);
        out.writeVLong(extraPerNodeMemoryBytes);
        out.writeVInt(extraPerNodeNodeProcessors);
        out.writeVLong(extraModelMemoryInBytes);
        out.writeVInt(extraProcessors);
        out.writeVLong(removeNodeMemoryInBytes);
        out.writeVLong(perNodeMemoryOverheadInBytes);
    }
}
