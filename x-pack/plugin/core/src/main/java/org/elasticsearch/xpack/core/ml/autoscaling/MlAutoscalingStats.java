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
    int existingTotalProcessorsInUse,
    int minNodes,
    long extraPerNodeMemoryBytes,
    int extraPerNodeNodeProcessors,
    long extraModelMemoryBytes,
    int extraProcessors,
    long removeNodeMemoryBytes,
    long perNodeMemoryOverheadBytes
) implements Writeable {

    public MlAutoscalingStats(StreamInput in) throws IOException {
        this(
            in.readVInt(), // existingTotalNodes
            in.readVLong(),  // existingPerNodeMemoryBytes
            in.readVLong(), // modelMemoryInBytes
            in.readVInt(), // existingTotalProcessorsInUse
            in.readVInt(), // minNodes
            in.readVLong(), // extraPerNodeMemoryBytes
            in.readVInt(), // extraPerNodeNodeProcessors
            in.readVLong(), // extraModelMemoryBytes
            in.readVInt(), // extraProcessors
            in.readVLong(), // removeNodeMemoryBytes
            in.readVLong() // perNodeMemoryOverheadBytes
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(existingTotalNodes);
        out.writeVLong(existingPerNodeMemoryBytes);
        out.writeVLong(existingTotalModelMemoryBytes);
        out.writeVLong(existingTotalProcessorsInUse);
        out.writeVInt(minNodes);
        out.writeVLong(extraPerNodeMemoryBytes);
        out.writeVInt(extraPerNodeNodeProcessors);
        out.writeVLong(extraModelMemoryBytes);
        out.writeVInt(extraProcessors);
        out.writeVLong(removeNodeMemoryBytes);
        out.writeVLong(perNodeMemoryOverheadBytes);
    }
}
