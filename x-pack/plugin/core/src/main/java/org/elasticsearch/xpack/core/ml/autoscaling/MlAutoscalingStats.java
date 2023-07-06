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
    int nodes,
    long memoryInBytesSum,
    long modelMemoryInBytesSum,
    int minNodes,
    long extraSingleNodeModelMemoryInBytes,
    int extraSingleNodeProcessors,
    long extraModelMemoryInBytes,
    int extraProcessors,
    long removeNodeMemoryInBytes,
    long perNodeMemoryOverheadInBytes
) implements Writeable {

    public MlAutoscalingStats(StreamInput in) throws IOException {
        this(
            in.readVInt(), // nodes
            in.readVLong(),  // memoryInBytesSum
            in.readVLong(), // modelMemoryInBytes
            in.readVInt(), // minNodes
            in.readVLong(), // extraSingleNodeModelMemoryInBytes
            in.readVInt(), // extraSingleNodeProcessors
            in.readVLong(), // extraModelMemoryInBytes
            in.readVInt(), // extraProcessors
            in.readVLong(), // removeNodeMemoryInBytes
            in.readVLong() // perNodeMemoryOverheadInBytes
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(nodes);
        out.writeVLong(memoryInBytesSum);
        out.writeVLong(modelMemoryInBytesSum);
        out.writeVInt(minNodes);
        out.writeVLong(extraSingleNodeModelMemoryInBytes);
        out.writeVInt(extraSingleNodeProcessors);
        out.writeVLong(extraModelMemoryInBytes);
        out.writeVInt(extraProcessors);
        out.writeVLong(removeNodeMemoryInBytes);
        out.writeVLong(perNodeMemoryOverheadInBytes);
    }
}
