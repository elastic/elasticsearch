/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a Generative AI
 */

package org.elasticsearch.xpack.core.ml.autoscaling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * MlAutoscalingStats is the record which is transmitted to the elasticsearch-autoscaler to decide which nodes to deliver.
 * <p>
 * The "existing" attributes exist only so that the autoscaler can confirm that elasticsearch has the same view of the available hardware.
 * <p>
 * The "extra" attributes are used to communicate the additional resources that are required.
 * <p>
 * The "perNode" attributes define the minimum amount of resources that must be available on every node.
 * <p>
 * removeNodeMemoryBytes is used to communicate the amount of memory that should be removed from the node. No attribute exists to remove
 * processors.
 * <p>
 * The word "total" in an attribute name indicates that the attribute is a sum across all nodes.
 *
 * @param existingTotalNodes the count of nodes that are currently in the cluster
 * @param existingPerNodeMemoryBytes the minimum size (memory) of all nodes in the cluster
 * @param existingTotalModelMemoryBytes the sum of model memory over every assignment/deployment
 * @param existingTotalProcessorsInUse the sum of processors used over every assignment/deployment
 *
 * @param minNodes the minimum number of nodes that must be provided by the autoscaler
 *
 * @param extraPerNodeMemoryBytes the amount of additional memory that must be provided on every node (this value must be >0 to trigger a
 *                                scale up based on memory)
 * @param extraPerNodeNodeProcessors the number of additional processors that must be provided on every node (this value must be >0 to
 *                                   trigger a scale up based on processors)
 * @param extraModelMemoryBytes the amount of additional model memory that is newly required (due to a new assignment/deployment)
 * @param extraProcessors the number of additional processors that are required to be added to the cluster
 *
 * @param removeNodeMemoryBytes the amount of memory that should be removed from the cluster. If this is equal to the amount of memory
 *                              provided by a node, a node will be removed.
 *
 * @param perNodeMemoryOverheadBytes always equal to MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD
 */

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
