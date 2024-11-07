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
 * unwantedNodeMemoryBytesToRemove is used to communicate the amount of memory that should be removed from the node.
 * No attribute exists to remove processors.
 * <p>
 * The word "total" in an attribute name indicates that the attribute is a sum across all nodes.
 *
 * @param currentTotalNodes                 The count of nodes that are currently in the cluster,
 *                                          used to confirm that both sides have same view of current state
 * @param currentPerNodeMemoryBytes         The minimum size (memory) of all nodes in the cluster
 *                                          used to confirm that both sides have same view of current state.
 * @param currentTotalModelMemoryBytes      The sum of model memory over every assignment/deployment, used to calculate requirements
 * @param currentTotalProcessorsInUse       The sum of processors used over every assignment/deployment, not used by autoscaler
 * @param currentPerNodeMemoryOverheadBytes Always equal to MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD,
 * @param wantedMinNodes                    The minimum number of nodes that must be provided by the autoscaler
 * @param wantedExtraPerNodeMemoryBytes     If there are jobs or trained models that have been started but cannot be allocated on the
 *                                          ML nodes currently within the cluster then this will be the *max* of the ML native memory
 *                                          requirements of those jobs/trained models. The metric is in terms of ML native memory,
 *                                          not container memory.
 * @param wantedExtraPerNodeNodeProcessors  If there are trained model allocations that have been started but cannot be allocated on the
 *                                          ML nodes currently within the cluster then this will be the *max* of the vCPU requirements of
 *                                          those allocations. Zero otherwise.
 * @param wantedExtraModelMemoryBytes       If there are jobs or trained models that have been started but cannot be allocated on the ML
 *                                          nodes currently within the cluster then this will be the *sum* of the ML native memory
 *                                          requirements of those jobs/trained models. The metric is in terms of ML native memory,
 *                                          not container memory.
 * @param wantedExtraProcessors             If there are trained model allocations that have been started but cannot be allocated on the
 *                                          ML nodes currently within the cluster then this will be the *sum* of the vCPU requirements
 *                                          of those allocations. Zero otherwise.
 * @param unwantedNodeMemoryBytesToRemove   The size of the ML node to be removed, in GB rounded to the nearest GB,
 *                                          or zero if no nodes could be removed.
 */

public record MlAutoscalingStats(
    int currentTotalNodes,
    long currentPerNodeMemoryBytes,
    long currentTotalModelMemoryBytes,
    int currentTotalProcessorsInUse,
    int wantedMinNodes,
    long wantedExtraPerNodeMemoryBytes,
    int wantedExtraPerNodeNodeProcessors,
    long wantedExtraModelMemoryBytes,
    int wantedExtraProcessors,
    long unwantedNodeMemoryBytesToRemove,
    long currentPerNodeMemoryOverheadBytes
) implements Writeable {

    public MlAutoscalingStats(StreamInput in) throws IOException {
        this(
            in.readVInt(), // currentTotalNodes
            in.readVLong(),  // currentPerNodeMemoryBytes
            in.readVLong(), // modelMemoryInBytes
            in.readVInt(), // currentTotalProcessorsInUse
            in.readVInt(), // wantedMinNodes
            in.readVLong(), // wantedExtraPerNodeMemoryBytes
            in.readVInt(), // wantedExtraPerNodeNodeProcessors
            in.readVLong(), // wantedExtraModelMemoryBytes
            in.readVInt(), // wantedExtraProcessors
            in.readVLong(), // unwantedNodeMemoryBytesToRemove
            in.readVLong() // currentPerNodeMemoryOverheadBytes
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(currentTotalNodes);
        out.writeVLong(currentPerNodeMemoryBytes);
        out.writeVLong(currentTotalModelMemoryBytes);
        out.writeVLong(currentTotalProcessorsInUse);
        out.writeVInt(wantedMinNodes);
        out.writeVLong(wantedExtraPerNodeMemoryBytes);
        out.writeVInt(wantedExtraPerNodeNodeProcessors);
        out.writeVLong(wantedExtraModelMemoryBytes);
        out.writeVInt(wantedExtraProcessors);
        out.writeVLong(unwantedNodeMemoryBytesToRemove);
        out.writeVLong(currentPerNodeMemoryOverheadBytes);
    }
}
