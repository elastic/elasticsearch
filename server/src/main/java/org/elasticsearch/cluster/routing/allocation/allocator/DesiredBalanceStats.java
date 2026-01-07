/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record DesiredBalanceStats(
    long lastConvergedIndex,
    boolean computationActive,
    long computationSubmitted,
    long computationExecuted,
    long computationConverged,
    long computationIterations,
    long computedShardMovements,
    long cumulativeComputationTime,
    long cumulativeReconciliationTime,
    long unassignedShards,
    long totalAllocations,
    long undesiredAllocations
) implements Writeable, ToXContentObject {

    public DesiredBalanceStats {
        if (lastConvergedIndex < 0) {
            assert false : lastConvergedIndex;
            throw new IllegalStateException("lastConvergedIndex must be nonnegative, but got [" + lastConvergedIndex + ']');
        }
    }

    public static DesiredBalanceStats readFrom(StreamInput in) throws IOException {
        return new DesiredBalanceStats(
            in.readVLong(),
            in.readBoolean(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(lastConvergedIndex);
        out.writeBoolean(computationActive);
        out.writeVLong(computationSubmitted);
        out.writeVLong(computationExecuted);
        out.writeVLong(computationConverged);
        out.writeVLong(computationIterations);
        out.writeVLong(computedShardMovements);
        out.writeVLong(cumulativeComputationTime);
        out.writeVLong(cumulativeReconciliationTime);
        out.writeVLong(unassignedShards);
        out.writeVLong(totalAllocations);
        out.writeVLong(undesiredAllocations);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("computation_converged_index", lastConvergedIndex);
        builder.field("computation_active", computationActive);
        builder.field("computation_submitted", computationSubmitted);
        builder.field("computation_executed", computationExecuted);
        builder.field("computation_converged", computationConverged);
        builder.field("computation_iterations", computationIterations);
        builder.field("computed_shard_movements", computedShardMovements);
        builder.humanReadableField("computation_time_in_millis", "computation_time", new TimeValue(cumulativeComputationTime));
        builder.humanReadableField("reconciliation_time_in_millis", "reconciliation_time", new TimeValue(cumulativeReconciliationTime));
        builder.field("unassigned_shards", unassignedShards);
        builder.field("total_allocations", totalAllocations);
        builder.field("undesired_allocations", undesiredAllocations);
        builder.field("undesired_allocations_ratio", undesiredAllocationsRatio());
        builder.endObject();
        return builder;
    }

    public double undesiredAllocationsRatio() {
        if (unassignedShards == -1 || totalAllocations == -1 || undesiredAllocations == -1) {
            return -1.0;
        } else if (totalAllocations == 0) {
            return 0.0;
        } else {
            return (double) undesiredAllocations / totalAllocations;
        }
    }
}
