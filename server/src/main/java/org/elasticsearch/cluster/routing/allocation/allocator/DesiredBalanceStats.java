/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.TransportVersion;
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
    long cumulativeReconciliationTime
) implements Writeable, ToXContentObject {

    private static final TransportVersion COMPUTED_SHARD_MOVEMENTS_VERSION = TransportVersion.V_8_8_0;

    public static DesiredBalanceStats readFrom(StreamInput in) throws IOException {
        return new DesiredBalanceStats(
            in.readVLong(),
            in.readBoolean(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.getTransportVersion().onOrAfter(COMPUTED_SHARD_MOVEMENTS_VERSION) ? in.readVLong() : -1,
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
        if (out.getTransportVersion().onOrAfter(COMPUTED_SHARD_MOVEMENTS_VERSION)) {
            out.writeVLong(computedShardMovements);
        }
        out.writeVLong(cumulativeComputationTime);
        out.writeVLong(cumulativeReconciliationTime);
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
        builder.endObject();
        return builder;
    }
}
