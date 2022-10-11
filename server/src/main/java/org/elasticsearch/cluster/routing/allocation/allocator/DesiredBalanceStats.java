/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record DesiredBalanceStats(
    long lastConvergedIndex,
    boolean computationActive,
    long computationSubmitted,
    long computationExecuted,
    long computationConverged,
    long cumulativeComputationTime,
    long cumulativeReconciliationTime
) implements Writeable, ToXContentFragment {

    public static DesiredBalanceStats readFrom(StreamInput in) throws IOException {
        // TODO
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field("computation.active", computationActive);
        builder.field("computation.submitted", computationSubmitted);
        builder.field("computation.executed", computationExecuted);
        builder.field("computation.converged", computationConverged);
        builder.field("computation.converged.index", lastConvergedIndex);
        builder.humanReadableField("computation.time_in_millis", "computation.time", cumulativeComputationTime);
        builder.humanReadableField("reconciliation.time_in_millis", "reconciliation.time", cumulativeReconciliationTime);

        return builder;
    }
}
