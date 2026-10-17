/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Per-node counters of where captured searches ended up, since the node started.
 *
 * @param untrackedArrivals arrivals not counted because the tracker was full
 * @param starved           arrivals that could not be picked because the picks-per-hour limit was reached
 * @param groundTruthCreditMillis exact-search time this node may still spend, negative if in debt
 */
public record QuerySamplingStats(
    long knnSearches,
    long captured,
    long dropped,
    long distinctQueries,
    long untrackedArrivals,
    long picked,
    long written,
    long writeFailures,
    long writeDropped,
    long weightsRefreshed,
    long expired,
    long groundTruthComputed,
    long groundTruthFailed,
    long starved,
    double effectiveCaptureRate,
    double effectiveAcceptanceScale,
    double groundTruthCreditMillis
) implements Writeable, ToXContentFragment {

    public QuerySamplingStats(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(knnSearches);
        out.writeVLong(captured);
        out.writeVLong(dropped);
        out.writeVLong(distinctQueries);
        out.writeVLong(untrackedArrivals);
        out.writeVLong(picked);
        out.writeVLong(written);
        out.writeVLong(writeFailures);
        out.writeVLong(writeDropped);
        out.writeVLong(weightsRefreshed);
        out.writeVLong(expired);
        out.writeVLong(groundTruthComputed);
        out.writeVLong(groundTruthFailed);
        out.writeVLong(starved);
        out.writeDouble(effectiveCaptureRate);
        out.writeDouble(effectiveAcceptanceScale);
        out.writeDouble(groundTruthCreditMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("knn_searches", knnSearches);
        builder.field("captured", captured);
        builder.field("dropped", dropped);
        builder.field("distinct_queries", distinctQueries);
        builder.field("untracked_arrivals", untrackedArrivals);
        builder.field("picked", picked);
        builder.field("written", written);
        builder.field("write_failures", writeFailures);
        builder.field("write_dropped", writeDropped);
        builder.field("weights_refreshed", weightsRefreshed);
        builder.field("expired", expired);
        builder.field("ground_truth_computed", groundTruthComputed);
        builder.field("ground_truth_failed", groundTruthFailed);
        builder.field("starved", starved);
        builder.field("effective_capture_rate", effectiveCaptureRate);
        builder.field("effective_acceptance_scale", effectiveAcceptanceScale);
        builder.field("ground_truth_credit_millis", groundTruthCreditMillis);
        return builder;
    }
}
