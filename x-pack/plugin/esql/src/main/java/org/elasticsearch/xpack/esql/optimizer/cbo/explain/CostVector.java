/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * The cost model's estimate for a plan or a part of it, in the units the optimizer ranks plans by.
 * Every cost in the report uses this shape and always renders all of its fields, so readers can compare costs field by field.
 * Peak memory does not contribute to {@link #score()}: it drives fit checks and tie-breaks only.
 */
public record CostVector(
    double latencyNanos,
    double workUnits,
    double networkInBytes,
    double coordinatorInBytes,
    double peakNodeInBytes,
    double peakLaneInBytes
) implements Writeable, ToXContentFragment {

    /** Weight of {@link #workUnits()} in {@link #score()}, relative to latency in seconds. */
    public static final double WORK_WEIGHT = 1e-9;

    public static final CostVector ZERO = new CostVector(0, 0, 0, 0, 0, 0);

    public CostVector(StreamInput in) throws IOException {
        this(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(latencyNanos);
        out.writeDouble(workUnits);
        out.writeDouble(networkInBytes);
        out.writeDouble(coordinatorInBytes);
        out.writeDouble(peakNodeInBytes);
        out.writeDouble(peakLaneInBytes);
    }

    /** The ranking value of this cost; lower is better, and scores are only comparable within one optimizer run. */
    public double score() {
        return latencyNanos / 1e9 + WORK_WEIGHT * workUnits;
    }

    /** Combines two costs: latency, work, and transfer volumes add up, while peak memory takes the maximum. */
    public CostVector plus(CostVector o) {
        return new CostVector(
            latencyNanos + o.latencyNanos,
            workUnits + o.workUnits,
            networkInBytes + o.networkInBytes,
            coordinatorInBytes + o.coordinatorInBytes,
            Math.max(peakNodeInBytes, o.peakNodeInBytes),
            Math.max(peakLaneInBytes, o.peakLaneInBytes)
        );
    }

    /** Returns whether every field and the score are finite; a non-finite cost renders as {@code null} fields. */
    public boolean isFinite() {
        return Double.isFinite(latencyNanos)
            && Double.isFinite(workUnits)
            && Double.isFinite(networkInBytes)
            && Double.isFinite(coordinatorInBytes)
            && Double.isFinite(peakNodeInBytes)
            && Double.isFinite(peakLaneInBytes)
            && Double.isFinite(score());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        ExplainXContent.doubleField(b, "score", score());
        ExplainXContent.roundedField(b, "latency_nanos", latencyNanos);
        if (b.humanReadable() && hasReadableForm(latencyNanos)) {
            b.field("latency", TimeValue.timeValueNanos(Math.round(latencyNanos)).toString());
        }
        ExplainXContent.roundedField(b, "work_units", workUnits);
        bytesField(b, "network_in_bytes", "network", networkInBytes);
        bytesField(b, "coordinator_in_bytes", "coordinator", coordinatorInBytes);
        bytesField(b, "peak_node_in_bytes", "peak_node", peakNodeInBytes);
        bytesField(b, "peak_lane_in_bytes", "peak_lane", peakLaneInBytes);
        return b;
    }

    /** Writes this cost as an object under {@code name} in the enclosing object. */
    public void toXContentObject(XContentBuilder b, String name) throws IOException {
        b.startObject(name);
        toXContent(b, ToXContent.EMPTY_PARAMS);
        b.endObject();
    }

    private static void bytesField(XContentBuilder b, String rawName, String readableName, double v) throws IOException {
        ExplainXContent.roundedField(b, rawName, v);
        if (b.humanReadable() && hasReadableForm(v)) {
            b.field(readableName, ByteSizeValue.ofBytes(Math.round(v)).toString());
        }
    }

    /** {@link TimeValue} and {@link ByteSizeValue} reject negative amounts, so only finite non-negative values get a readable form. */
    private static boolean hasReadableForm(double v) {
        return Double.isFinite(v) && v >= 0;
    }
}
