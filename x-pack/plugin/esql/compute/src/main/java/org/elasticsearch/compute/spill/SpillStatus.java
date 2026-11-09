/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The spill counters every spilling operator carries in its status: one serialization and one XContent shape, behind
 * one transport version. Presence is by value: an operator that spilled nothing writes no field, so existing profile
 * assertions keep passing; on the wire the record is written last in a status, after the operator's own version gates,
 * and read back as {@link #NONE} from a node that predates it.
 * <p>
 * The counters are defined once, here, and every producer counts the same way: the aggregation's slices and their
 * restores, the freeze's partition runs and their replays at every depth, the Top-N's value segments with their
 * compactions and gathers, the result collector's run. Bytes are the records' payload bytes, before the run's framing
 * and the store's chunking: a run's header, index and footer are not counted, and a rewrite counts again, so a
 * compaction's output and a replay's partition run add to the bytes written as any run does. A run is one run of any
 * kind, a segment or a partition run included, counted at its seal, except the result collector's, counted from its
 * creation since it is sealed after the driver's profile is taken. The time is the latency of the operator's spill
 * operations, from the issue of each to its landing: the driver's wait where the driver waited, which is every
 * creation, seal, restore, gather and replay read, and the aggregation's slice writes besides, which overlap with
 * its work; appends in flight are not timed.
 *
 * @param spilledBytes payload bytes written to the store by this operator, rewrites included
 * @param readBackBytes payload bytes read back from the store by this operator
 * @param spillRuns runs this operator wrote, segments and partition runs included
 * @param spillNanos the latency of this operator's spill operations, the driver's wait where it waited
 */
public record SpillStatus(long spilledBytes, long readBackBytes, int spillRuns, long spillNanos) implements Writeable, ToXContentFragment {

    public static final TransportVersion ESQL_SPILL_STATUS = TransportVersion.fromName("esql_spill_status");

    public static final SpillStatus NONE = new SpillStatus(0, 0, 0, 0);

    /**
     * The XContent fields, for a status's profile schema, in {@link #toXContent}'s order. The order is fixed, unlike
     * {@link Map#of}'s, so that the generated profile schema is the same from one run to the next.
     */
    public static final Map<String, Map<String, Object>> SCHEMA = schema();

    private static Map<String, Map<String, Object>> schema() {
        Map<String, Map<String, Object>> schema = new LinkedHashMap<>();
        schema.put("spilled_bytes", ProfileSchema.integer());
        schema.put("spilled", ProfileSchema.string());
        schema.put("read_back_bytes", ProfileSchema.integer());
        schema.put("read_back", ProfileSchema.string());
        schema.put("spill_runs", ProfileSchema.integer());
        schema.put("spill_nanos", ProfileSchema.integer());
        schema.put("spill_time", ProfileSchema.string());
        return Collections.unmodifiableMap(schema);
    }

    /**
     * Adds the fields to a status's schema, all optional since they are absent when nothing spilled.
     */
    public static <T> ProfileSchema.Builder<T> addTo(ProfileSchema.Builder<T> builder) {
        for (Map.Entry<String, Map<String, Object>> field : SCHEMA.entrySet()) {
            builder.optional(field.getKey(), field.getValue());
        }
        return builder;
    }

    /**
     * Reads what {@link #writeTo} wrote, or {@link #NONE} from a node that predates the record.
     */
    public static SpillStatus readFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(ESQL_SPILL_STATUS) == false) {
            return NONE;
        }
        return new SpillStatus(in.readVLong(), in.readVLong(), in.readVInt(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_SPILL_STATUS)) {
            out.writeVLong(spilledBytes);
            out.writeVLong(readBackBytes);
            out.writeVInt(spillRuns);
            out.writeVLong(spillNanos);
        }
    }

    /**
     * True when nothing spilled, in which case the record writes no field.
     */
    public boolean isNone() {
        return equals(NONE);
    }

    /**
     * The sum, for an operator that reports its replay operators' counters with its own.
     */
    public SpillStatus plus(SpillStatus other) {
        return new SpillStatus(
            spilledBytes + other.spilledBytes,
            readBackBytes + other.readBackBytes,
            spillRuns + other.spillRuns,
            spillNanos + other.spillNanos
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (isNone()) {
            return builder;
        }
        builder.field("spilled_bytes", spilledBytes);
        builder.field("spilled", ByteSizeValue.ofBytes(spilledBytes));
        builder.field("read_back_bytes", readBackBytes);
        builder.field("read_back", ByteSizeValue.ofBytes(readBackBytes));
        builder.field("spill_runs", spillRuns);
        builder.field("spill_nanos", spillNanos);
        if (builder.humanReadable()) {
            builder.field("spill_time", TimeValue.timeValueNanos(spillNanos));
        }
        return builder;
    }
}
