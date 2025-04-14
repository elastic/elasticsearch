/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.codec.vectors.reflect.OffHeapByteSizeUtils;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.unit.ByteSizeValue.ofBytes;

/**
 * Statistics about indexed dense vector
 */
public class DenseVectorStats implements Writeable, ToXContentFragment {
    private long valueCount = 0;

    /** Per-field off-heap desired memory byte size, categorized by file extension. */
    Map<String, Map<String, Long>> offHeapStats;

    public DenseVectorStats() {}

    public DenseVectorStats(long count) {
        this(count, null);
    }

    public DenseVectorStats(long count, Map<String, Map<String, Long>> offHeapStats) {
        this.valueCount = count;
        this.offHeapStats = offHeapStats;
    }

    public DenseVectorStats(StreamInput in) throws IOException {
        this.valueCount = in.readVLong();
        if (in.getTransportVersion().onOrAfter(TransportVersions.DENSE_VECTOR_OFF_HEAP_STATS)) {
            this.offHeapStats = readOptionalOffHeapStats(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(valueCount);
        if (out.getTransportVersion().onOrAfter(TransportVersions.DENSE_VECTOR_OFF_HEAP_STATS)) {
            writeOptionalOffHeapStats(out);
        }
    }

    private Map<String, Map<String, Long>> readOptionalOffHeapStats(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return in.readMap(v -> in.readMap(StreamInput::readLong));
        } else {
            return null;
        }
    }

    private void writeOptionalOffHeapStats(StreamOutput out) throws IOException {
        if (offHeapStats != null) {
            out.writeBoolean(true);
            out.writeMap(offHeapStats, StreamOutput::writeString, DenseVectorStats::writeFieldStatsMap);
        } else {
            out.writeBoolean(false);
        }
    }

    static void writeFieldStatsMap(StreamOutput out, Map<String, Long> map) throws IOException {
        out.writeMap(map, StreamOutput::writeString, StreamOutput::writeLong);
    }

    public void add(DenseVectorStats other) {
        if (other == null) {
            return;
        }
        this.valueCount += other.valueCount;
        if (other.offHeapStats != null) {
            if (this.offHeapStats == null) {
                this.offHeapStats = other.offHeapStats;
            } else {
                this.offHeapStats = Stream.of(this.offHeapStats, other.offHeapStats)
                    .flatMap(map -> map.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, OffHeapByteSizeUtils::mergeOffHeapByteSizeMaps));
            }
        }
    }

    /** Returns the total number of dense vectors added in the index. */
    public long getValueCount() {
        return valueCount;
    }

    /** Returns a map of per-field off-heap stats. */
    public Map<String, Map<String, Long>> offHeapStats() {
        return offHeapStats;
    }

    private Map<String, Long> getTotalsByCategory() {
        if (offHeapStats == null) {
            return Map.of("veb", 0L, "vec", 0L, "veq", 0L, "vex", 0L);
        } else {
            return offHeapStats.entrySet()
                .stream()
                .flatMap(map -> map.getValue().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.NAME);
        builder.field(Fields.VALUE_COUNT, valueCount);
        if (params.paramAsBoolean(INCLUDE_OFF_HEAP, false)) {
            toXContentWithFields(builder, params);
        }
        builder.endObject();
        return builder;
    }

    private void toXContentWithFields(XContentBuilder builder, Params params) throws IOException {
        var totals = getTotalsByCategory();
        builder.startObject("off_heap");
        builder.humanReadableField("total_size_bytes", "total_size", ofBytes(totals.values().stream().mapToLong(Long::longValue).sum()));
        builder.humanReadableField("total_veb_size_bytes", "total_veb_size", ofBytes(totals.getOrDefault("veb", 0L)));
        builder.humanReadableField("total_vec_size_bytes", "total_vec_size", ofBytes(totals.getOrDefault("vec", 0L)));
        builder.humanReadableField("total_veq_size_bytes", "total_veq_size", ofBytes(totals.getOrDefault("veq", 0L)));
        builder.humanReadableField("total_vex_size_bytes", "total_vex_size", ofBytes(totals.getOrDefault("vex", 0L)));
        if (params.paramAsBoolean(INCLUDE_PER_FIELD_STATS, false) && offHeapStats != null && offHeapStats.size() > 0) {
            toXContentWithPerFieldStats(builder);
        }
        builder.endObject();
    }

    private void toXContentWithPerFieldStats(XContentBuilder builder) throws IOException {
        builder.startObject(Fields.FIELDS);
        for (var key : offHeapStats.keySet().stream().sorted().toList()) {
            Map<String, Long> entry = offHeapStats.get(key);
            if (entry.isEmpty() == false) {
                builder.startObject(key);
                for (var eKey : entry.keySet().stream().sorted().toList()) {
                    long value = entry.get(eKey);
                    assert value > 0L;
                    builder.humanReadableField(eKey + "_size_bytes", eKey + "_size", ofBytes(value));
                }
                builder.endObject();
            }
        }
        builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DenseVectorStats that = (DenseVectorStats) o;
        return valueCount == that.valueCount && Objects.equals(offHeapStats, that.offHeapStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueCount, offHeapStats);
    }

    public static final String INCLUDE_OFF_HEAP = "include_off_heap";
    public static final String INCLUDE_PER_FIELD_STATS = "include_per_field_stats";

    static final class Fields {
        static final String NAME = "dense_vector";
        static final String VALUE_COUNT = "value_count";
        static final String FIELDS = "fielddata";
    }
}
