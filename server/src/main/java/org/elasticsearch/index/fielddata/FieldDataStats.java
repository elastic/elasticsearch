/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class FieldDataStats implements Writeable, ToXContentFragment {

    private static final String FIELDDATA = "fielddata";
    private static final String MEMORY_SIZE = "memory_size";
    private static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
    private static final String EVICTIONS = "evictions";
    private static final String FIELDS = "fields";
    private static final String GLOBAL_ORDINALS = "global_ordinals";
    private static final String MAX_SHARD_VALUE_COUNT = "max_shard_value_count";
    private static final String BUILD_TIME = "build_time";
    private long memorySize;
    private long evictions;
    @Nullable
    private FieldMemoryStats fields;
    private GlobalOrdinalsStats globalOrdinalsStats;

    public FieldDataStats() {
        this.globalOrdinalsStats = new GlobalOrdinalsStats(0, new HashMap<>());
    }

    public FieldDataStats(StreamInput in) throws IOException {
        memorySize = in.readVLong();
        evictions = in.readVLong();
        fields = in.readOptionalWriteable(FieldMemoryStats::new);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            globalOrdinalsStats = new GlobalOrdinalsStats(
                in.readVLong(),
                in.readBoolean()
                    ? in.readMap(
                        StreamInput::readString,
                        in1 -> new GlobalOrdinalsStats.GlobalOrdinalFieldStats(in1.readVLong(), in1.readVLong())
                    )
                    : null
            );
        } else {
            globalOrdinalsStats = new GlobalOrdinalsStats(0, new HashMap<>());
        }
    }

    public FieldDataStats(long memorySize, long evictions, @Nullable FieldMemoryStats fields, GlobalOrdinalsStats globalOrdinalsStats) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.fields = fields;
        this.globalOrdinalsStats = globalOrdinalsStats;
    }

    public void add(FieldDataStats stats) {
        if (stats == null) {
            return;
        }
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
        if (stats.fields != null) {
            if (fields == null) {
                fields = stats.fields.copy();
            } else {
                fields.add(stats.fields);
            }
        }
        if (globalOrdinalsStats != null && stats.globalOrdinalsStats != null) {
            this.globalOrdinalsStats.add(stats.globalOrdinalsStats);
        } else if (stats.globalOrdinalsStats != null) {
            this.globalOrdinalsStats = stats.globalOrdinalsStats;
        }
    }

    public long getMemorySizeInBytes() {
        return this.memorySize;
    }

    public ByteSizeValue getMemorySize() {
        return ByteSizeValue.ofBytes(memorySize);
    }

    public long getEvictions() {
        return this.evictions;
    }

    @Nullable
    public FieldMemoryStats getFields() {
        return fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySize);
        out.writeVLong(evictions);
        out.writeOptionalWriteable(fields);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeVLong(globalOrdinalsStats.buildTimeMillis);
            if (globalOrdinalsStats.fieldGlobalOrdinalsStats != null) {
                out.writeBoolean(true);
                out.writeMap(globalOrdinalsStats.fieldGlobalOrdinalsStats, StreamOutput::writeString, (out1, value) -> {
                    out1.writeVLong(value.totalBuildingTime);
                    out1.writeVLong(value.valueCount);
                });
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FIELDDATA);
        builder.humanReadableField(MEMORY_SIZE_IN_BYTES, MEMORY_SIZE, getMemorySize());
        builder.field(EVICTIONS, getEvictions());
        if (fields != null) {
            fields.toXContent(builder, FIELDS, MEMORY_SIZE_IN_BYTES, MEMORY_SIZE);
        }
        if (globalOrdinalsStats != null) {
            builder.startObject(GLOBAL_ORDINALS);
            builder.humanReadableField(BUILD_TIME + "_in_millis", BUILD_TIME, new TimeValue(globalOrdinalsStats.buildTimeMillis));
            if (globalOrdinalsStats.fieldGlobalOrdinalsStats != null) {
                builder.startObject(FIELDS);
                for (var entry : globalOrdinalsStats.fieldGlobalOrdinalsStats.entrySet()) {
                    builder.startObject(entry.getKey());
                    builder.humanReadableField(BUILD_TIME + "in_millis", BUILD_TIME, new TimeValue(entry.getValue().totalBuildingTime));
                    builder.field(MAX_SHARD_VALUE_COUNT, entry.getValue().valueCount);
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldDataStats that = (FieldDataStats) o;
        return memorySize == that.memorySize
            && evictions == that.evictions
            && Objects.equals(fields, that.fields)
            && Objects.equals(globalOrdinalsStats, that.globalOrdinalsStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memorySize, evictions, fields, globalOrdinalsStats);
    }

    public static class GlobalOrdinalsStats {

        private long buildTimeMillis;
        private final Map<String, GlobalOrdinalFieldStats> fieldGlobalOrdinalsStats;

        public GlobalOrdinalsStats(long buildTimeMillis, Map<String, GlobalOrdinalFieldStats> fieldGlobalOrdinalsStats) {
            this.buildTimeMillis = buildTimeMillis;
            this.fieldGlobalOrdinalsStats = fieldGlobalOrdinalsStats;
        }

        public void add(GlobalOrdinalsStats other) {
            buildTimeMillis += other.buildTimeMillis;
            if (other.fieldGlobalOrdinalsStats != null) {
                for (var entry : other.fieldGlobalOrdinalsStats.entrySet()) {
                    fieldGlobalOrdinalsStats.merge(
                        entry.getKey(),
                        entry.getValue(),
                        (value1, value2) -> new GlobalOrdinalFieldStats(
                            value1.totalBuildingTime + value2.totalBuildingTime,
                            Math.max(value1.valueCount, value2.valueCount)
                        )
                    );
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GlobalOrdinalsStats that = (GlobalOrdinalsStats) o;
            return buildTimeMillis == that.buildTimeMillis && Objects.equals(fieldGlobalOrdinalsStats, that.fieldGlobalOrdinalsStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(buildTimeMillis, fieldGlobalOrdinalsStats);
        }

        public static class GlobalOrdinalFieldStats {

            long totalBuildingTime;
            long valueCount;

            public GlobalOrdinalFieldStats(long totalBuildingTime, long valueCount) {
                this.totalBuildingTime = totalBuildingTime;
                this.valueCount = valueCount;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                GlobalOrdinalFieldStats that = (GlobalOrdinalFieldStats) o;
                return totalBuildingTime == that.totalBuildingTime && valueCount == that.valueCount;
            }

            @Override
            public int hashCode() {
                return Objects.hash(totalBuildingTime, valueCount);
            }
        }

    }
}
