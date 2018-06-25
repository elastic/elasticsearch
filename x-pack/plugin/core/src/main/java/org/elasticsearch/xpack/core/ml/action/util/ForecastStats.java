/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.action.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A class to hold statistics about forecasts.
 */
public class ForecastStats implements ToXContentObject, Writeable {

    public static class Fields {
        public static final String total = "total";
        public static final String memory = "memory";
        public static final String runtime = "runtime";
        public static final String records = "records";
        public static final String byStatus = "status";
    }

    private long total;
    private final Map<String, Double> memoryStats;
    private final Map<String, Double> recordStats;
    private final Map<String, Double> runtimeStats;
    private final Map<String, Long> statusCounts;

    public ForecastStats() {
        this.total = 0;
        this.memoryStats = new HashMap<String, Double>();
        this.recordStats = new HashMap<String, Double>();
        this.runtimeStats = new HashMap<String, Double>();
        this.statusCounts = new HashMap<String, Long>();
    }
    
    public ForecastStats(long total, Map<String, Double> memoryStats, Map<String, Double> recordStats, Map<String, Double> runtimeStats,
            Map<String, Long> statusCounts) {
        this.total = total;
        this.memoryStats = Objects.requireNonNull(memoryStats);
        this.recordStats = Objects.requireNonNull(recordStats);
        this.runtimeStats = Objects.requireNonNull(runtimeStats);
        this.statusCounts = Objects.requireNonNull(statusCounts);
    }

    public ForecastStats(StreamInput in) throws IOException {
        this.total = in.readLong();
        this.memoryStats = in.readMap(StreamInput::readString, StreamInput::readDouble);
        this.recordStats = in.readMap(StreamInput::readString, StreamInput::readDouble);
        this.runtimeStats = in.readMap(StreamInput::readString, StreamInput::readDouble);
        this.statusCounts = in.readMap(StreamInput::readString, StreamInput::readLong);
    }
    
    public void combine(ForecastStats otherStats) {
        this.total += otherStats.total;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContentBody(builder, params);
        return builder.endObject();
    }

    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.total, total);
        if (total > 0) {
            builder.field(Fields.memory, memoryStats);
            builder.field(Fields.records, recordStats);
            builder.field(Fields.runtime, runtimeStats);
            builder.field(Fields.byStatus, statusCounts);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(total);
        out.writeMap(memoryStats, StreamOutput::writeString, StreamOutput::writeDouble);
        out.writeMap(recordStats, StreamOutput::writeString, StreamOutput::writeDouble);
        out.writeMap(runtimeStats, StreamOutput::writeString, StreamOutput::writeDouble);
        out.writeMap(statusCounts, StreamOutput::writeString, StreamOutput::writeLong);
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, memoryStats, recordStats, runtimeStats, statusCounts);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        ForecastStats other = (ForecastStats) obj;
        return Objects.equals(total, other.total) && Objects.equals(memoryStats, other.memoryStats)
                && Objects.equals(recordStats, other.recordStats) && Objects.equals(runtimeStats, other.runtimeStats)
                && Objects.equals(statusCounts, other.statusCounts);
    }
}
