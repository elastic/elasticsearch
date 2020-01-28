/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.stats;

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
        public static final String TOTAL = "total";
        public static final String FORECASTED_JOBS = "forecasted_jobs";
        public static final String MEMORY = "memory_bytes";
        public static final String RUNTIME = "processing_time_ms";
        public static final String RECORDS = "records";
        public static final String STATUSES = "status";
    }

    private long total;
    private long forecastedJobs;
    private StatsAccumulator memoryStats;
    private StatsAccumulator recordStats;
    private StatsAccumulator runtimeStats;
    private CountAccumulator statusCounts;

    public ForecastStats() {
        this.total = 0;
        this.forecastedJobs = 0;
        this.memoryStats = new StatsAccumulator();
        this.recordStats = new StatsAccumulator();
        this.runtimeStats = new StatsAccumulator();
        this.statusCounts = new CountAccumulator();
    }

    /*
     * Construct ForecastStats for 1 job. Additional statistics can be added by merging other ForecastStats into it.
     */
    public ForecastStats(long total, StatsAccumulator memoryStats, StatsAccumulator recordStats, StatsAccumulator runtimeStats,
            CountAccumulator statusCounts) {
        this.total = total;
        this.forecastedJobs = total > 0 ? 1 : 0;
        this.memoryStats = Objects.requireNonNull(memoryStats);
        this.recordStats = Objects.requireNonNull(recordStats);
        this.runtimeStats = Objects.requireNonNull(runtimeStats);
        this.statusCounts = Objects.requireNonNull(statusCounts);
    }

    public ForecastStats(StreamInput in) throws IOException {
        this.total = in.readLong();
        this.forecastedJobs = in.readLong();
        this.memoryStats = new StatsAccumulator(in);
        this.recordStats = new StatsAccumulator(in);
        this.runtimeStats = new StatsAccumulator(in);
        this.statusCounts = new CountAccumulator(in);
    }

    public ForecastStats merge(ForecastStats other) {
        if (other == null) {
            return this;
        }
        total += other.total;
        forecastedJobs += other.forecastedJobs;
        memoryStats.merge(other.memoryStats);
        recordStats.merge(other.recordStats);
        runtimeStats.merge(other.runtimeStats);
        statusCounts.merge(other.statusCounts);

        return this;
    }

    public long getTotal() {
        return total;
    }

    public long getForecastedJobs() {
        return forecastedJobs;
    }

    public StatsAccumulator getMemoryStats() {
        return memoryStats;
    }

    public StatsAccumulator getRecordStats() {
        return recordStats;
    }

    public StatsAccumulator getRuntimeStats() {
        return runtimeStats;
    }

    public CountAccumulator getStatusCounts() {
        return statusCounts;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContentBody(builder, params);
        return builder.endObject();
    }

    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.FORECASTED_JOBS, forecastedJobs);

        if (total > 0) {
            builder.field(Fields.MEMORY, memoryStats.asMap());
            builder.field(Fields.RECORDS, recordStats.asMap());
            builder.field(Fields.RUNTIME, runtimeStats.asMap());
            builder.field(Fields.STATUSES, statusCounts.asMap());
        }

        return builder;
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(Fields.TOTAL, total);
        map.put(Fields.FORECASTED_JOBS, forecastedJobs);

        if (total > 0) {
            map.put(Fields.MEMORY, memoryStats.asMap());
            map.put(Fields.RECORDS, recordStats.asMap());
            map.put(Fields.RUNTIME, runtimeStats.asMap());
            map.put(Fields.STATUSES, statusCounts.asMap());
        }

        return map;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(total);
        out.writeLong(forecastedJobs);
        memoryStats.writeTo(out);
        recordStats.writeTo(out);
        runtimeStats.writeTo(out);
        statusCounts.writeTo(out);
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, forecastedJobs, memoryStats, recordStats, runtimeStats, statusCounts);
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
        return Objects.equals(total, other.total) && Objects.equals(forecastedJobs, other.forecastedJobs)
                && Objects.equals(memoryStats, other.memoryStats) && Objects.equals(recordStats, other.recordStats)
                && Objects.equals(runtimeStats, other.runtimeStats) && Objects.equals(statusCounts, other.statusCounts);
    }
}
