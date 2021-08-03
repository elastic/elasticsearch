/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.stats;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A class to hold statistics about forecasts.
 */
public class ForecastStats implements ToXContentObject {

    public static final ParseField TOTAL = new ParseField("total");
    public static final ParseField FORECASTED_JOBS = new ParseField("forecasted_jobs");
    public static final ParseField MEMORY_BYTES = new ParseField("memory_bytes");
    public static final ParseField PROCESSING_TIME_MS = new ParseField("processing_time_ms");
    public static final ParseField RECORDS = new ParseField("records");
    public static final ParseField STATUS = new ParseField("status");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ForecastStats, Void> PARSER =
        new ConstructingObjectParser<>("forecast_stats",
            true,
            (a) -> {
                int i = 0;
                long total = (long)a[i++];
                SimpleStats memoryStats = (SimpleStats)a[i++];
                SimpleStats recordStats = (SimpleStats)a[i++];
                SimpleStats runtimeStats = (SimpleStats)a[i++];
                Map<String, Long> statusCounts = (Map<String, Long>)a[i];
                return new ForecastStats(total, memoryStats, recordStats, runtimeStats, statusCounts);
            });

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SimpleStats.PARSER, MEMORY_BYTES);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SimpleStats.PARSER, RECORDS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SimpleStats.PARSER, PROCESSING_TIME_MS);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            p -> {
                Map<String, Long> counts = new HashMap<>();
                p.map().forEach((key, value) -> counts.put(key, ((Number)value).longValue()));
                return counts;
            }, STATUS, ObjectParser.ValueType.OBJECT);
    }

    private final long total;
    private final long forecastedJobs;
    private SimpleStats memoryStats;
    private SimpleStats recordStats;
    private SimpleStats runtimeStats;
    private Map<String, Long> statusCounts;

    public ForecastStats(long total,
                         SimpleStats memoryStats,
                         SimpleStats recordStats,
                         SimpleStats runtimeStats,
                         Map<String, Long> statusCounts) {
        this.total = total;
        this.forecastedJobs = total > 0 ? 1 : 0;
        if (total > 0) {
            this.memoryStats = Objects.requireNonNull(memoryStats);
            this.recordStats = Objects.requireNonNull(recordStats);
            this.runtimeStats = Objects.requireNonNull(runtimeStats);
            this.statusCounts = Collections.unmodifiableMap(statusCounts);
        }
    }

    /**
     * The number of forecasts currently available for this model.
     */
    public long getTotal() {
        return total;
    }

    /**
     * The number of jobs that have at least one forecast.
     */
    public long getForecastedJobs() {
        return forecastedJobs;
    }

    /**
     * Statistics about the memory usage: minimum, maximum, average and total.
     */
    public SimpleStats getMemoryStats() {
        return memoryStats;
    }

    /**
     * Statistics about the number of forecast records: minimum, maximum, average and total.
     */
    public SimpleStats getRecordStats() {
        return recordStats;
    }

    /**
     * Statistics about the forecast runtime in milliseconds: minimum, maximum, average and total
     */
    public SimpleStats getRuntimeStats() {
        return runtimeStats;
    }

    /**
     * Counts per forecast status, for example: {"finished" : 2}.
     */
    public Map<String, Long> getStatusCounts() {
        return statusCounts;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOTAL.getPreferredName(), total);
        builder.field(FORECASTED_JOBS.getPreferredName(), forecastedJobs);

        if (total > 0) {
            builder.field(MEMORY_BYTES.getPreferredName(), memoryStats);
            builder.field(RECORDS.getPreferredName(), recordStats);
            builder.field(PROCESSING_TIME_MS.getPreferredName(), runtimeStats);
            builder.field(STATUS.getPreferredName(), statusCounts);
        }
        return builder.endObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, forecastedJobs, memoryStats, recordStats, runtimeStats, statusCounts);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ForecastStats other = (ForecastStats) obj;
        return Objects.equals(total, other.total) &&
            Objects.equals(forecastedJobs, other.forecastedJobs) &&
            Objects.equals(memoryStats, other.memoryStats) &&
            Objects.equals(recordStats, other.recordStats) &&
            Objects.equals(runtimeStats, other.runtimeStats) &&
            Objects.equals(statusCounts, other.statusCounts);
    }
}
