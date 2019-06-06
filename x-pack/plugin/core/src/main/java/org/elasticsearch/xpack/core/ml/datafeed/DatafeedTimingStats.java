/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DatafeedTimingStats implements ToXContentObject, Writeable {

    public static final ParseField JOB_ID = new ParseField("job_id");
    public static final ParseField TOTAL_SEARCH_TIME_MS = new ParseField("total_search_time_ms");

    public static final ParseField TYPE = new ParseField("datafeed_timing_stats");

    public static final ConstructingObjectParser<DatafeedTimingStats, Void> PARSER = createParser();

    private static ConstructingObjectParser<DatafeedTimingStats, Void> createParser() {
        ConstructingObjectParser<DatafeedTimingStats, Void> parser =
            new ConstructingObjectParser<>(
                "datafeed_timing_stats", true, args -> new DatafeedTimingStats((String) args[0], (double) args[1]));
        parser.declareString(constructorArg(), JOB_ID);
        parser.declareDouble(optionalConstructorArg(), TOTAL_SEARCH_TIME_MS);
        return parser;
    }

    public static String documentId(String jobId) {
        return jobId + "_datafeed_timing_stats";
    }

    private final String jobId;
    private double totalSearchTimeMs;

    public DatafeedTimingStats(String jobId, double totalSearchTimeMs) {
        this.jobId = Objects.requireNonNull(jobId);
        this.totalSearchTimeMs = totalSearchTimeMs;
    }

    public DatafeedTimingStats(String jobId) {
        this(jobId, 0);
    }

    public DatafeedTimingStats(StreamInput in) throws IOException {
        jobId = in.readString();
        totalSearchTimeMs = in.readOptionalDouble();
    }

    public DatafeedTimingStats(DatafeedTimingStats other) {
        this(other.jobId, other.totalSearchTimeMs);
    }

    public String getJobId() {
        return jobId;
    }

    public double getTotalSearchTimeMs() {
        return totalSearchTimeMs;
    }

    public void incrementTotalSearchTimeMs(double totalSearchTimeMs) {
        this.totalSearchTimeMs += totalSearchTimeMs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeOptionalDouble(totalSearchTimeMs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(JOB_ID.getPreferredName(), jobId);
        builder.field(TOTAL_SEARCH_TIME_MS.getPreferredName(), totalSearchTimeMs);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DatafeedTimingStats other = (DatafeedTimingStats) obj;
        return Objects.equals(this.jobId, other.jobId)
            && Objects.equals(this.totalSearchTimeMs, other.totalSearchTimeMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, totalSearchTimeMs);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Returns true if given stats objects differ from each other by more than 10% for at least one of the statistics.
     */
    public static boolean differSignificantly(DatafeedTimingStats stats1, DatafeedTimingStats stats2) {
        return differSignificantly(stats1.totalSearchTimeMs, stats2.totalSearchTimeMs);
    }

    /**
     * Returns {@code true} if one of the ratios { value1 / value2, value2 / value1 } is smaller than MIN_VALID_RATIO.
     * This can be interpreted as values { value1, value2 } differing significantly from each other.
     */
    private static boolean differSignificantly(double value1, double value2) {
        return (value2 / value1 < MIN_VALID_RATIO) || (value1 / value2 < MIN_VALID_RATIO);
    }

    /**
     * Minimum ratio of values that is interpreted as values being similar.
     * If the values ratio is less than MIN_VALID_RATIO, the values are interpreted as significantly different.
     */
    private static final double MIN_VALID_RATIO = 0.9;
}
