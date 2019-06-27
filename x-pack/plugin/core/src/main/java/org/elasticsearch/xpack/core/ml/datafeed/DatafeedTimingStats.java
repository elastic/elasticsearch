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
        totalSearchTimeMs = in.readDouble();
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
        out.writeDouble(totalSearchTimeMs);
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
}
