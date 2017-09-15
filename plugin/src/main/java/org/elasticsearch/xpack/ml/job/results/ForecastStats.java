/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.config.Job;

import java.io.IOException;
import java.util.Objects;

/**
 * Model ForecastStats POJO.
 * 
 * Note forecast stats are sent from the autodetect process but do not get
 * indexed.
 */
public class ForecastStats implements ToXContentObject, Writeable {
    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "model_forecast_stats";
    public static final ParseField RESULTS_FIELD = new ParseField(RESULT_TYPE_VALUE);

    public static final ParseField FORECAST_ID = new ParseField("forecast_id");
    public static final ParseField RECORD_COUNT = new ParseField("forecast_record_count");

    public static final ConstructingObjectParser<ForecastStats, Void> PARSER =
            new ConstructingObjectParser<>(RESULT_TYPE_VALUE, a -> new ForecastStats((String) a[0], (long) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), FORECAST_ID);

        PARSER.declareString((modelForecastStats, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareLong(ForecastStats::setRecordCount, RECORD_COUNT);
    }

    private final String jobId;
    private final long forecastId;
    private long recordCount;

    public ForecastStats(String jobId, long forecastId) {
        this.jobId = jobId;
        this.forecastId = forecastId;
    }

    public ForecastStats(StreamInput in) throws IOException {
        jobId = in.readString();
        forecastId = in.readLong();
        recordCount = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeLong(forecastId);
        out.writeLong(recordCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(FORECAST_ID.getPreferredName(), forecastId);
        builder.field(RECORD_COUNT.getPreferredName(), recordCount);
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public String getId() {
        return jobId + "_model_forecast_stats";
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public double getRecordCount() {
        return recordCount;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof ForecastStats == false) {
            return false;
        }
        ForecastStats that = (ForecastStats) other;
        return Objects.equals(this.jobId, that.jobId) &&
                this.forecastId == that.forecastId && 
                this.recordCount == that.recordCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, forecastId, recordCount);
    }
}
