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
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xpack.ml.job.config.Job;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

/**
 * Model ForecastRequestStats POJO.
 *
 * This information is produced by the autodetect process and contains
 * information about errors, progress and counters. There is exactly 1 document
 * per forecast request, getting updated while the request is processed.
 */
public class ForecastRequestStats implements ToXContentObject, Writeable {
    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "model_forecast_request_stats";

    public static final ParseField RESULTS_FIELD = new ParseField(RESULT_TYPE_VALUE);
    public static final ParseField FORECAST_ID = new ParseField("forecast_id");
    public static final ParseField START_TIME = new ParseField("forecast_start_timestamp");
    public static final ParseField END_TIME = new ParseField("forecast_end_timestamp");
    public static final ParseField MESSAGE = new ParseField("forecast_message");
    public static final ParseField PROCESSING_TIME_MS = new ParseField("processing_time_ms");
    public static final ParseField PROGRESS = new ParseField("forecast_progress");
    public static final ParseField PROCESSED_RECORD_COUNT = new ParseField("processed_record_count");
    public static final ParseField STATUS = new ParseField("forecast_status");

    public static final ConstructingObjectParser<ForecastRequestStats, Void> PARSER =
            new ConstructingObjectParser<>(RESULT_TYPE_VALUE, a -> new ForecastRequestStats((String) a[0], (long) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), FORECAST_ID);

        PARSER.declareString((modelForecastRequestStats, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareLong(ForecastRequestStats::setRecordCount, PROCESSED_RECORD_COUNT);
        PARSER.declareString(ForecastRequestStats::setMessage, MESSAGE);
        PARSER.declareField(ForecastRequestStats::setStartTimeStamp,
                p -> Instant.ofEpochMilli(p.longValue()), START_TIME, ValueType.LONG);
        PARSER.declareField(ForecastRequestStats::setEndTimeStamp,
                p -> Instant.ofEpochMilli(p.longValue()), END_TIME, ValueType.LONG);
        PARSER.declareDouble(ForecastRequestStats::setProgress, PROGRESS);
        PARSER.declareLong(ForecastRequestStats::setProcessingTime, PROCESSING_TIME_MS);
        PARSER.declareField(ForecastRequestStats::setStatus, p -> ForecastRequestStatus.fromString(p.text()), STATUS, ValueType.STRING);
    }

    public enum ForecastRequestStatus implements Writeable {
        OK, FAILED, STOPPED, STARTED, FINISHED, SCHEDULED;

        public static ForecastRequestStatus fromString(String statusName) {
            return valueOf(statusName.trim().toUpperCase(Locale.ROOT));
        }

        public static ForecastRequestStatus readFromStream(StreamInput in) throws IOException {
            return in.readEnum(ForecastRequestStatus.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final String jobId;
    private final long forecastId;
    private long recordCount;
    private String message;
    private Instant dateStarted = Instant.EPOCH;
    private Instant dateEnded = Instant.EPOCH;
    private double progress;
    private long processingTime;
    private ForecastRequestStatus status = ForecastRequestStatus.OK;

    public ForecastRequestStats(String jobId, long forecastId) {
        this.jobId = jobId;
        this.forecastId = forecastId;
    }

    public ForecastRequestStats(StreamInput in) throws IOException {
        jobId = in.readString();
        forecastId = in.readLong();
        recordCount = in.readLong();
        message = in.readOptionalString();
        dateStarted = Instant.ofEpochMilli(in.readVLong());
        dateEnded = Instant.ofEpochMilli(in.readVLong());
        progress = in.readDouble();
        processingTime = in.readLong();
        status = ForecastRequestStatus.readFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeLong(forecastId);
        out.writeLong(recordCount);
        out.writeOptionalString(message);
        out.writeVLong(dateStarted.toEpochMilli());
        out.writeVLong(dateEnded.toEpochMilli());
        out.writeDouble(progress);
        out.writeLong(processingTime);
        status.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(FORECAST_ID.getPreferredName(), forecastId);
        builder.field(PROCESSED_RECORD_COUNT.getPreferredName(), recordCount);
        if (message != null) {
            builder.field(MESSAGE.getPreferredName(), message);
        }
        if (dateStarted.equals(Instant.EPOCH) == false) {
            builder.field(START_TIME.getPreferredName(), dateStarted.toEpochMilli());
        }
        if (dateEnded.equals(Instant.EPOCH) == false) {
            builder.field(END_TIME.getPreferredName(), dateEnded.toEpochMilli());
        }
        builder.field(PROGRESS.getPreferredName(), progress);
        builder.field(PROCESSING_TIME_MS.getPreferredName(), processingTime);
        builder.field(STATUS.getPreferredName(), status);
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    /**
     * Return the document ID used for indexing. As there is 1 and only 1 document
     * per forecast request, the id has no dynamic parts.
     *
     * @return id
     */
    public String getId() {
        return jobId + "_model_forecast_request_stats_" + forecastId;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public double getRecordCount() {
        return recordCount;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Instant getDateStarted() {
        return dateStarted;
    }

    public void setStartTimeStamp(Instant dateStarted) {
        this.dateStarted = dateStarted;
    }

    public Instant getDateEnded() {
        return dateEnded;
    }

    public void setEndTimeStamp(Instant dateEnded) {
        this.dateEnded = dateEnded;
    }

    /**
     * Progress information of the ForecastRequest in the range 0 to 1,
     * while 1 means finished
     * 
     * @return progress value
     */
    public double getProgress() {
        return progress;
    }

    public void setProgress(double progress) {
        this.progress = progress;
    }

    public long getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(long processingTime) {
        this.processingTime = processingTime;
    }

    public ForecastRequestStatus getStatus() {
        return status;
    }

    public void setStatus(ForecastRequestStatus jobStatus) {
        Objects.requireNonNull(jobStatus, "[" + STATUS.getPreferredName() + "] must not be null");
        this.status = jobStatus;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof ForecastRequestStats == false) {
            return false;
        }
        ForecastRequestStats that = (ForecastRequestStats) other;
        return Objects.equals(this.jobId, that.jobId) &&
                this.forecastId == that.forecastId &&
                this.recordCount == that.recordCount &&
                Objects.equals(this.message, that.message) &&
                Objects.equals(this.dateStarted, that.dateStarted) &&
                Objects.equals(this.dateEnded, that.dateEnded) &&
                this.progress == that.progress &&
                this.processingTime == that.processingTime &&
                Objects.equals(this.status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, forecastId, recordCount, message, dateStarted, dateEnded, progress, 
                processingTime, status);
    }
}
