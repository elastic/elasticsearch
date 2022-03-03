/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
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
    public static final ParseField CREATE_TIME = new ParseField("forecast_create_timestamp");
    public static final ParseField EXPIRY_TIME = new ParseField("forecast_expiry_timestamp");
    public static final ParseField MESSAGES = new ParseField("forecast_messages");
    public static final ParseField PROCESSING_TIME_MS = new ParseField("processing_time_ms");
    public static final ParseField PROGRESS = new ParseField("forecast_progress");
    public static final ParseField PROCESSED_RECORD_COUNT = new ParseField("processed_record_count");
    public static final ParseField STATUS = new ParseField("forecast_status");
    public static final ParseField MEMORY_USAGE = new ParseField("forecast_memory_bytes");

    public static final ConstructingObjectParser<ForecastRequestStats, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<ForecastRequestStats, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<ForecastRequestStats, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<ForecastRequestStats, Void> parser = new ConstructingObjectParser<>(
            RESULT_TYPE_VALUE,
            ignoreUnknownFields,
            a -> new ForecastRequestStats((String) a[0], (String) a[1])
        );

        parser.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        parser.declareString(ConstructingObjectParser.constructorArg(), FORECAST_ID);

        parser.declareString((modelForecastRequestStats, s) -> {}, Result.RESULT_TYPE);
        parser.declareLong(ForecastRequestStats::setRecordCount, PROCESSED_RECORD_COUNT);
        parser.declareStringArray(ForecastRequestStats::setMessages, MESSAGES);
        parser.declareField(ForecastRequestStats::setTimeStamp, p -> Instant.ofEpochMilli(p.longValue()), Result.TIMESTAMP, ValueType.LONG);
        parser.declareField(ForecastRequestStats::setStartTime, p -> Instant.ofEpochMilli(p.longValue()), START_TIME, ValueType.LONG);
        parser.declareField(ForecastRequestStats::setEndTime, p -> Instant.ofEpochMilli(p.longValue()), END_TIME, ValueType.LONG);
        parser.declareField(ForecastRequestStats::setCreateTime, p -> Instant.ofEpochMilli(p.longValue()), CREATE_TIME, ValueType.LONG);
        parser.declareField(ForecastRequestStats::setExpiryTime, p -> Instant.ofEpochMilli(p.longValue()), EXPIRY_TIME, ValueType.LONG);
        parser.declareDouble(ForecastRequestStats::setProgress, PROGRESS);
        parser.declareLong(ForecastRequestStats::setProcessingTime, PROCESSING_TIME_MS);
        parser.declareField(ForecastRequestStats::setStatus, p -> ForecastRequestStatus.fromString(p.text()), STATUS, ValueType.STRING);
        parser.declareLong(ForecastRequestStats::setMemoryUsage, MEMORY_USAGE);

        return parser;
    }

    public enum ForecastRequestStatus implements Writeable {
        OK,
        FAILED,
        STOPPED,
        STARTED,
        FINISHED,
        SCHEDULED;

        public static ForecastRequestStatus fromString(String statusName) {
            return valueOf(statusName.trim().toUpperCase(Locale.ROOT));
        }

        public static ForecastRequestStatus readFromStream(StreamInput in) throws IOException {
            return in.readEnum(ForecastRequestStatus.class);
        }

        /**
         * @return {@code true} if state matches any of the given {@code candidates}
         */
        public boolean isAnyOf(ForecastRequestStatus... candidates) {
            return Arrays.stream(candidates).anyMatch(candidate -> this == candidate);
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
    private final String forecastId;
    private long recordCount;
    private List<String> messages;
    private Instant timestamp = Instant.EPOCH;
    private Instant startTime = Instant.EPOCH;
    private Instant endTime = Instant.EPOCH;
    private Instant createTime = Instant.EPOCH;
    private Instant expiryTime = Instant.EPOCH;
    private double progress;
    private long processingTime;
    private long memoryUsage;
    private ForecastRequestStatus status = ForecastRequestStatus.OK;

    public ForecastRequestStats(String jobId, String forecastId) {
        this.jobId = Objects.requireNonNull(jobId);
        this.forecastId = Objects.requireNonNull(forecastId);
    }

    public ForecastRequestStats(ForecastRequestStats forecastRequestStats) {
        this.jobId = forecastRequestStats.jobId;
        this.forecastId = forecastRequestStats.forecastId;
        this.recordCount = forecastRequestStats.recordCount;
        this.messages = forecastRequestStats.messages;
        this.timestamp = forecastRequestStats.timestamp;
        this.startTime = forecastRequestStats.startTime;
        this.endTime = forecastRequestStats.endTime;
        this.createTime = forecastRequestStats.createTime;
        this.expiryTime = forecastRequestStats.expiryTime;
        this.progress = forecastRequestStats.progress;
        this.processingTime = forecastRequestStats.processingTime;
        this.memoryUsage = forecastRequestStats.memoryUsage;
        this.status = forecastRequestStats.status;
    }

    public ForecastRequestStats(StreamInput in) throws IOException {
        jobId = in.readString();
        forecastId = in.readString();
        recordCount = in.readLong();
        if (in.readBoolean()) {
            messages = in.readStringList();
        } else {
            messages = null;
        }

        timestamp = in.readInstant();
        startTime = in.readInstant();
        endTime = in.readInstant();
        createTime = in.readInstant();
        expiryTime = in.readInstant();

        progress = in.readDouble();
        processingTime = in.readLong();
        setMemoryUsage(in.readLong());
        status = ForecastRequestStatus.readFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeString(forecastId);
        out.writeLong(recordCount);
        if (messages != null) {
            out.writeBoolean(true);
            out.writeStringCollection(messages);
        } else {
            out.writeBoolean(false);
        }

        out.writeInstant(timestamp);
        out.writeInstant(startTime);
        out.writeInstant(endTime);
        out.writeInstant(createTime);
        out.writeInstant(expiryTime);

        out.writeDouble(progress);
        out.writeLong(processingTime);
        out.writeLong(getMemoryUsage());
        status.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(FORECAST_ID.getPreferredName(), forecastId);
        builder.field(PROCESSED_RECORD_COUNT.getPreferredName(), recordCount);
        if (messages != null) {
            builder.field(MESSAGES.getPreferredName(), messages);
        }
        if (timestamp.equals(Instant.EPOCH) == false) {
            builder.field(Result.TIMESTAMP.getPreferredName(), timestamp.toEpochMilli());
        }
        if (startTime.equals(Instant.EPOCH) == false) {
            builder.field(START_TIME.getPreferredName(), startTime.toEpochMilli());
        }
        if (endTime.equals(Instant.EPOCH) == false) {
            builder.field(END_TIME.getPreferredName(), endTime.toEpochMilli());
        }
        if (createTime.equals(Instant.EPOCH) == false) {
            builder.field(CREATE_TIME.getPreferredName(), createTime.toEpochMilli());
        }
        if (expiryTime.equals(Instant.EPOCH) == false) {
            builder.field(EXPIRY_TIME.getPreferredName(), expiryTime.toEpochMilli());
        }
        builder.field(PROGRESS.getPreferredName(), progress);
        builder.field(PROCESSING_TIME_MS.getPreferredName(), processingTime);
        builder.field(MEMORY_USAGE.getPreferredName(), getMemoryUsage());
        builder.field(STATUS.getPreferredName(), status);
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public String getForecastId() {
        return forecastId;
    }

    public static String documentId(String jobId, String forecastId) {
        return jobId + "_model_forecast_request_stats_" + forecastId;
    }

    /**
     * Return the document ID used for indexing. As there is 1 and only 1 document
     * per forecast request, the id has no dynamic parts.
     *
     * @return id
     */
    public String getId() {
        return documentId(jobId, forecastId);
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }

    public void setTimeStamp(Instant timeStamp) {
        this.timestamp = Instant.ofEpochMilli(timeStamp.toEpochMilli());
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = Instant.ofEpochMilli(startTime.toEpochMilli());
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = Instant.ofEpochMilli(endTime.toEpochMilli());
    }

    public void setCreateTime(Instant createTime) {
        this.createTime = Instant.ofEpochMilli(createTime.toEpochMilli());
    }

    public Instant getCreateTime() {
        return createTime;
    }

    public void setExpiryTime(Instant expiryTime) {
        this.expiryTime = Instant.ofEpochMilli(expiryTime.toEpochMilli());
    }

    public Instant getExpiryTime() {
        return expiryTime;
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

    public long getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(long memoryUsage) {
        this.memoryUsage = memoryUsage;
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
        return Objects.equals(this.jobId, that.jobId)
            && Objects.equals(this.forecastId, that.forecastId)
            && this.recordCount == that.recordCount
            && Objects.equals(this.messages, that.messages)
            && Objects.equals(this.timestamp, that.timestamp)
            && Objects.equals(this.startTime, that.startTime)
            && Objects.equals(this.endTime, that.endTime)
            && Objects.equals(this.createTime, that.createTime)
            && Objects.equals(this.expiryTime, that.expiryTime)
            && this.progress == that.progress
            && this.processingTime == that.processingTime
            && this.memoryUsage == that.memoryUsage
            && Objects.equals(this.status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            forecastId,
            recordCount,
            messages,
            timestamp,
            startTime,
            endTime,
            createTime,
            expiryTime,
            progress,
            processingTime,
            memoryUsage,
            status
        );
    }
}
