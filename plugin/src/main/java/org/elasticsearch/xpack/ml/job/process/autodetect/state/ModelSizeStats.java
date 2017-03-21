/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.state;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.results.Result;
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;

/**
 * Provide access to the C++ model memory usage numbers for the Java process.
 */
public class ModelSizeStats extends ToXContentToBytes implements Writeable {

    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "model_size_stats";
    public static final ParseField RESULT_TYPE_FIELD = new ParseField(RESULT_TYPE_VALUE);

    /**
     * Field Names
     */
    public static final ParseField MODEL_BYTES_FIELD = new ParseField("model_bytes");
    public static final ParseField TOTAL_BY_FIELD_COUNT_FIELD = new ParseField("total_by_field_count");
    public static final ParseField TOTAL_OVER_FIELD_COUNT_FIELD = new ParseField("total_over_field_count");
    public static final ParseField TOTAL_PARTITION_FIELD_COUNT_FIELD = new ParseField("total_partition_field_count");
    public static final ParseField BUCKET_ALLOCATION_FAILURES_COUNT_FIELD = new ParseField("bucket_allocation_failures_count");
    public static final ParseField MEMORY_STATUS_FIELD = new ParseField("memory_status");
    public static final ParseField LOG_TIME_FIELD = new ParseField("log_time");
    public static final ParseField TIMESTAMP_FIELD = new ParseField("timestamp");

    public static final ConstructingObjectParser<Builder, Void> PARSER = new ConstructingObjectParser<>(
            RESULT_TYPE_FIELD.getPreferredName(), a -> new Builder((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString((modelSizeStat, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareLong(Builder::setModelBytes, MODEL_BYTES_FIELD);
        PARSER.declareLong(Builder::setBucketAllocationFailuresCount, BUCKET_ALLOCATION_FAILURES_COUNT_FIELD);
        PARSER.declareLong(Builder::setTotalByFieldCount, TOTAL_BY_FIELD_COUNT_FIELD);
        PARSER.declareLong(Builder::setTotalOverFieldCount, TOTAL_OVER_FIELD_COUNT_FIELD);
        PARSER.declareLong(Builder::setTotalPartitionFieldCount, TOTAL_PARTITION_FIELD_COUNT_FIELD);
        PARSER.declareField(Builder::setLogTime, p -> {
            if (p.currentToken() == Token.VALUE_NUMBER) {
                return new Date(p.longValue());
            } else if (p.currentToken() == Token.VALUE_STRING) {
                return new Date(TimeUtils.dateStringToEpoch(p.text()));
            }
            throw new IllegalArgumentException(
                    "unexpected token [" + p.currentToken() + "] for [" + LOG_TIME_FIELD.getPreferredName() + "]");
        }, LOG_TIME_FIELD, ValueType.VALUE);
        PARSER.declareField(Builder::setTimestamp, p -> {
            if (p.currentToken() == Token.VALUE_NUMBER) {
                return new Date(p.longValue());
            } else if (p.currentToken() == Token.VALUE_STRING) {
                return new Date(TimeUtils.dateStringToEpoch(p.text()));
            }
            throw new IllegalArgumentException(
                    "unexpected token [" + p.currentToken() + "] for [" + TIMESTAMP_FIELD.getPreferredName() + "]");
        }, TIMESTAMP_FIELD, ValueType.VALUE);
        PARSER.declareField(Builder::setMemoryStatus, p -> MemoryStatus.fromString(p.text()), MEMORY_STATUS_FIELD, ValueType.STRING);
    }

    public static String documentId(String jobId) {
        return jobId + "-" + RESULT_TYPE_VALUE;
    }

    /**
     * The status of the memory monitored by the ResourceMonitor. OK is default,
     * SOFT_LIMIT means that the models have done some aggressive pruning to
     * keep the memory below the limit, and HARD_LIMIT means that samples have
     * been dropped
     */
    public enum MemoryStatus implements Writeable {
        OK, SOFT_LIMIT, HARD_LIMIT;

        public static MemoryStatus fromString(String statusName) {
            return valueOf(statusName.trim().toUpperCase(Locale.ROOT));
        }

        public static MemoryStatus readFromStream(StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown MemoryStatus ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final String jobId;
    private final long modelBytes;
    private final long totalByFieldCount;
    private final long totalOverFieldCount;
    private final long totalPartitionFieldCount;
    private final long bucketAllocationFailuresCount;
    private final MemoryStatus memoryStatus;
    private final Date timestamp;
    private final Date logTime;

    private ModelSizeStats(String jobId, String id, long modelBytes, long totalByFieldCount, long totalOverFieldCount,
                           long totalPartitionFieldCount, long bucketAllocationFailuresCount, MemoryStatus memoryStatus,
                           Date timestamp, Date logTime) {
        this.jobId = jobId;
        this.modelBytes = modelBytes;
        this.totalByFieldCount = totalByFieldCount;
        this.totalOverFieldCount = totalOverFieldCount;
        this.totalPartitionFieldCount = totalPartitionFieldCount;
        this.bucketAllocationFailuresCount = bucketAllocationFailuresCount;
        this.memoryStatus = memoryStatus;
        this.timestamp = timestamp;
        this.logTime = logTime;
    }

    public ModelSizeStats(StreamInput in) throws IOException {
        jobId = in.readString();
        modelBytes = in.readVLong();
        totalByFieldCount = in.readVLong();
        totalOverFieldCount = in.readVLong();
        totalPartitionFieldCount = in.readVLong();
        bucketAllocationFailuresCount = in.readVLong();
        memoryStatus = MemoryStatus.readFromStream(in);
        logTime = new Date(in.readVLong());
        timestamp = in.readBoolean() ? new Date(in.readVLong()) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeVLong(modelBytes);
        out.writeVLong(totalByFieldCount);
        out.writeVLong(totalOverFieldCount);
        out.writeVLong(totalPartitionFieldCount);
        out.writeVLong(bucketAllocationFailuresCount);
        memoryStatus.writeTo(out);
        out.writeVLong(logTime.getTime());
        boolean hasTimestamp = timestamp != null;
        out.writeBoolean(hasTimestamp);
        if (hasTimestamp) {
            out.writeVLong(timestamp.getTime());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContentBody(builder);
        builder.endObject();
        return builder;
    }

    public XContentBuilder doXContentBody(XContentBuilder builder) throws IOException {
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(MODEL_BYTES_FIELD.getPreferredName(), modelBytes);
        builder.field(TOTAL_BY_FIELD_COUNT_FIELD.getPreferredName(), totalByFieldCount);
        builder.field(TOTAL_OVER_FIELD_COUNT_FIELD.getPreferredName(), totalOverFieldCount);
        builder.field(TOTAL_PARTITION_FIELD_COUNT_FIELD.getPreferredName(), totalPartitionFieldCount);
        builder.field(BUCKET_ALLOCATION_FAILURES_COUNT_FIELD.getPreferredName(), bucketAllocationFailuresCount);
        builder.field(MEMORY_STATUS_FIELD.getPreferredName(), memoryStatus);
        builder.dateField(LOG_TIME_FIELD.getPreferredName(), LOG_TIME_FIELD.getPreferredName() + "_string", logTime.getTime());
        if (timestamp != null) {
            builder.dateField(TIMESTAMP_FIELD.getPreferredName(), TIMESTAMP_FIELD.getPreferredName() + "_string", timestamp.getTime());
        }

        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public long getModelBytes() {
        return modelBytes;
    }

    public long getTotalByFieldCount() {
        return totalByFieldCount;
    }

    public long getTotalPartitionFieldCount() {
        return totalPartitionFieldCount;
    }

    public long getTotalOverFieldCount() {
        return totalOverFieldCount;
    }

    public long getBucketAllocationFailuresCount() {
        return bucketAllocationFailuresCount;
    }

    public MemoryStatus getMemoryStatus() {
        return memoryStatus;
    }

    /**
     * The timestamp of the last processed record when this instance was created.
     * @return The record time
     */
    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * The wall clock time at the point when this instance was created.
     * @return The wall clock time
     */
    public Date getLogTime() {
        return logTime;
    }

    @Override
    public int hashCode() {
        // this.id excluded here as it is generated by the datastore
        return Objects.hash(jobId, modelBytes, totalByFieldCount, totalOverFieldCount, totalPartitionFieldCount,
                this.bucketAllocationFailuresCount, memoryStatus, timestamp, logTime);
    }

    /**
     * Compare all the fields.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof ModelSizeStats == false) {
            return false;
        }

        ModelSizeStats that = (ModelSizeStats) other;

        return this.modelBytes == that.modelBytes && this.totalByFieldCount == that.totalByFieldCount
                && this.totalOverFieldCount == that.totalOverFieldCount && this.totalPartitionFieldCount == that.totalPartitionFieldCount
                && this.bucketAllocationFailuresCount == that.bucketAllocationFailuresCount
                && Objects.equals(this.memoryStatus, that.memoryStatus) && Objects.equals(this.timestamp, that.timestamp)
                && Objects.equals(this.logTime, that.logTime)
                && Objects.equals(this.jobId, that.jobId);
    }

    // NORELEASE This will not be needed once we are able to parse ModelSizeStats all at once.
    public static class Builder {

        private final String jobId;
        private String id;
        private long modelBytes;
        private long totalByFieldCount;
        private long totalOverFieldCount;
        private long totalPartitionFieldCount;
        private long bucketAllocationFailuresCount;
        private MemoryStatus memoryStatus;
        private Date timestamp;
        private Date logTime;

        public Builder(String jobId) {
            this.jobId = jobId;
            id = RESULT_TYPE_FIELD.getPreferredName();
            memoryStatus = MemoryStatus.OK;
            logTime = new Date();
        }

        public void setId(String id) {
            this.id = Objects.requireNonNull(id);
        }

        public void setModelBytes(long modelBytes) {
            this.modelBytes = modelBytes;
        }

        public void setTotalByFieldCount(long totalByFieldCount) {
            this.totalByFieldCount = totalByFieldCount;
        }

        public void setTotalPartitionFieldCount(long totalPartitionFieldCount) {
            this.totalPartitionFieldCount = totalPartitionFieldCount;
        }

        public void setTotalOverFieldCount(long totalOverFieldCount) {
            this.totalOverFieldCount = totalOverFieldCount;
        }

        public void setBucketAllocationFailuresCount(long bucketAllocationFailuresCount) {
            this.bucketAllocationFailuresCount = bucketAllocationFailuresCount;
        }

        public void setMemoryStatus(MemoryStatus memoryStatus) {
            Objects.requireNonNull(memoryStatus, "[" + MEMORY_STATUS_FIELD.getPreferredName() + "] must not be null");
            this.memoryStatus = memoryStatus;
        }

        public void setTimestamp(Date timestamp) {
            this.timestamp = timestamp;
        }

        public void setLogTime(Date logTime) {
            this.logTime = logTime;
        }

        public ModelSizeStats build() {
            return new ModelSizeStats(jobId, id, modelBytes, totalByFieldCount, totalOverFieldCount, totalPartitionFieldCount,
                    bucketAllocationFailuresCount, memoryStatus, timestamp, logTime);
        }
    }
}
