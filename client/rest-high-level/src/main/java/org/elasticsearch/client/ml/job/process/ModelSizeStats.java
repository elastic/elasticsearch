/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.results.Result;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;

/**
 * Provide access to the C++ model size stats for the Java process.
 */
public class ModelSizeStats implements ToXContentObject {

    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "model_size_stats";
    public static final ParseField RESULT_TYPE_FIELD = new ParseField(RESULT_TYPE_VALUE);

    /**
     * Field Names
     */
    public static final ParseField MODEL_BYTES_FIELD = new ParseField("model_bytes");
    public static final ParseField PEAK_MODEL_BYTES_FIELD = new ParseField("peak_model_bytes");
    public static final ParseField MODEL_BYTES_EXCEEDED_FIELD = new ParseField("model_bytes_exceeded");
    public static final ParseField MODEL_BYTES_MEMORY_LIMIT_FIELD = new ParseField("model_bytes_memory_limit");
    public static final ParseField TOTAL_BY_FIELD_COUNT_FIELD = new ParseField("total_by_field_count");
    public static final ParseField TOTAL_OVER_FIELD_COUNT_FIELD = new ParseField("total_over_field_count");
    public static final ParseField TOTAL_PARTITION_FIELD_COUNT_FIELD = new ParseField("total_partition_field_count");
    public static final ParseField BUCKET_ALLOCATION_FAILURES_COUNT_FIELD = new ParseField("bucket_allocation_failures_count");
    public static final ParseField MEMORY_STATUS_FIELD = new ParseField("memory_status");
    public static final ParseField CATEGORIZED_DOC_COUNT_FIELD = new ParseField("categorized_doc_count");
    public static final ParseField TOTAL_CATEGORY_COUNT_FIELD = new ParseField("total_category_count");
    public static final ParseField FREQUENT_CATEGORY_COUNT_FIELD = new ParseField("frequent_category_count");
    public static final ParseField RARE_CATEGORY_COUNT_FIELD = new ParseField("rare_category_count");
    public static final ParseField DEAD_CATEGORY_COUNT_FIELD = new ParseField("dead_category_count");
    public static final ParseField FAILED_CATEGORY_COUNT_FIELD = new ParseField("failed_category_count");
    public static final ParseField CATEGORIZATION_STATUS_FIELD = new ParseField("categorization_status");
    public static final ParseField LOG_TIME_FIELD = new ParseField("log_time");
    public static final ParseField TIMESTAMP_FIELD = new ParseField("timestamp");

    public static final ConstructingObjectParser<Builder, Void> PARSER =
        new ConstructingObjectParser<>(RESULT_TYPE_VALUE, true, a -> new Builder((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareLong(Builder::setModelBytes, MODEL_BYTES_FIELD);
        PARSER.declareLong(Builder::setPeakModelBytes, PEAK_MODEL_BYTES_FIELD);
        PARSER.declareLong(Builder::setModelBytesExceeded, MODEL_BYTES_EXCEEDED_FIELD);
        PARSER.declareLong(Builder::setModelBytesMemoryLimit, MODEL_BYTES_MEMORY_LIMIT_FIELD);
        PARSER.declareLong(Builder::setBucketAllocationFailuresCount, BUCKET_ALLOCATION_FAILURES_COUNT_FIELD);
        PARSER.declareLong(Builder::setTotalByFieldCount, TOTAL_BY_FIELD_COUNT_FIELD);
        PARSER.declareLong(Builder::setTotalOverFieldCount, TOTAL_OVER_FIELD_COUNT_FIELD);
        PARSER.declareLong(Builder::setTotalPartitionFieldCount, TOTAL_PARTITION_FIELD_COUNT_FIELD);
        PARSER.declareField(Builder::setMemoryStatus, p -> MemoryStatus.fromString(p.text()), MEMORY_STATUS_FIELD, ValueType.STRING);
        PARSER.declareLong(Builder::setCategorizedDocCount, CATEGORIZED_DOC_COUNT_FIELD);
        PARSER.declareLong(Builder::setTotalCategoryCount, TOTAL_CATEGORY_COUNT_FIELD);
        PARSER.declareLong(Builder::setFrequentCategoryCount, FREQUENT_CATEGORY_COUNT_FIELD);
        PARSER.declareLong(Builder::setRareCategoryCount, RARE_CATEGORY_COUNT_FIELD);
        PARSER.declareLong(Builder::setDeadCategoryCount, DEAD_CATEGORY_COUNT_FIELD);
        PARSER.declareLong(Builder::setFailedCategoryCount, FAILED_CATEGORY_COUNT_FIELD);
        PARSER.declareField(Builder::setCategorizationStatus,
            p -> CategorizationStatus.fromString(p.text()), CATEGORIZATION_STATUS_FIELD, ValueType.STRING);
        PARSER.declareField(Builder::setLogTime,
            (p) -> TimeUtil.parseTimeField(p, LOG_TIME_FIELD.getPreferredName()),
            LOG_TIME_FIELD,
            ValueType.VALUE);
        PARSER.declareField(Builder::setTimestamp,
            (p) -> TimeUtil.parseTimeField(p, TIMESTAMP_FIELD.getPreferredName()),
            TIMESTAMP_FIELD,
            ValueType.VALUE);
    }

    /**
     * The status of the memory monitored by the ResourceMonitor. OK is default,
     * SOFT_LIMIT means that the models have done some aggressive pruning to
     * keep the memory below the limit, and HARD_LIMIT means that samples have
     * been dropped
     */
    public enum MemoryStatus {
        OK, SOFT_LIMIT, HARD_LIMIT;

        public static MemoryStatus fromString(String statusName) {
            return valueOf(statusName.trim().toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * The status of categorization for a job. OK is default, WARN
     * means that inappropriate numbers of categories are being found
     */
    public enum CategorizationStatus {
        OK, WARN;

        public static CategorizationStatus fromString(String statusName) {
            return valueOf(statusName.trim().toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final String jobId;
    private final long modelBytes;
    private final Long peakModelBytes;
    private final Long modelBytesExceeded;
    private final Long modelBytesMemoryLimit;
    private final long totalByFieldCount;
    private final long totalOverFieldCount;
    private final long totalPartitionFieldCount;
    private final long bucketAllocationFailuresCount;
    private final MemoryStatus memoryStatus;
    private final long categorizedDocCount;
    private final long totalCategoryCount;
    private final long frequentCategoryCount;
    private final long rareCategoryCount;
    private final long deadCategoryCount;
    private final long failedCategoryCount;
    private final CategorizationStatus categorizationStatus;
    private final Date timestamp;
    private final Date logTime;

    private ModelSizeStats(String jobId, long modelBytes, Long peakModelBytes, Long modelBytesExceeded, Long modelBytesMemoryLimit,
                           long totalByFieldCount, long totalOverFieldCount, long totalPartitionFieldCount,
                           long bucketAllocationFailuresCount, MemoryStatus memoryStatus, long categorizedDocCount, long totalCategoryCount,
                           long frequentCategoryCount, long rareCategoryCount, long deadCategoryCount, long failedCategoryCount,
                           CategorizationStatus categorizationStatus, Date timestamp, Date logTime) {
        this.jobId = jobId;
        this.modelBytes = modelBytes;
        this.peakModelBytes = peakModelBytes;
        this.modelBytesExceeded = modelBytesExceeded;
        this.modelBytesMemoryLimit = modelBytesMemoryLimit;
        this.totalByFieldCount = totalByFieldCount;
        this.totalOverFieldCount = totalOverFieldCount;
        this.totalPartitionFieldCount = totalPartitionFieldCount;
        this.bucketAllocationFailuresCount = bucketAllocationFailuresCount;
        this.memoryStatus = memoryStatus;
        this.categorizedDocCount = categorizedDocCount;
        this.totalCategoryCount = totalCategoryCount;
        this.frequentCategoryCount = frequentCategoryCount;
        this.rareCategoryCount = rareCategoryCount;
        this.deadCategoryCount = deadCategoryCount;
        this.failedCategoryCount = failedCategoryCount;
        this.categorizationStatus = categorizationStatus;
        this.timestamp = timestamp;
        this.logTime = logTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(MODEL_BYTES_FIELD.getPreferredName(), modelBytes);
        if (peakModelBytes != null) {
            builder.field(PEAK_MODEL_BYTES_FIELD.getPreferredName(), peakModelBytes);
        }
        if (modelBytesExceeded != null) {
            builder.field(MODEL_BYTES_EXCEEDED_FIELD.getPreferredName(), modelBytesExceeded);
        }
        if (modelBytesMemoryLimit != null) {
            builder.field(MODEL_BYTES_MEMORY_LIMIT_FIELD.getPreferredName(), modelBytesMemoryLimit);
        }
        builder.field(TOTAL_BY_FIELD_COUNT_FIELD.getPreferredName(), totalByFieldCount);
        builder.field(TOTAL_OVER_FIELD_COUNT_FIELD.getPreferredName(), totalOverFieldCount);
        builder.field(TOTAL_PARTITION_FIELD_COUNT_FIELD.getPreferredName(), totalPartitionFieldCount);
        builder.field(BUCKET_ALLOCATION_FAILURES_COUNT_FIELD.getPreferredName(), bucketAllocationFailuresCount);
        builder.field(MEMORY_STATUS_FIELD.getPreferredName(), memoryStatus);
        builder.field(CATEGORIZED_DOC_COUNT_FIELD.getPreferredName(), categorizedDocCount);
        builder.field(TOTAL_CATEGORY_COUNT_FIELD.getPreferredName(), totalCategoryCount);
        builder.field(FREQUENT_CATEGORY_COUNT_FIELD.getPreferredName(), frequentCategoryCount);
        builder.field(RARE_CATEGORY_COUNT_FIELD.getPreferredName(), rareCategoryCount);
        builder.field(DEAD_CATEGORY_COUNT_FIELD.getPreferredName(), deadCategoryCount);
        builder.field(FAILED_CATEGORY_COUNT_FIELD.getPreferredName(), failedCategoryCount);
        builder.field(CATEGORIZATION_STATUS_FIELD.getPreferredName(), categorizationStatus);
        builder.timeField(LOG_TIME_FIELD.getPreferredName(), LOG_TIME_FIELD.getPreferredName() + "_string", logTime.getTime());
        if (timestamp != null) {
            builder.timeField(TIMESTAMP_FIELD.getPreferredName(), TIMESTAMP_FIELD.getPreferredName() + "_string", timestamp.getTime());
        }

        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public long getModelBytes() {
        return modelBytes;
    }

    public Long getPeakModelBytes() {
        return peakModelBytes;
    }

    public Long getModelBytesExceeded() {
        return modelBytesExceeded;
    }

    public Long getModelBytesMemoryLimit() {
        return modelBytesMemoryLimit;
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

    public long getCategorizedDocCount() {
        return categorizedDocCount;
    }

    public long getTotalCategoryCount() {
        return totalCategoryCount;
    }

    public long getFrequentCategoryCount() {
        return frequentCategoryCount;
    }

    public long getRareCategoryCount() {
        return rareCategoryCount;
    }

    public long getDeadCategoryCount() {
        return deadCategoryCount;
    }

    public long getFailedCategoryCount() {
        return failedCategoryCount;
    }

    public CategorizationStatus getCategorizationStatus() {
        return categorizationStatus;
    }

    /**
     * The timestamp of the last processed record when this instance was created.
     *
     * @return The record time
     */
    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * The wall clock time at the point when this instance was created.
     *
     * @return The wall clock time
     */
    public Date getLogTime() {
        return logTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId, modelBytes, peakModelBytes, modelBytesExceeded, modelBytesMemoryLimit, totalByFieldCount, totalOverFieldCount,
            totalPartitionFieldCount, this.bucketAllocationFailuresCount, memoryStatus, categorizedDocCount, totalCategoryCount,
            frequentCategoryCount, rareCategoryCount, deadCategoryCount, failedCategoryCount, categorizationStatus, timestamp, logTime);
    }

    /**
     * Compare all the fields.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        ModelSizeStats that = (ModelSizeStats) other;

        return this.modelBytes == that.modelBytes
            && Objects.equals(this.peakModelBytes, that.peakModelBytes)
            && Objects.equals(this.modelBytesExceeded, that.modelBytesExceeded)
            && Objects.equals(this.modelBytesMemoryLimit, that.modelBytesMemoryLimit) && this.totalByFieldCount == that.totalByFieldCount
            && this.totalOverFieldCount == that.totalOverFieldCount && this.totalPartitionFieldCount == that.totalPartitionFieldCount
            && this.bucketAllocationFailuresCount == that.bucketAllocationFailuresCount
            && Objects.equals(this.memoryStatus, that.memoryStatus)
            && this.categorizedDocCount == that.categorizedDocCount
            && this.totalCategoryCount == that.totalCategoryCount
            && this.frequentCategoryCount == that.frequentCategoryCount
            && this.rareCategoryCount == that.rareCategoryCount
            && this.deadCategoryCount == that.deadCategoryCount
            && this.failedCategoryCount == that.failedCategoryCount
            && Objects.equals(this.categorizationStatus, that.categorizationStatus)
            && Objects.equals(this.timestamp, that.timestamp)
            && Objects.equals(this.logTime, that.logTime)
            && Objects.equals(this.jobId, that.jobId);
    }

    public static class Builder {

        private final String jobId;
        private long modelBytes;
        private Long peakModelBytes;
        private Long modelBytesExceeded;
        private Long modelBytesMemoryLimit;
        private long totalByFieldCount;
        private long totalOverFieldCount;
        private long totalPartitionFieldCount;
        private long bucketAllocationFailuresCount;
        private MemoryStatus memoryStatus;
        private long categorizedDocCount;
        private long totalCategoryCount;
        private long frequentCategoryCount;
        private long rareCategoryCount;
        private long deadCategoryCount;
        private long failedCategoryCount;
        private CategorizationStatus categorizationStatus;
        private Date timestamp;
        private Date logTime;

        public Builder(String jobId) {
            this.jobId = jobId;
            memoryStatus = MemoryStatus.OK;
            categorizationStatus = CategorizationStatus.OK;
            logTime = new Date();
        }

        public Builder(ModelSizeStats modelSizeStats) {
            this.jobId = modelSizeStats.jobId;
            this.modelBytes = modelSizeStats.modelBytes;
            this.peakModelBytes = modelSizeStats.peakModelBytes;
            this.modelBytesExceeded = modelSizeStats.modelBytesExceeded;
            this.modelBytesMemoryLimit = modelSizeStats.modelBytesMemoryLimit;
            this.totalByFieldCount = modelSizeStats.totalByFieldCount;
            this.totalOverFieldCount = modelSizeStats.totalOverFieldCount;
            this.totalPartitionFieldCount = modelSizeStats.totalPartitionFieldCount;
            this.bucketAllocationFailuresCount = modelSizeStats.bucketAllocationFailuresCount;
            this.memoryStatus = modelSizeStats.memoryStatus;
            this.categorizedDocCount = modelSizeStats.categorizedDocCount;
            this.totalCategoryCount = modelSizeStats.totalCategoryCount;
            this.frequentCategoryCount = modelSizeStats.frequentCategoryCount;
            this.rareCategoryCount = modelSizeStats.rareCategoryCount;
            this.deadCategoryCount = modelSizeStats.deadCategoryCount;
            this.failedCategoryCount = modelSizeStats.failedCategoryCount;
            this.categorizationStatus = modelSizeStats.categorizationStatus;
            this.timestamp = modelSizeStats.timestamp;
            this.logTime = modelSizeStats.logTime;
        }

        public Builder setModelBytes(long modelBytes) {
            this.modelBytes = modelBytes;
            return this;
        }

        public Builder setPeakModelBytes(long peakModelBytes) {
            this.peakModelBytes = peakModelBytes;
            return this;
        }

        public Builder setModelBytesExceeded(long modelBytesExceeded) {
            this.modelBytesExceeded = modelBytesExceeded;
            return this;
        }

        public Builder setModelBytesMemoryLimit(long modelBytesMemoryLimit) {
            this.modelBytesMemoryLimit = modelBytesMemoryLimit;
            return this;
        }

        public Builder setTotalByFieldCount(long totalByFieldCount) {
            this.totalByFieldCount = totalByFieldCount;
            return this;
        }

        public Builder setTotalPartitionFieldCount(long totalPartitionFieldCount) {
            this.totalPartitionFieldCount = totalPartitionFieldCount;
            return this;
        }

        public Builder setTotalOverFieldCount(long totalOverFieldCount) {
            this.totalOverFieldCount = totalOverFieldCount;
            return this;
        }

        public Builder setBucketAllocationFailuresCount(long bucketAllocationFailuresCount) {
            this.bucketAllocationFailuresCount = bucketAllocationFailuresCount;
            return this;
        }

        public Builder setMemoryStatus(MemoryStatus memoryStatus) {
            Objects.requireNonNull(memoryStatus, "[" + MEMORY_STATUS_FIELD.getPreferredName() + "] must not be null");
            this.memoryStatus = memoryStatus;
            return this;
        }

        public Builder setCategorizedDocCount(long categorizedDocCount) {
            this.categorizedDocCount = categorizedDocCount;
            return this;
        }

        public Builder setTotalCategoryCount(long totalCategoryCount) {
            this.totalCategoryCount = totalCategoryCount;
            return this;
        }

        public Builder setFrequentCategoryCount(long frequentCategoryCount) {
            this.frequentCategoryCount = frequentCategoryCount;
            return this;
        }

        public Builder setRareCategoryCount(long rareCategoryCount) {
            this.rareCategoryCount = rareCategoryCount;
            return this;
        }

        public Builder setDeadCategoryCount(long deadCategoryCount) {
            this.deadCategoryCount = deadCategoryCount;
            return this;
        }

        public Builder setFailedCategoryCount(long failedCategoryCount) {
            this.failedCategoryCount = failedCategoryCount;
            return this;
        }

        public Builder setCategorizationStatus(CategorizationStatus categorizationStatus) {
            Objects.requireNonNull(categorizationStatus, "[" + CATEGORIZATION_STATUS_FIELD.getPreferredName() + "] must not be null");
            this.categorizationStatus = categorizationStatus;
            return this;
        }

        public Builder setTimestamp(Date timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder setLogTime(Date logTime) {
            this.logTime = logTime;
            return this;
        }

        public ModelSizeStats build() {
            return new ModelSizeStats(
                jobId, modelBytes, peakModelBytes, modelBytesExceeded, modelBytesMemoryLimit, totalByFieldCount, totalOverFieldCount,
                totalPartitionFieldCount, bucketAllocationFailuresCount, memoryStatus, categorizedDocCount, totalCategoryCount,
                frequentCategoryCount, rareCategoryCount, deadCategoryCount, failedCategoryCount, categorizationStatus, timestamp, logTime);
        }
    }
}
