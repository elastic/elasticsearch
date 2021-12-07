/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.ReservedFieldNames;
import org.elasticsearch.xpack.core.ml.job.results.Result;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class CategorizerStats implements ToXContentObject, Writeable {

    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "categorizer_stats";
    public static final ParseField RESULT_TYPE_FIELD = new ParseField(RESULT_TYPE_VALUE);

    /**
     * Field Names
     */
    public static final ParseField PARTITION_FIELD_NAME = new ParseField("partition_field_name");
    public static final ParseField PARTITION_FIELD_VALUE = new ParseField("partition_field_value");
    public static final ParseField CATEGORIZED_DOC_COUNT_FIELD = new ParseField("categorized_doc_count");
    public static final ParseField TOTAL_CATEGORY_COUNT_FIELD = new ParseField("total_category_count");
    public static final ParseField FREQUENT_CATEGORY_COUNT_FIELD = new ParseField("frequent_category_count");
    public static final ParseField RARE_CATEGORY_COUNT_FIELD = new ParseField("rare_category_count");
    public static final ParseField DEAD_CATEGORY_COUNT_FIELD = new ParseField("dead_category_count");
    public static final ParseField FAILED_CATEGORY_COUNT_FIELD = new ParseField("failed_category_count");
    public static final ParseField CATEGORIZATION_STATUS_FIELD = new ParseField("categorization_status");
    public static final ParseField LOG_TIME_FIELD = new ParseField("log_time");
    public static final ParseField TIMESTAMP_FIELD = new ParseField("timestamp");

    public static final ConstructingObjectParser<Builder, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Builder, Void> parser = new ConstructingObjectParser<>(
            RESULT_TYPE_FIELD.getPreferredName(),
            ignoreUnknownFields,
            a -> new Builder((String) a[0])
        );

        parser.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        parser.declareString((modelSizeStat, s) -> {}, Result.RESULT_TYPE);
        parser.declareString(Builder::setPartitionFieldName, PARTITION_FIELD_NAME);
        parser.declareString(Builder::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        parser.declareLong(Builder::setCategorizedDocCount, CATEGORIZED_DOC_COUNT_FIELD);
        parser.declareLong(Builder::setTotalCategoryCount, TOTAL_CATEGORY_COUNT_FIELD);
        parser.declareLong(Builder::setFrequentCategoryCount, FREQUENT_CATEGORY_COUNT_FIELD);
        parser.declareLong(Builder::setRareCategoryCount, RARE_CATEGORY_COUNT_FIELD);
        parser.declareLong(Builder::setDeadCategoryCount, DEAD_CATEGORY_COUNT_FIELD);
        parser.declareLong(Builder::setFailedCategoryCount, FAILED_CATEGORY_COUNT_FIELD);
        parser.declareField(
            Builder::setCategorizationStatus,
            p -> CategorizationStatus.fromString(p.text()),
            CATEGORIZATION_STATUS_FIELD,
            ValueType.STRING
        );
        parser.declareField(
            Builder::setLogTime,
            p -> TimeUtils.parseTimeFieldToInstant(p, LOG_TIME_FIELD.getPreferredName()),
            LOG_TIME_FIELD,
            ValueType.VALUE
        );
        parser.declareField(
            Builder::setTimestamp,
            p -> TimeUtils.parseTimeFieldToInstant(p, TIMESTAMP_FIELD.getPreferredName()),
            TIMESTAMP_FIELD,
            ValueType.VALUE
        );

        return parser;
    }

    private final String jobId;
    private final String partitionFieldName;
    private final String partitionFieldValue;
    private final long categorizedDocCount;
    private final long totalCategoryCount;
    private final long frequentCategoryCount;
    private final long rareCategoryCount;
    private final long deadCategoryCount;
    private final long failedCategoryCount;
    private final CategorizationStatus categorizationStatus;
    private final Instant timestamp;
    private final Instant logTime;

    private CategorizerStats(
        String jobId,
        @Nullable String partitionFieldName,
        @Nullable String partitionFieldValue,
        long categorizedDocCount,
        long totalCategoryCount,
        long frequentCategoryCount,
        long rareCategoryCount,
        long deadCategoryCount,
        long failedCategoryCount,
        CategorizationStatus categorizationStatus,
        Instant timestamp,
        Instant logTime
    ) {
        this.jobId = Objects.requireNonNull(jobId);
        this.partitionFieldName = partitionFieldName;
        this.partitionFieldValue = partitionFieldValue;
        this.categorizedDocCount = categorizedDocCount;
        this.totalCategoryCount = totalCategoryCount;
        this.frequentCategoryCount = frequentCategoryCount;
        this.rareCategoryCount = rareCategoryCount;
        this.deadCategoryCount = deadCategoryCount;
        this.failedCategoryCount = failedCategoryCount;
        this.categorizationStatus = Objects.requireNonNull(categorizationStatus);
        this.timestamp = Instant.ofEpochMilli(timestamp.toEpochMilli());
        this.logTime = Instant.ofEpochMilli(logTime.toEpochMilli());
    }

    public CategorizerStats(StreamInput in) throws IOException {
        jobId = in.readString();
        partitionFieldName = in.readOptionalString();
        partitionFieldValue = in.readOptionalString();
        categorizedDocCount = in.readVLong();
        totalCategoryCount = in.readVLong();
        frequentCategoryCount = in.readVLong();
        rareCategoryCount = in.readVLong();
        deadCategoryCount = in.readVLong();
        failedCategoryCount = in.readVLong();
        categorizationStatus = CategorizationStatus.readFromStream(in);
        logTime = in.readInstant();
        timestamp = in.readInstant();
    }

    public String getId() {
        StringBuilder idBuilder = new StringBuilder(documentIdPrefix(jobId));
        idBuilder.append(logTime.toEpochMilli());
        if (partitionFieldName != null) {
            idBuilder.append('_').append(MachineLearningField.valuesToId(partitionFieldValue));
        }
        return idBuilder.toString();
    }

    public static String documentIdPrefix(String jobId) {
        return jobId + "_categorizer_stats_";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeOptionalString(partitionFieldName);
        out.writeOptionalString(partitionFieldValue);
        out.writeVLong(categorizedDocCount);
        out.writeVLong(totalCategoryCount);
        out.writeVLong(frequentCategoryCount);
        out.writeVLong(rareCategoryCount);
        out.writeVLong(deadCategoryCount);
        out.writeVLong(failedCategoryCount);
        categorizationStatus.writeTo(out);
        out.writeInstant(logTime);
        out.writeInstant(timestamp);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        if (partitionFieldName != null) {
            builder.field(PARTITION_FIELD_NAME.getPreferredName(), partitionFieldName);
            builder.field(PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue);
            if (ReservedFieldNames.isValidFieldName(partitionFieldName)) {
                builder.field(partitionFieldName, partitionFieldValue);
            }
        }
        builder.field(CATEGORIZED_DOC_COUNT_FIELD.getPreferredName(), categorizedDocCount);
        builder.field(TOTAL_CATEGORY_COUNT_FIELD.getPreferredName(), totalCategoryCount);
        builder.field(FREQUENT_CATEGORY_COUNT_FIELD.getPreferredName(), frequentCategoryCount);
        builder.field(RARE_CATEGORY_COUNT_FIELD.getPreferredName(), rareCategoryCount);
        builder.field(DEAD_CATEGORY_COUNT_FIELD.getPreferredName(), deadCategoryCount);
        builder.field(FAILED_CATEGORY_COUNT_FIELD.getPreferredName(), failedCategoryCount);
        builder.field(CATEGORIZATION_STATUS_FIELD.getPreferredName(), categorizationStatus);
        builder.timeField(LOG_TIME_FIELD.getPreferredName(), LOG_TIME_FIELD.getPreferredName() + "_string", logTime.toEpochMilli());
        builder.timeField(TIMESTAMP_FIELD.getPreferredName(), TIMESTAMP_FIELD.getPreferredName() + "_string", timestamp.toEpochMilli());
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    public String getPartitionFieldValue() {
        return partitionFieldValue;
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
        return deadCategoryCount;
    }

    public CategorizationStatus getCategorizationStatus() {
        return categorizationStatus;
    }

    /**
     * The model timestamp when these stats were created.
     * @return The model time
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * The wall clock time at the point when this instance was created.
     * @return The wall clock time
     */
    public Instant getLogTime() {
        return logTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            partitionFieldName,
            partitionFieldValue,
            categorizedDocCount,
            totalCategoryCount,
            frequentCategoryCount,
            rareCategoryCount,
            deadCategoryCount,
            failedCategoryCount,
            categorizationStatus,
            timestamp,
            logTime
        );
    }

    /**
     * Compare all the fields.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof CategorizerStats == false) {
            return false;
        }

        CategorizerStats that = (CategorizerStats) other;

        return Objects.equals(this.jobId, that.jobId)
            && Objects.equals(this.partitionFieldName, that.partitionFieldName)
            && Objects.equals(this.partitionFieldValue, that.partitionFieldValue)
            && Objects.equals(this.categorizedDocCount, that.categorizedDocCount)
            && Objects.equals(this.totalCategoryCount, that.totalCategoryCount)
            && Objects.equals(this.frequentCategoryCount, that.frequentCategoryCount)
            && Objects.equals(this.rareCategoryCount, that.rareCategoryCount)
            && Objects.equals(this.deadCategoryCount, that.deadCategoryCount)
            && Objects.equals(this.failedCategoryCount, that.failedCategoryCount)
            && Objects.equals(this.categorizationStatus, that.categorizationStatus)
            && Objects.equals(this.timestamp, that.timestamp)
            && Objects.equals(this.logTime, that.logTime);
    }

    public static class Builder {

        private final String jobId;
        private String partitionFieldName;
        private String partitionFieldValue;
        private long categorizedDocCount;
        private long totalCategoryCount;
        private long frequentCategoryCount;
        private long rareCategoryCount;
        private long deadCategoryCount;
        private long failedCategoryCount;
        private CategorizationStatus categorizationStatus = CategorizationStatus.OK;
        private Instant timestamp = Instant.EPOCH;
        private Instant logTime = Instant.EPOCH;

        public Builder(String jobId) {
            this.jobId = Objects.requireNonNull(jobId, "[" + Job.ID.getPreferredName() + "] must not be null");
        }

        public Builder(CategorizerStats categorizerStats) {
            this.jobId = categorizerStats.jobId;
            this.partitionFieldName = categorizerStats.partitionFieldName;
            this.partitionFieldValue = categorizerStats.partitionFieldValue;
            this.categorizedDocCount = categorizerStats.categorizedDocCount;
            this.totalCategoryCount = categorizerStats.totalCategoryCount;
            this.frequentCategoryCount = categorizerStats.frequentCategoryCount;
            this.rareCategoryCount = categorizerStats.rareCategoryCount;
            this.deadCategoryCount = categorizerStats.deadCategoryCount;
            this.failedCategoryCount = categorizerStats.failedCategoryCount;
            this.categorizationStatus = categorizerStats.categorizationStatus;
            this.timestamp = categorizerStats.timestamp;
            this.logTime = categorizerStats.logTime;
        }

        public Builder setPartitionFieldName(String partitionFieldName) {
            this.partitionFieldName = partitionFieldName;
            return this;
        }

        public Builder setPartitionFieldValue(String partitionFieldValue) {
            this.partitionFieldValue = partitionFieldValue;
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
            this.categorizationStatus = Objects.requireNonNull(
                categorizationStatus,
                "[" + CATEGORIZATION_STATUS_FIELD.getPreferredName() + "] must not be null"
            );
            ;
            return this;
        }

        public Builder setTimestamp(Instant timestamp) {
            this.timestamp = Objects.requireNonNull(timestamp, "[" + TIMESTAMP_FIELD.getPreferredName() + "] must not be null");
            return this;
        }

        public Builder setLogTime(Instant logTime) {
            this.logTime = Objects.requireNonNull(logTime, "[" + LOG_TIME_FIELD.getPreferredName() + "] must not be null");
            return this;
        }

        public CategorizerStats build() {
            return new CategorizerStats(
                jobId,
                partitionFieldName,
                partitionFieldValue,
                categorizedDocCount,
                totalCategoryCount,
                frequentCategoryCount,
                rareCategoryCount,
                deadCategoryCount,
                failedCategoryCount,
                categorizationStatus,
                timestamp,
                logTime
            );
        }
    }
}
