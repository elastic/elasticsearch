/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;

/**
 * Job processed record counts.
 * <p>
 * The getInput... methods return the actual number of
 * fields/records sent the API including invalid records.
 * The getProcessed... methods are the number sent to the
 * Engine.
 * <p>
 * The <code>inputRecordCount</code> field is calculated so it
 * should not be set in deserialization but it should be serialised
 * so the field is visible.
 */
public class DataCounts implements ToXContentObject {

    public static final ParseField PROCESSED_RECORD_COUNT = new ParseField("processed_record_count");
    public static final ParseField PROCESSED_FIELD_COUNT = new ParseField("processed_field_count");
    public static final ParseField INPUT_BYTES = new ParseField("input_bytes");
    public static final ParseField INPUT_RECORD_COUNT = new ParseField("input_record_count");
    public static final ParseField INPUT_FIELD_COUNT = new ParseField("input_field_count");
    public static final ParseField INVALID_DATE_COUNT = new ParseField("invalid_date_count");
    public static final ParseField MISSING_FIELD_COUNT = new ParseField("missing_field_count");
    public static final ParseField OUT_OF_ORDER_TIME_COUNT = new ParseField("out_of_order_timestamp_count");
    public static final ParseField EMPTY_BUCKET_COUNT = new ParseField("empty_bucket_count");
    public static final ParseField SPARSE_BUCKET_COUNT = new ParseField("sparse_bucket_count");
    public static final ParseField BUCKET_COUNT = new ParseField("bucket_count");
    public static final ParseField EARLIEST_RECORD_TIME = new ParseField("earliest_record_timestamp");
    public static final ParseField LATEST_RECORD_TIME = new ParseField("latest_record_timestamp");
    public static final ParseField LAST_DATA_TIME = new ParseField("last_data_time");
    public static final ParseField LATEST_EMPTY_BUCKET_TIME = new ParseField("latest_empty_bucket_timestamp");
    public static final ParseField LATEST_SPARSE_BUCKET_TIME = new ParseField("latest_sparse_bucket_timestamp");
    public static final ParseField LOG_TIME = new ParseField("log_time");

    public static final ConstructingObjectParser<DataCounts, Void> PARSER = new ConstructingObjectParser<>("data_counts", true,
            a -> new DataCounts((String) a[0], (long) a[1], (long) a[2], (long) a[3], (long) a[4], (long) a[5], (long) a[6],
                    (long) a[7], (long) a[8], (long) a[9], (long) a[10], (Date) a[11], (Date) a[12], (Date) a[13], (Date) a[14],
                    (Date) a[15], (Instant) a[16]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), PROCESSED_RECORD_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), PROCESSED_FIELD_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INPUT_BYTES);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INPUT_FIELD_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INVALID_DATE_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MISSING_FIELD_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), OUT_OF_ORDER_TIME_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), EMPTY_BUCKET_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), SPARSE_BUCKET_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_COUNT);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p) -> TimeUtil.parseTimeField(p, EARLIEST_RECORD_TIME.getPreferredName()),
            EARLIEST_RECORD_TIME,
            ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p) -> TimeUtil.parseTimeField(p, LATEST_RECORD_TIME.getPreferredName()),
            LATEST_RECORD_TIME,
            ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p) -> TimeUtil.parseTimeField(p, LAST_DATA_TIME.getPreferredName()),
            LAST_DATA_TIME,
            ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p) -> TimeUtil.parseTimeField(p, LATEST_EMPTY_BUCKET_TIME.getPreferredName()),
            LATEST_EMPTY_BUCKET_TIME,
            ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p) -> TimeUtil.parseTimeField(p, LATEST_SPARSE_BUCKET_TIME.getPreferredName()),
            LATEST_SPARSE_BUCKET_TIME,
            ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            p -> TimeUtil.parseTimeFieldToInstant(p, LOG_TIME.getPreferredName()),
            LOG_TIME,
            ValueType.VALUE);
    }

    private final String jobId;
    private long processedRecordCount;
    private long processedFieldCount;
    private long inputBytes;
    private long inputFieldCount;
    private long invalidDateCount;
    private long missingFieldCount;
    private long outOfOrderTimeStampCount;
    private long emptyBucketCount;
    private long sparseBucketCount;
    private long bucketCount;
    private Date earliestRecordTimeStamp;
    private Date latestRecordTimeStamp;
    private Date lastDataTimeStamp;
    private Date latestEmptyBucketTimeStamp;
    private Date latestSparseBucketTimeStamp;
    private Instant logTime;

    public DataCounts(String jobId, long processedRecordCount, long processedFieldCount, long inputBytes,
                      long inputFieldCount, long invalidDateCount, long missingFieldCount, long outOfOrderTimeStampCount,
                      long emptyBucketCount, long sparseBucketCount, long bucketCount,
                      Date earliestRecordTimeStamp, Date latestRecordTimeStamp, Date lastDataTimeStamp,
                      Date latestEmptyBucketTimeStamp, Date latestSparseBucketTimeStamp, Instant logTime) {
        this.jobId = jobId;
        this.processedRecordCount = processedRecordCount;
        this.processedFieldCount = processedFieldCount;
        this.inputBytes = inputBytes;
        this.inputFieldCount = inputFieldCount;
        this.invalidDateCount = invalidDateCount;
        this.missingFieldCount = missingFieldCount;
        this.outOfOrderTimeStampCount = outOfOrderTimeStampCount;
        this.emptyBucketCount = emptyBucketCount;
        this.sparseBucketCount = sparseBucketCount;
        this.bucketCount = bucketCount;
        this.latestRecordTimeStamp = latestRecordTimeStamp;
        this.earliestRecordTimeStamp = earliestRecordTimeStamp;
        this.lastDataTimeStamp = lastDataTimeStamp;
        this.latestEmptyBucketTimeStamp = latestEmptyBucketTimeStamp;
        this.latestSparseBucketTimeStamp = latestSparseBucketTimeStamp;
        this.logTime = logTime == null ? null : Instant.ofEpochMilli(logTime.toEpochMilli());
    }

    DataCounts(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    /**
     * Number of records processed by this job.
     * This value is the number of records sent passed on to
     * the engine i.e. {@linkplain #getInputRecordCount()} minus
     * records with bad dates or out of order
     *
     * @return Number of records processed by this job {@code long}
     */
    public long getProcessedRecordCount() {
        return processedRecordCount;
    }

    /**
     * Number of data points (processed record count * the number
     * of analysed fields) processed by this job. This count does
     * not include the time field.
     *
     * @return Number of data points processed by this job {@code long}
     */
    public long getProcessedFieldCount() {
        return processedFieldCount;
    }

    /**
     * Total number of input records read.
     * This = processed record count + date parse error records count
     * + out of order record count.
     * <p>
     * Records with missing fields are counted as they are still written.
     *
     * @return Total number of input records read {@code long}
     */
    public long getInputRecordCount() {
        return processedRecordCount + outOfOrderTimeStampCount
                + invalidDateCount;
    }

    /**
     * The total number of bytes sent to this job.
     * This value includes the bytes from any  records
     * that have been discarded for any  reason
     * e.g. because the date cannot be read
     *
     * @return Volume in bytes
     */
    public long getInputBytes() {
        return inputBytes;
    }

    /**
     * The total number of fields sent to the job
     * including fields that aren't analysed.
     *
     * @return The total number of fields sent to the job
     */
    public long getInputFieldCount() {
        return inputFieldCount;
    }

    /**
     * The number of records with an invalid date field that could
     * not be parsed or converted to epoch time.
     *
     * @return The number of records with an invalid date field
     */
    public long getInvalidDateCount() {
        return invalidDateCount;
    }

    /**
     * The number of missing fields that had been
     * configured for analysis.
     *
     * @return The number of missing fields
     */
    public long getMissingFieldCount() {
        return missingFieldCount;
    }

    /**
     * The number of records with a timestamp that is
     * before the time of the latest record. Records should
     * be in ascending chronological order
     *
     * @return The number of records with a timestamp that is before the time of the latest record
     */
    public long getOutOfOrderTimeStampCount() {
        return outOfOrderTimeStampCount;
    }

    /**
     * The number of buckets with no records in it. Used to measure general data fitness and/or
     * configuration problems (bucket span).
     *
     * @return Number of empty buckets processed by this job {@code long}
     */
    public long getEmptyBucketCount() {
        return emptyBucketCount;
    }

    /**
     * The number of buckets with few records compared to the overall counts.
     * Used to measure general data fitness and/or configuration problems (bucket span).
     *
     * @return Number of sparse buckets processed by this job {@code long}
     */
    public long getSparseBucketCount() {
        return sparseBucketCount;
    }

    /**
     * The number of buckets overall.
     *
     * @return Number of buckets processed by this job {@code long}
     */
    public long getBucketCount() {
        return bucketCount;
    }

    /**
     * The time of the first record seen.
     *
     * @return The first record time
     */
    public Date getEarliestRecordTimeStamp() {
        return earliestRecordTimeStamp;
    }

    /**
     * The time of the latest record seen.
     *
     * @return Latest record time
     */
    public Date getLatestRecordTimeStamp() {
        return latestRecordTimeStamp;
    }

    /**
     * The wall clock time the latest record was seen.
     *
     * @return Wall clock time of the lastest record
     */
    public Date getLastDataTimeStamp() {
        return lastDataTimeStamp;
    }

    /**
     * The time of the latest empty bucket seen.
     *
     * @return Latest empty bucket time
     */
    public Date getLatestEmptyBucketTimeStamp() {
        return latestEmptyBucketTimeStamp;
    }

    /**
     * The time of the latest sparse bucket seen.
     *
     * @return Latest sparse bucket time
     */
    public Date getLatestSparseBucketTimeStamp() {
        return latestSparseBucketTimeStamp;
    }

    /**
     * The wall clock time at the point when this instance was created.
     *
     * @return The wall clock time
     */
    public Instant getLogTime() {
        return logTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(PROCESSED_RECORD_COUNT.getPreferredName(), processedRecordCount);
        builder.field(PROCESSED_FIELD_COUNT.getPreferredName(), processedFieldCount);
        builder.field(INPUT_BYTES.getPreferredName(), inputBytes);
        builder.field(INPUT_FIELD_COUNT.getPreferredName(), inputFieldCount);
        builder.field(INVALID_DATE_COUNT.getPreferredName(), invalidDateCount);
        builder.field(MISSING_FIELD_COUNT.getPreferredName(), missingFieldCount);
        builder.field(OUT_OF_ORDER_TIME_COUNT.getPreferredName(), outOfOrderTimeStampCount);
        builder.field(EMPTY_BUCKET_COUNT.getPreferredName(), emptyBucketCount);
        builder.field(SPARSE_BUCKET_COUNT.getPreferredName(), sparseBucketCount);
        builder.field(BUCKET_COUNT.getPreferredName(), bucketCount);
        if (earliestRecordTimeStamp != null) {
            builder.timeField(EARLIEST_RECORD_TIME.getPreferredName(), EARLIEST_RECORD_TIME.getPreferredName() + "_string",
                earliestRecordTimeStamp.getTime());
        }
        if (latestRecordTimeStamp != null) {
            builder.timeField(LATEST_RECORD_TIME.getPreferredName(), LATEST_RECORD_TIME.getPreferredName() + "_string",
                latestRecordTimeStamp.getTime());
        }
        if (lastDataTimeStamp != null) {
            builder.timeField(LAST_DATA_TIME.getPreferredName(), LAST_DATA_TIME.getPreferredName() + "_string",
                lastDataTimeStamp.getTime());
        }
        if (latestEmptyBucketTimeStamp != null) {
            builder.timeField(LATEST_EMPTY_BUCKET_TIME.getPreferredName(), LATEST_EMPTY_BUCKET_TIME.getPreferredName() + "_string",
                latestEmptyBucketTimeStamp.getTime());
        }
        if (latestSparseBucketTimeStamp != null) {
            builder.timeField(LATEST_SPARSE_BUCKET_TIME.getPreferredName(), LATEST_SPARSE_BUCKET_TIME.getPreferredName() + "_string",
                latestSparseBucketTimeStamp.getTime());
        }
        builder.field(INPUT_RECORD_COUNT.getPreferredName(), getInputRecordCount());
        if (logTime != null) {
            builder.timeField(LOG_TIME.getPreferredName(), LOG_TIME.getPreferredName() + "_string", logTime.toEpochMilli());
        }

        builder.endObject();
        return builder;
    }

    /**
     * Equality test
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataCounts that = (DataCounts) other;

        return Objects.equals(this.jobId, that.jobId) &&
                this.processedRecordCount == that.processedRecordCount &&
                this.processedFieldCount == that.processedFieldCount &&
                this.inputBytes == that.inputBytes &&
                this.inputFieldCount == that.inputFieldCount &&
                this.invalidDateCount == that.invalidDateCount &&
                this.missingFieldCount == that.missingFieldCount &&
                this.outOfOrderTimeStampCount == that.outOfOrderTimeStampCount &&
                this.emptyBucketCount == that.emptyBucketCount &&
                this.sparseBucketCount == that.sparseBucketCount &&
                this.bucketCount == that.bucketCount &&
                Objects.equals(this.latestRecordTimeStamp, that.latestRecordTimeStamp) &&
                Objects.equals(this.earliestRecordTimeStamp, that.earliestRecordTimeStamp) &&
                Objects.equals(this.lastDataTimeStamp, that.lastDataTimeStamp) &&
                Objects.equals(this.latestEmptyBucketTimeStamp, that.latestEmptyBucketTimeStamp) &&
                Objects.equals(this.latestSparseBucketTimeStamp, that.latestSparseBucketTimeStamp) &&
                Objects.equals(this.logTime, that.logTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, processedRecordCount, processedFieldCount,
                inputBytes, inputFieldCount, invalidDateCount, missingFieldCount,
                outOfOrderTimeStampCount, lastDataTimeStamp, emptyBucketCount, sparseBucketCount, bucketCount,
                latestRecordTimeStamp, earliestRecordTimeStamp, latestEmptyBucketTimeStamp, latestSparseBucketTimeStamp, logTime);
    }
}
