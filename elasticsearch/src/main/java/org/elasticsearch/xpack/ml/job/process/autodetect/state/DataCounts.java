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
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

/**
 * Job processed record counts.
 * <p>
 * The getInput... methods return the actual number of
 * fields/records sent the the API including invalid records.
 * The getProcessed... methods are the number sent to the
 * Engine.
 * <p>
 * The <code>inputRecordCount</code> field is calculated so it
 * should not be set in deserialisation but it should be serialised
 * so the field is visible.
 */

public class DataCounts extends ToXContentToBytes implements Writeable {

    private static final String DOCUMENT_SUFFIX = "-data-counts";
    public static final String PROCESSED_RECORD_COUNT_STR = "processed_record_count";
    public static final String PROCESSED_FIELD_COUNT_STR = "processed_field_count";
    public static final String INPUT_BYTES_STR = "input_bytes";
    public static final String INPUT_RECORD_COUNT_STR = "input_record_count";
    public static final String INPUT_FIELD_COUNT_STR = "input_field_count";
    public static final String INVALID_DATE_COUNT_STR = "invalid_date_count";
    public static final String MISSING_FIELD_COUNT_STR = "missing_field_count";
    public static final String OUT_OF_ORDER_TIME_COUNT_STR = "out_of_order_timestamp_count";
    public static final String EARLIEST_RECORD_TIME_STR = "earliest_record_timestamp";
    public static final String LATEST_RECORD_TIME_STR = "latest_record_timestamp";

    public static final ParseField PROCESSED_RECORD_COUNT = new ParseField(PROCESSED_RECORD_COUNT_STR);
    public static final ParseField PROCESSED_FIELD_COUNT = new ParseField(PROCESSED_FIELD_COUNT_STR);
    public static final ParseField INPUT_BYTES = new ParseField(INPUT_BYTES_STR);
    public static final ParseField INPUT_RECORD_COUNT = new ParseField(INPUT_RECORD_COUNT_STR);
    public static final ParseField INPUT_FIELD_COUNT = new ParseField(INPUT_FIELD_COUNT_STR);
    public static final ParseField INVALID_DATE_COUNT = new ParseField(INVALID_DATE_COUNT_STR);
    public static final ParseField MISSING_FIELD_COUNT = new ParseField(MISSING_FIELD_COUNT_STR);
    public static final ParseField OUT_OF_ORDER_TIME_COUNT = new ParseField(OUT_OF_ORDER_TIME_COUNT_STR);
    public static final ParseField EARLIEST_RECORD_TIME = new ParseField(EARLIEST_RECORD_TIME_STR);
    public static final ParseField LATEST_RECORD_TIME = new ParseField(LATEST_RECORD_TIME_STR);

    public static final ParseField TYPE = new ParseField("data_counts");

    public static final ConstructingObjectParser<DataCounts, Void> PARSER =
            new ConstructingObjectParser<>("data_counts", a -> new DataCounts((String) a[0], (long) a[1], (long) a[2], (long) a[3],
                    (long) a[4], (long) a[5], (long) a[6], (long) a[7], (Date) a[8], (Date) a[9]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), PROCESSED_RECORD_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), PROCESSED_FIELD_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INPUT_BYTES);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INPUT_FIELD_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INVALID_DATE_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MISSING_FIELD_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), OUT_OF_ORDER_TIME_COUNT);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == Token.VALUE_NUMBER) {
                return new Date(p.longValue());
            } else if (p.currentToken() == Token.VALUE_STRING) {
                return new Date(TimeUtils.dateStringToEpoch(p.text()));
            }
            throw new IllegalArgumentException(
                    "unexpected token [" + p.currentToken() + "] for [" + EARLIEST_RECORD_TIME.getPreferredName() + "]");
        }, EARLIEST_RECORD_TIME, ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == Token.VALUE_NUMBER) {
                return new Date(p.longValue());
            } else if (p.currentToken() == Token.VALUE_STRING) {
                return new Date(TimeUtils.dateStringToEpoch(p.text()));
            }
            throw new IllegalArgumentException(
                    "unexpected token [" + p.currentToken() + "] for [" + LATEST_RECORD_TIME.getPreferredName() + "]");
        }, LATEST_RECORD_TIME, ValueType.VALUE);
        PARSER.declareLong((t, u) -> {;}, INPUT_RECORD_COUNT);
    }

    public static String documentId(String jobId) {
        return jobId + DOCUMENT_SUFFIX;
    }

    private final String jobId;
    private long processedRecordCount;
    private long processedFieldCount;
    private long inputBytes;
    private long inputFieldCount;
    private long invalidDateCount;
    private long missingFieldCount;
    private long outOfOrderTimeStampCount;
    // NORELEASE: Use Jodatime instead
    private Date earliestRecordTimeStamp;
    private Date latestRecordTimeStamp;

    public DataCounts(String jobId, long processedRecordCount, long processedFieldCount, long inputBytes,
                      long inputFieldCount, long invalidDateCount, long missingFieldCount, long outOfOrderTimeStampCount,
                      Date earliestRecordTimeStamp, Date latestRecordTimeStamp) {
        this.jobId = jobId;
        this.processedRecordCount = processedRecordCount;
        this.processedFieldCount = processedFieldCount;
        this.inputBytes = inputBytes;
        this.inputFieldCount = inputFieldCount;
        this.invalidDateCount = invalidDateCount;
        this.missingFieldCount = missingFieldCount;
        this.outOfOrderTimeStampCount = outOfOrderTimeStampCount;
        this.latestRecordTimeStamp = latestRecordTimeStamp;
        this.earliestRecordTimeStamp = earliestRecordTimeStamp;
    }

    public DataCounts(String jobId) {
        this.jobId = jobId;
    }

    public DataCounts(DataCounts lhs) {
        jobId = lhs.jobId;
        processedRecordCount = lhs.processedRecordCount;
        processedFieldCount = lhs.processedFieldCount;
        inputBytes = lhs.inputBytes;
        inputFieldCount = lhs.inputFieldCount;
        invalidDateCount = lhs.invalidDateCount;
        missingFieldCount = lhs.missingFieldCount;
        outOfOrderTimeStampCount = lhs.outOfOrderTimeStampCount;
        latestRecordTimeStamp = lhs.latestRecordTimeStamp;
        earliestRecordTimeStamp = lhs.earliestRecordTimeStamp;
    }

    public DataCounts(StreamInput in) throws IOException {
        jobId = in.readString();
        processedRecordCount = in.readVLong();
        processedFieldCount = in.readVLong();
        inputBytes = in.readVLong();
        inputFieldCount = in.readVLong();
        invalidDateCount = in.readVLong();
        missingFieldCount = in.readVLong();
        outOfOrderTimeStampCount = in.readVLong();
        if (in.readBoolean()) {
            latestRecordTimeStamp = new Date(in.readVLong());
        }
        if (in.readBoolean()) {
            earliestRecordTimeStamp = new Date(in.readVLong());
        }
        in.readVLong(); // throw away inputRecordCount
    }

    public String getJobid() {
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

    public void incrementProcessedRecordCount(long additional) {
        processedRecordCount += additional;
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

    public void calcProcessedFieldCount(long analysisFieldsPerRecord) {
        processedFieldCount =
                (processedRecordCount * analysisFieldsPerRecord)
                - missingFieldCount;

        // processedFieldCount could be a -ve value if no
        // records have been written in which case it should be 0
        processedFieldCount = (processedFieldCount < 0) ? 0 : processedFieldCount;
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

    public void incrementInputBytes(long additional) {
        inputBytes += additional;
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

    public void incrementInputFieldCount(long additional) {
        inputFieldCount += additional;
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

    public void incrementInvalidDateCount(long additional) {
        invalidDateCount += additional;
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

    public void incrementMissingFieldCount(long additional) {
        missingFieldCount += additional;
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

    public void incrementOutOfOrderTimeStampCount(long additional) {
        outOfOrderTimeStampCount += additional;
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
     * If {@code earliestRecordTimeStamp} has not been set (i.e. is {@code null})
     * then set it to {@code timeStamp}
     *
     * @param timeStamp Candidate time
     * @throws IllegalStateException if {@code earliestRecordTimeStamp} is already set
     */
    public void setEarliestRecordTimeStamp(Date timeStamp) {
        if (earliestRecordTimeStamp != null) {
            throw new IllegalStateException("earliestRecordTimeStamp can only be set once");
        }
        earliestRecordTimeStamp = timeStamp;
    }


    /**
     * The time of the latest record seen.
     *
     * @return Latest record time
     */
    public Date getLatestRecordTimeStamp() {
        return latestRecordTimeStamp;
    }

    public void setLatestRecordTimeStamp(Date latestRecordTimeStamp) {
        this.latestRecordTimeStamp = latestRecordTimeStamp;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeVLong(processedRecordCount);
        out.writeVLong(processedFieldCount);
        out.writeVLong(inputBytes);
        out.writeVLong(inputFieldCount);
        out.writeVLong(invalidDateCount);
        out.writeVLong(missingFieldCount);
        out.writeVLong(outOfOrderTimeStampCount);
        if (latestRecordTimeStamp != null) {
            out.writeBoolean(true);
            out.writeVLong(latestRecordTimeStamp.getTime());
        } else {
            out.writeBoolean(false);
        }
        if (earliestRecordTimeStamp != null) {
            out.writeBoolean(true);
            out.writeVLong(earliestRecordTimeStamp.getTime());
        } else {
            out.writeBoolean(false);
        }
        out.writeVLong(getInputRecordCount());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(PROCESSED_RECORD_COUNT.getPreferredName(), processedRecordCount);
        builder.field(PROCESSED_FIELD_COUNT.getPreferredName(), processedFieldCount);
        builder.field(INPUT_BYTES.getPreferredName(), inputBytes);
        builder.field(INPUT_FIELD_COUNT.getPreferredName(), inputFieldCount);
        builder.field(INVALID_DATE_COUNT.getPreferredName(), invalidDateCount);
        builder.field(MISSING_FIELD_COUNT.getPreferredName(), missingFieldCount);
        builder.field(OUT_OF_ORDER_TIME_COUNT.getPreferredName(), outOfOrderTimeStampCount);
        if (earliestRecordTimeStamp != null) {
            builder.field(EARLIEST_RECORD_TIME.getPreferredName(), earliestRecordTimeStamp.getTime());
        }
        if (latestRecordTimeStamp != null) {
            builder.field(LATEST_RECORD_TIME.getPreferredName(), latestRecordTimeStamp.getTime());
        }
        builder.field(INPUT_RECORD_COUNT.getPreferredName(), getInputRecordCount());

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

        if (other instanceof DataCounts == false) {
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
                Objects.equals(this.latestRecordTimeStamp, that.latestRecordTimeStamp) &&
                Objects.equals(this.earliestRecordTimeStamp, that.earliestRecordTimeStamp);

    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, processedRecordCount, processedFieldCount,
                inputBytes, inputFieldCount, invalidDateCount, missingFieldCount,
                outOfOrderTimeStampCount, latestRecordTimeStamp, earliestRecordTimeStamp);
    }
}
