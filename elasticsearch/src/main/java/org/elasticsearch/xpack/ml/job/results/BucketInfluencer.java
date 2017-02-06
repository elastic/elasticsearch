/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

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
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class BucketInfluencer extends ToXContentToBytes implements Writeable {
    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "bucket_influencer";
    public static final ParseField RESULT_TYPE_FIELD = new ParseField(RESULT_TYPE_VALUE);

    /**
     * Field names
     */
    public static final ParseField INFLUENCER_FIELD_NAME = new ParseField("influencer_field_name");
    public static final ParseField INITIAL_ANOMALY_SCORE = new ParseField("initial_anomaly_score");
    public static final ParseField ANOMALY_SCORE = new ParseField("anomaly_score");
    public static final ParseField RAW_ANOMALY_SCORE = new ParseField("raw_anomaly_score");
    public static final ParseField PROBABILITY = new ParseField("probability");
    public static final ParseField IS_INTERIM = new ParseField("is_interim");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField SEQUENCE_NUM = new ParseField("sequence_num");

    /**
     * The influencer field name used for time influencers
     */
    public static final String BUCKET_TIME = "bucket_time";

    public static final ConstructingObjectParser<BucketInfluencer, Void> PARSER =
            new ConstructingObjectParser<>(RESULT_TYPE_FIELD.getPreferredName(), a -> new BucketInfluencer((String) a[0],
                    (Date) a[1], (long) a[2], (int) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == Token.VALUE_NUMBER) {
                return new Date(p.longValue());
            } else if (p.currentToken() == Token.VALUE_STRING) {
                return new Date(TimeUtils.dateStringToEpoch(p.text()));
            }
            throw new IllegalArgumentException("unexpected token [" + p.currentToken() + "] for [" + TIMESTAMP.getPreferredName() + "]");
        }, TIMESTAMP, ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SEQUENCE_NUM);
        PARSER.declareString((bucketInfluencer, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareString(BucketInfluencer::setInfluencerFieldName, INFLUENCER_FIELD_NAME);
        PARSER.declareDouble(BucketInfluencer::setInitialAnomalyScore, INITIAL_ANOMALY_SCORE);
        PARSER.declareDouble(BucketInfluencer::setAnomalyScore, ANOMALY_SCORE);
        PARSER.declareDouble(BucketInfluencer::setRawAnomalyScore, RAW_ANOMALY_SCORE);
        PARSER.declareDouble(BucketInfluencer::setProbability, PROBABILITY);
        PARSER.declareBoolean(BucketInfluencer::setIsInterim, IS_INTERIM);
    }

    private final String jobId;
    private String influenceField;
    private double initialAnomalyScore;
    private double anomalyScore;
    private double rawAnomalyScore;
    private double probability;
    private boolean isInterim;
    private final Date timestamp;
    private final long bucketSpan;
    private final int sequenceNum;

    public BucketInfluencer(String jobId, Date timestamp, long bucketSpan, int sequenceNum) {
        this.jobId = jobId;
        this.timestamp = ExceptionsHelper.requireNonNull(timestamp, TIMESTAMP.getPreferredName());
        this.bucketSpan = bucketSpan;
        this.sequenceNum = sequenceNum;
    }

    public BucketInfluencer(StreamInput in) throws IOException {
        jobId = in.readString();
        influenceField = in.readOptionalString();
        initialAnomalyScore = in.readDouble();
        anomalyScore = in.readDouble();
        rawAnomalyScore = in.readDouble();
        probability = in.readDouble();
        isInterim = in.readBoolean();
        timestamp = new Date(in.readLong());
        bucketSpan = in.readLong();
        sequenceNum = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeOptionalString(influenceField);
        out.writeDouble(initialAnomalyScore);
        out.writeDouble(anomalyScore);
        out.writeDouble(rawAnomalyScore);
        out.writeDouble(probability);
        out.writeBoolean(isInterim);
        out.writeLong(timestamp.getTime());
        out.writeLong(bucketSpan);
        out.writeInt(sequenceNum);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        if (influenceField != null) {
            builder.field(INFLUENCER_FIELD_NAME.getPreferredName(), influenceField);
        }
        builder.field(INITIAL_ANOMALY_SCORE.getPreferredName(), initialAnomalyScore);
        builder.field(ANOMALY_SCORE.getPreferredName(), anomalyScore);
        builder.field(RAW_ANOMALY_SCORE.getPreferredName(), rawAnomalyScore);
        builder.field(PROBABILITY.getPreferredName(), probability);
        builder.dateField(TIMESTAMP.getPreferredName(), TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(SEQUENCE_NUM.getPreferredName(), sequenceNum);
        builder.field(IS_INTERIM.getPreferredName(), isInterim);
        builder.endObject();
        return builder;
    }

    /**
     * Data store ID of this bucket influencer.
     */
    public String getId() {
        return jobId + "_" + timestamp.getTime() + "_" + bucketSpan + "_" + sequenceNum;
    }

    public String getJobId() {
        return jobId;
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

    public String getInfluencerFieldName() {
        return influenceField;
    }

    public void setInfluencerFieldName(String fieldName) {
        this.influenceField = fieldName;
    }

    public double getInitialAnomalyScore() {
        return initialAnomalyScore;
    }

    public void setInitialAnomalyScore(double influenceScore) {
        this.initialAnomalyScore = influenceScore;
    }

    public double getAnomalyScore() {
        return anomalyScore;
    }

    public void setAnomalyScore(double score) {
        anomalyScore = score;
    }

    public double getRawAnomalyScore() {
        return rawAnomalyScore;
    }

    public void setRawAnomalyScore(double score) {
        rawAnomalyScore = score;
    }

    public void setIsInterim(boolean isInterim) {
        this.isInterim = isInterim;
    }

    public boolean isInterim() {
        return isInterim;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(influenceField, initialAnomalyScore, anomalyScore, rawAnomalyScore, probability, isInterim, timestamp, jobId,
                bucketSpan, sequenceNum);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        BucketInfluencer other = (BucketInfluencer) obj;

        return Objects.equals(influenceField, other.influenceField) && Double.compare(initialAnomalyScore, other.initialAnomalyScore) == 0
                && Double.compare(anomalyScore, other.anomalyScore) == 0 && Double.compare(rawAnomalyScore, other.rawAnomalyScore) == 0
                && Double.compare(probability, other.probability) == 0 && Objects.equals(isInterim, other.isInterim)
                && Objects.equals(timestamp, other.timestamp) && Objects.equals(jobId, other.jobId) && bucketSpan == other.bucketSpan
                && sequenceNum == other.sequenceNum;
    }
}
