/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class BucketInfluencer implements ToXContentObject, Writeable {
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
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");

    /**
     * The influencer field name used for time influencers
     */
    public static final String BUCKET_TIME = "bucket_time";

    public static final ConstructingObjectParser<BucketInfluencer, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<BucketInfluencer, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<BucketInfluencer, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<BucketInfluencer, Void> parser = new ConstructingObjectParser<>(RESULT_TYPE_FIELD.getPreferredName(),
                ignoreUnknownFields, a -> new BucketInfluencer((String) a[0], (Date) a[1], (long) a[2]));

        parser.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        parser.declareField(ConstructingObjectParser.constructorArg(),
                p -> TimeUtils.parseTimeField(p, Result.TIMESTAMP.getPreferredName()), Result.TIMESTAMP, ValueType.VALUE);
        parser.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        parser.declareString((bucketInfluencer, s) -> {}, Result.RESULT_TYPE);
        parser.declareString(BucketInfluencer::setInfluencerFieldName, INFLUENCER_FIELD_NAME);
        parser.declareDouble(BucketInfluencer::setInitialAnomalyScore, INITIAL_ANOMALY_SCORE);
        parser.declareDouble(BucketInfluencer::setAnomalyScore, ANOMALY_SCORE);
        parser.declareDouble(BucketInfluencer::setRawAnomalyScore, RAW_ANOMALY_SCORE);
        parser.declareDouble(BucketInfluencer::setProbability, PROBABILITY);
        parser.declareBoolean(BucketInfluencer::setIsInterim, Result.IS_INTERIM);

        return parser;
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

    public BucketInfluencer(String jobId, Date timestamp, long bucketSpan) {
        this.jobId = jobId;
        this.timestamp = ExceptionsHelper.requireNonNull(timestamp, Result.TIMESTAMP.getPreferredName());
        this.bucketSpan = bucketSpan;
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        if (influenceField != null) {
            builder.field(INFLUENCER_FIELD_NAME.getPreferredName(), influenceField);
        }
        builder.field(INITIAL_ANOMALY_SCORE.getPreferredName(), initialAnomalyScore);
        builder.field(ANOMALY_SCORE.getPreferredName(), anomalyScore);
        builder.field(RAW_ANOMALY_SCORE.getPreferredName(), rawAnomalyScore);
        builder.field(PROBABILITY.getPreferredName(), probability);
        builder.timeField(Result.TIMESTAMP.getPreferredName(), Result.TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(Result.IS_INTERIM.getPreferredName(), isInterim);
        return builder;
    }

    /**
     * Data store ID of this bucket influencer.
     */
    public String getId() {
        return jobId + "_bucket_influencer_" + timestamp.getTime() + "_" + bucketSpan
                + (influenceField == null ? "" :  "_" + influenceField);
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
                bucketSpan);
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
                && Objects.equals(timestamp, other.timestamp) && Objects.equals(jobId, other.jobId) && bucketSpan == other.bucketSpan;

    }
}
