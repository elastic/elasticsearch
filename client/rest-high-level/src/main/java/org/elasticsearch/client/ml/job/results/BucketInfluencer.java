/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class BucketInfluencer implements ToXContentObject {

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

    public static final ConstructingObjectParser<BucketInfluencer, Void> PARSER =
        new ConstructingObjectParser<>(RESULT_TYPE_FIELD.getPreferredName(), true,
            a -> new BucketInfluencer((String) a[0], (Date) a[1], (long) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p) -> TimeUtil.parseTimeField(p, Result.TIMESTAMP.getPreferredName()),
                Result.TIMESTAMP, ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        PARSER.declareString((bucketInfluencer, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareString(BucketInfluencer::setInfluencerFieldName, INFLUENCER_FIELD_NAME);
        PARSER.declareDouble(BucketInfluencer::setInitialAnomalyScore, INITIAL_ANOMALY_SCORE);
        PARSER.declareDouble(BucketInfluencer::setAnomalyScore, ANOMALY_SCORE);
        PARSER.declareDouble(BucketInfluencer::setRawAnomalyScore, RAW_ANOMALY_SCORE);
        PARSER.declareDouble(BucketInfluencer::setProbability, PROBABILITY);
        PARSER.declareBoolean(BucketInfluencer::setIsInterim, Result.IS_INTERIM);
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

    BucketInfluencer(String jobId, Date timestamp, long bucketSpan) {
        this.jobId = jobId;
        this.timestamp = Objects.requireNonNull(timestamp);
        this.bucketSpan = bucketSpan;
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
        builder.timeField(Result.TIMESTAMP.getPreferredName(), Result.TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(Result.IS_INTERIM.getPreferredName(), isInterim);
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public double getProbability() {
        return probability;
    }

    void setProbability(double probability) {
        this.probability = probability;
    }

    public String getInfluencerFieldName() {
        return influenceField;
    }

    void setInfluencerFieldName(String fieldName) {
        this.influenceField = fieldName;
    }

    public double getInitialAnomalyScore() {
        return initialAnomalyScore;
    }

    void setInitialAnomalyScore(double influenceScore) {
        this.initialAnomalyScore = influenceScore;
    }

    public double getAnomalyScore() {
        return anomalyScore;
    }

    void setAnomalyScore(double score) {
        anomalyScore = score;
    }

    public double getRawAnomalyScore() {
        return rawAnomalyScore;
    }

    void setRawAnomalyScore(double score) {
        rawAnomalyScore = score;
    }

    void setIsInterim(boolean isInterim) {
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
