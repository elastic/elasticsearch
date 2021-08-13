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

public class Influencer implements ToXContentObject {

    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "influencer";
    public static final ParseField RESULT_TYPE_FIELD = new ParseField(RESULT_TYPE_VALUE);

    /*
     * Field names
     */
    public static final ParseField PROBABILITY = new ParseField("probability");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField INFLUENCER_FIELD_NAME = new ParseField("influencer_field_name");
    public static final ParseField INFLUENCER_FIELD_VALUE = new ParseField("influencer_field_value");
    public static final ParseField INITIAL_INFLUENCER_SCORE = new ParseField("initial_influencer_score");
    public static final ParseField INFLUENCER_SCORE = new ParseField("influencer_score");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("influencers");

    public static final ConstructingObjectParser<Influencer, Void> PARSER = new ConstructingObjectParser<>(
            RESULT_TYPE_FIELD.getPreferredName(), true,
            a -> new Influencer((String) a[0], (String) a[1], (String) a[2], (Date) a[3], (long) a[4]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INFLUENCER_FIELD_NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INFLUENCER_FIELD_VALUE);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p) -> TimeUtil.parseTimeField(p, Result.TIMESTAMP.getPreferredName()),
                Result.TIMESTAMP, ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        PARSER.declareString((influencer, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareDouble(Influencer::setProbability, PROBABILITY);
        PARSER.declareDouble(Influencer::setInfluencerScore, INFLUENCER_SCORE);
        PARSER.declareDouble(Influencer::setInitialInfluencerScore, INITIAL_INFLUENCER_SCORE);
        PARSER.declareBoolean(Influencer::setInterim, Result.IS_INTERIM);
    }

    private final String jobId;
    private final Date timestamp;
    private final long bucketSpan;
    private String influenceField;
    private String influenceValue;
    private double probability;
    private double initialInfluencerScore;
    private double influencerScore;
    private boolean isInterim;

    Influencer(String jobId, String fieldName, String fieldValue, Date timestamp, long bucketSpan) {
        this.jobId = jobId;
        influenceField = fieldName;
        influenceValue = fieldValue;
        this.timestamp = Objects.requireNonNull(timestamp);
        this.bucketSpan = bucketSpan;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(INFLUENCER_FIELD_NAME.getPreferredName(), influenceField);
        builder.field(INFLUENCER_FIELD_VALUE.getPreferredName(), influenceValue);
        builder.field(INFLUENCER_SCORE.getPreferredName(), influencerScore);
        builder.field(INITIAL_INFLUENCER_SCORE.getPreferredName(), initialInfluencerScore);
        builder.field(PROBABILITY.getPreferredName(), probability);
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(Result.IS_INTERIM.getPreferredName(), isInterim);
        builder.timeField(Result.TIMESTAMP.getPreferredName(), Result.TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
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

    public Date getTimestamp() {
        return timestamp;
    }

    public String getInfluencerFieldName() {
        return influenceField;
    }

    public String getInfluencerFieldValue() {
        return influenceValue;
    }

    public double getInitialInfluencerScore() {
        return initialInfluencerScore;
    }

    void setInitialInfluencerScore(double score) {
        initialInfluencerScore = score;
    }

    public double getInfluencerScore() {
        return influencerScore;
    }

    void setInfluencerScore(double score) {
        influencerScore = score;
    }

    public boolean isInterim() {
        return isInterim;
    }

    void setInterim(boolean value) {
        isInterim = value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, influenceField, influenceValue, initialInfluencerScore,
                influencerScore, probability, isInterim, bucketSpan);
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

        Influencer other = (Influencer) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(timestamp, other.timestamp)
                && Objects.equals(influenceField, other.influenceField)
                && Objects.equals(influenceValue, other.influenceValue)
                && Double.compare(initialInfluencerScore, other.initialInfluencerScore) == 0
                && Double.compare(influencerScore, other.influencerScore) == 0 && Double.compare(probability, other.probability) == 0
                && (isInterim == other.isInterim) && (bucketSpan == other.bucketSpan);
    }
}
