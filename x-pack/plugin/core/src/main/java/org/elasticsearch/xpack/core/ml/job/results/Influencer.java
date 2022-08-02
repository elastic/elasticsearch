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
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class Influencer implements ToXContentObject, Writeable {
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

    // Influencers contain data fields, thus we always parse them leniently
    public static final ConstructingObjectParser<Influencer, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
        RESULT_TYPE_FIELD.getPreferredName(),
        true,
        a -> new Influencer((String) a[0], (String) a[1], (String) a[2], (Date) a[3], (long) a[4])
    );

    static {
        LENIENT_PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        LENIENT_PARSER.declareString(ConstructingObjectParser.constructorArg(), INFLUENCER_FIELD_NAME);
        LENIENT_PARSER.declareString(ConstructingObjectParser.constructorArg(), INFLUENCER_FIELD_VALUE);
        LENIENT_PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeField(p, Result.TIMESTAMP.getPreferredName()),
            Result.TIMESTAMP,
            ValueType.VALUE
        );
        LENIENT_PARSER.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        LENIENT_PARSER.declareString((influencer, s) -> {}, Result.RESULT_TYPE);
        LENIENT_PARSER.declareDouble(Influencer::setProbability, PROBABILITY);
        LENIENT_PARSER.declareDouble(Influencer::setInfluencerScore, INFLUENCER_SCORE);
        LENIENT_PARSER.declareDouble(Influencer::setInitialInfluencerScore, INITIAL_INFLUENCER_SCORE);
        LENIENT_PARSER.declareBoolean(Influencer::setInterim, Result.IS_INTERIM);
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

    public Influencer(String jobId, String fieldName, String fieldValue, Date timestamp, long bucketSpan) {
        this.jobId = jobId;
        influenceField = fieldName;
        influenceValue = fieldValue;
        this.timestamp = ExceptionsHelper.requireNonNull(timestamp, Result.TIMESTAMP.getPreferredName());
        this.bucketSpan = bucketSpan;
    }

    public Influencer(StreamInput in) throws IOException {
        jobId = in.readString();
        timestamp = new Date(in.readLong());
        influenceField = in.readString();
        influenceValue = in.readString();
        probability = in.readDouble();
        initialInfluencerScore = in.readDouble();
        influencerScore = in.readDouble();
        isInterim = in.readBoolean();
        bucketSpan = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeLong(timestamp.getTime());
        out.writeString(influenceField);
        out.writeString(influenceValue);
        out.writeDouble(probability);
        out.writeDouble(initialInfluencerScore);
        out.writeDouble(influencerScore);
        out.writeBoolean(isInterim);
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
        builder.field(INFLUENCER_FIELD_NAME.getPreferredName(), influenceField);
        builder.field(INFLUENCER_FIELD_VALUE.getPreferredName(), influenceValue);
        if (ReservedFieldNames.isValidFieldName(influenceField)) {
            builder.field(influenceField, influenceValue);
        }
        builder.field(INFLUENCER_SCORE.getPreferredName(), influencerScore);
        builder.field(INITIAL_INFLUENCER_SCORE.getPreferredName(), initialInfluencerScore);
        builder.field(PROBABILITY.getPreferredName(), probability);
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(Result.IS_INTERIM.getPreferredName(), isInterim);
        builder.timeField(Result.TIMESTAMP.getPreferredName(), Result.TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public String getId() {
        return jobId
            + "_influencer_"
            + timestamp.getTime()
            + "_"
            + bucketSpan
            + "_"
            + influenceField
            + "_"
            + MachineLearningField.valuesToId(influenceValue);
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
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

    public void setInitialInfluencerScore(double score) {
        initialInfluencerScore = score;
    }

    public double getInfluencerScore() {
        return influencerScore;
    }

    public void setInfluencerScore(double score) {
        influencerScore = score;
    }

    public boolean isInterim() {
        return isInterim;
    }

    public void setInterim(boolean value) {
        isInterim = value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            timestamp,
            influenceField,
            influenceValue,
            initialInfluencerScore,
            influencerScore,
            probability,
            isInterim,
            bucketSpan
        );
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
        return Objects.equals(jobId, other.jobId)
            && Objects.equals(timestamp, other.timestamp)
            && Objects.equals(influenceField, other.influenceField)
            && Objects.equals(influenceValue, other.influenceValue)
            && Double.compare(initialInfluencerScore, other.initialInfluencerScore) == 0
            && Double.compare(influencerScore, other.influencerScore) == 0
            && Double.compare(probability, other.probability) == 0
            && (isInterim == other.isInterim)
            && (bucketSpan == other.bucketSpan);
    }
}
