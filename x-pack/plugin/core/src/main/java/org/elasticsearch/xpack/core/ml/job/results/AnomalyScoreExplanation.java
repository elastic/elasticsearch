/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class AnomalyScoreExplanation implements ToXContentObject, Writeable {
    public static final ParseField ANOMALY_SCORE_EXPLANATION = new ParseField("anomaly_score_explanation");

    public static final ParseField ANOMALY_TYPE = new ParseField("anomaly_type");
    public static final ParseField ANOMALY_LENGTH = new ParseField("anomaly_length");
    public static final ParseField SINGLE_BUCKET_IMPACT = new ParseField("single_bucket_impact");
    public static final ParseField MULTI_BUCKET_IMPACT = new ParseField("multi_bucket_impact");
    public static final ParseField ANOMALY_CHARACTERISTICS_IMPACT = new ParseField("anomaly_characteristics_impact");
    public static final ParseField LOWER_CONFIDENCE_BOUND = new ParseField("lower_confidence_bound");
    public static final ParseField TYPICAL_VALUE = new ParseField("typical_value");
    public static final ParseField UPPER_CONFIDENCE_BOUND = new ParseField("upper_confidence_bound");
    public static final ParseField HIGH_VARIANCE_PENALTY = new ParseField("high_variance_penalty");
    public static final ParseField INCOMPLETE_BUCKET_PENALTY = new ParseField("incomplete_bucket_penalty");
    public static final ParseField MULTIMODAL_DISTRIBUTION = new ParseField("multimodal_distribution");
    public static final ParseField BY_FIELD_FIRST_OCCURRENCE = new ParseField("by_field_first_occurrence");
    public static final ParseField BY_FIELD_RELATIVE_RARITY = new ParseField("by_field_relative_rarity");

    public static final ObjectParser<AnomalyScoreExplanation, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<AnomalyScoreExplanation, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<AnomalyScoreExplanation, Void> createParser(Boolean ignoreUnknownFields) {
        ObjectParser<AnomalyScoreExplanation, Void> parser = new ObjectParser<>(
            ANOMALY_SCORE_EXPLANATION.getPreferredName(),
            ignoreUnknownFields,
            AnomalyScoreExplanation::new
        );
        parser.declareString(AnomalyScoreExplanation::setAnomalyType, ANOMALY_TYPE);
        parser.declareInt(AnomalyScoreExplanation::setAnomalyLength, ANOMALY_LENGTH);
        parser.declareInt(AnomalyScoreExplanation::setSingleBucketImpact, SINGLE_BUCKET_IMPACT);
        parser.declareInt(AnomalyScoreExplanation::setMultiBucketImpact, MULTI_BUCKET_IMPACT);
        parser.declareInt(AnomalyScoreExplanation::setAnomalyCharacteristicsImpact, ANOMALY_CHARACTERISTICS_IMPACT);
        parser.declareDouble(AnomalyScoreExplanation::setLowerConfidenceBound, LOWER_CONFIDENCE_BOUND);
        parser.declareDouble(AnomalyScoreExplanation::setTypicalValue, TYPICAL_VALUE);
        parser.declareDouble(AnomalyScoreExplanation::setUpperConfidenceBound, UPPER_CONFIDENCE_BOUND);
        parser.declareBoolean(AnomalyScoreExplanation::setHighVariancePenalty, HIGH_VARIANCE_PENALTY);
        parser.declareBoolean(AnomalyScoreExplanation::setIncompleteBucketPenalty, INCOMPLETE_BUCKET_PENALTY);
        parser.declareBoolean(AnomalyScoreExplanation::setMultimodalDistribution, MULTIMODAL_DISTRIBUTION);
        parser.declareBoolean(AnomalyScoreExplanation::setByFieldFirstOccurrence, BY_FIELD_FIRST_OCCURRENCE);
        parser.declareDouble(AnomalyScoreExplanation::setByFieldRelativeRarity, BY_FIELD_RELATIVE_RARITY);
        return parser;
    }

    private String anomalyType;
    private Integer anomalyLength;
    private Integer singleBucketImpact;
    private Integer multiBucketImpact;
    private Integer anomalyCharacteristicsImpact;
    private Double lowerConfidenceBound;
    private Double typicalValue;
    private Double upperConfidenceBound;
    private Boolean highVariancePenalty;
    private Boolean incompleteBucketPenalty;
    private Boolean multimodalDistribution;
    private Boolean byFieldFirstOccurrence;
    private Double byFieldRelativeRarity;

    AnomalyScoreExplanation() {}

    public AnomalyScoreExplanation(StreamInput in) throws IOException {
        this.anomalyType = in.readOptionalString();
        this.anomalyLength = in.readOptionalInt();
        this.singleBucketImpact = in.readOptionalInt();
        this.multiBucketImpact = in.readOptionalInt();
        this.anomalyCharacteristicsImpact = in.readOptionalInt();
        this.lowerConfidenceBound = in.readOptionalDouble();
        this.typicalValue = in.readOptionalDouble();
        this.upperConfidenceBound = in.readOptionalDouble();
        this.highVariancePenalty = in.readOptionalBoolean();
        this.incompleteBucketPenalty = in.readOptionalBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            this.multimodalDistribution = in.readOptionalBoolean();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            this.byFieldFirstOccurrence = in.readOptionalBoolean();
            this.byFieldRelativeRarity = in.readOptionalDouble();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(anomalyType);
        out.writeOptionalInt(anomalyLength);
        out.writeOptionalInt(singleBucketImpact);
        out.writeOptionalInt(multiBucketImpact);
        out.writeOptionalInt(anomalyCharacteristicsImpact);
        out.writeOptionalDouble(lowerConfidenceBound);
        out.writeOptionalDouble(typicalValue);
        out.writeOptionalDouble(upperConfidenceBound);
        out.writeOptionalBoolean(highVariancePenalty);
        out.writeOptionalBoolean(incompleteBucketPenalty);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            out.writeOptionalBoolean(multimodalDistribution);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeOptionalBoolean(byFieldFirstOccurrence);
            out.writeOptionalDouble(byFieldRelativeRarity);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (anomalyType != null) {
            builder.field(ANOMALY_TYPE.getPreferredName(), anomalyType);
        }
        if (anomalyLength != null) {
            builder.field(ANOMALY_LENGTH.getPreferredName(), anomalyLength);
        }
        if (singleBucketImpact != null) {
            builder.field(SINGLE_BUCKET_IMPACT.getPreferredName(), singleBucketImpact);
        }
        if (multiBucketImpact != null) {
            builder.field(MULTI_BUCKET_IMPACT.getPreferredName(), multiBucketImpact);
        }
        if (anomalyCharacteristicsImpact != null) {
            builder.field(ANOMALY_CHARACTERISTICS_IMPACT.getPreferredName(), anomalyCharacteristicsImpact);
        }
        if (lowerConfidenceBound != null) {
            builder.field(LOWER_CONFIDENCE_BOUND.getPreferredName(), lowerConfidenceBound);
        }
        if (typicalValue != null) {
            builder.field(TYPICAL_VALUE.getPreferredName(), typicalValue);
        }
        if (upperConfidenceBound != null) {
            builder.field(UPPER_CONFIDENCE_BOUND.getPreferredName(), upperConfidenceBound);
        }
        if (highVariancePenalty != null) {
            builder.field(HIGH_VARIANCE_PENALTY.getPreferredName(), highVariancePenalty);
        }
        if (incompleteBucketPenalty != null) {
            builder.field(INCOMPLETE_BUCKET_PENALTY.getPreferredName(), incompleteBucketPenalty);
        }
        if (multimodalDistribution != null) {
            builder.field(MULTIMODAL_DISTRIBUTION.getPreferredName(), multimodalDistribution);
        }
        if (byFieldFirstOccurrence != null) {
            builder.field(BY_FIELD_FIRST_OCCURRENCE.getPreferredName(), byFieldFirstOccurrence);
        }
        if (byFieldRelativeRarity != null) {
            builder.field(BY_FIELD_RELATIVE_RARITY.getPreferredName(), byFieldRelativeRarity);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            anomalyType,
            anomalyLength,
            singleBucketImpact,
            multiBucketImpact,
            anomalyCharacteristicsImpact,
            lowerConfidenceBound,
            typicalValue,
            upperConfidenceBound,
            highVariancePenalty,
            incompleteBucketPenalty,
            multimodalDistribution,
            byFieldFirstOccurrence,
            byFieldRelativeRarity
        );
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        AnomalyScoreExplanation that = (AnomalyScoreExplanation) other;
        return Objects.equals(this.anomalyType, that.anomalyType)
            && Objects.equals(this.anomalyLength, that.anomalyLength)
            && Objects.equals(this.singleBucketImpact, that.singleBucketImpact)
            && Objects.equals(this.multiBucketImpact, that.multiBucketImpact)
            && Objects.equals(this.anomalyCharacteristicsImpact, that.anomalyCharacteristicsImpact)
            && Objects.equals(this.lowerConfidenceBound, that.lowerConfidenceBound)
            && Objects.equals(this.typicalValue, that.typicalValue)
            && Objects.equals(this.upperConfidenceBound, that.upperConfidenceBound)
            && Objects.equals(this.highVariancePenalty, that.highVariancePenalty)
            && Objects.equals(this.incompleteBucketPenalty, that.incompleteBucketPenalty)
            && Objects.equals(this.multimodalDistribution, that.multimodalDistribution)
            && Objects.equals(this.byFieldFirstOccurrence, that.byFieldFirstOccurrence)
            && Objects.equals(this.byFieldRelativeRarity, that.byFieldRelativeRarity);
    }

    public String getAnomalyType() {
        return anomalyType;
    }

    public void setAnomalyType(String anomalyType) {
        this.anomalyType = anomalyType;
    }

    public Integer getAnomalyLength() {
        return anomalyLength;
    }

    public void setAnomalyLength(Integer anomalyLength) {
        this.anomalyLength = anomalyLength;
    }

    public Integer getSingleBucketImpact() {
        return singleBucketImpact;
    }

    public void setSingleBucketImpact(Integer singleBucketImpact) {
        this.singleBucketImpact = singleBucketImpact;
    }

    public Integer getMultiBucketImpact() {
        return multiBucketImpact;
    }

    public void setMultiBucketImpact(Integer multiBucketImpact) {
        this.multiBucketImpact = multiBucketImpact;
    }

    public Integer getAnomalyCharacteristicsImpact() {
        return anomalyCharacteristicsImpact;
    }

    public void setAnomalyCharacteristicsImpact(Integer anomalyCharacteristicsImpact) {
        this.anomalyCharacteristicsImpact = anomalyCharacteristicsImpact;
    }

    public Double getLowerConfidenceBound() {
        return lowerConfidenceBound;
    }

    public void setLowerConfidenceBound(Double lowerConfidenceBound) {
        this.lowerConfidenceBound = lowerConfidenceBound;
    }

    public Double getTypicalValue() {
        return typicalValue;
    }

    public void setTypicalValue(Double typicalValue) {
        this.typicalValue = typicalValue;
    }

    public Double getUpperConfidenceBound() {
        return upperConfidenceBound;
    }

    public void setUpperConfidenceBound(Double upperConfidenceBound) {
        this.upperConfidenceBound = upperConfidenceBound;
    }

    public Boolean isHighVariancePenalty() {
        return highVariancePenalty;
    }

    public void setHighVariancePenalty(Boolean highVariancePenalty) {
        this.highVariancePenalty = highVariancePenalty;
    }

    public Boolean isIncompleteBucketPenalty() {
        return incompleteBucketPenalty;
    }

    public void setIncompleteBucketPenalty(Boolean incompleteBucketPenalty) {
        this.incompleteBucketPenalty = incompleteBucketPenalty;
    }

    public Boolean isMultimodalDistribution() {
        return multimodalDistribution;
    }

    public void setMultimodalDistribution(Boolean multimodalDistribution) {
        this.multimodalDistribution = multimodalDistribution;
    }

    public Boolean isByFieldFirstOccurrence() {
        return byFieldFirstOccurrence;
    }

    public void setByFieldFirstOccurrence(Boolean byFieldFirstOccurrence) {
        this.byFieldFirstOccurrence = byFieldFirstOccurrence;
    }

    public Double getByFieldRelativeRarity() {
        return byFieldRelativeRarity;
    }

    public void setByFieldRelativeRarity(Double byFieldRelativeRarity) {
        this.byFieldRelativeRarity = byFieldRelativeRarity;
    }

}
