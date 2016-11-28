/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.results;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.xpack.prelert.utils.time.TimeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Anomaly Record POJO.
 * Uses the object wrappers Boolean and Double so <code>null</code> values
 * can be returned if the members have not been set.
 */
public class AnomalyRecord extends ToXContentToBytes implements Writeable {

    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "record";
    /**
     * Result fields (all detector types)
     */
    public static final ParseField JOB_ID = new ParseField("jobId");
    public static final ParseField DETECTOR_INDEX = new ParseField("detectorIndex");
    public static final ParseField PROBABILITY = new ParseField("probability");
    public static final ParseField BY_FIELD_NAME = new ParseField("byFieldName");
    public static final ParseField BY_FIELD_VALUE = new ParseField("byFieldValue");
    public static final ParseField CORRELATED_BY_FIELD_VALUE = new ParseField("correlatedByFieldValue");
    public static final ParseField PARTITION_FIELD_NAME = new ParseField("partitionFieldName");
    public static final ParseField PARTITION_FIELD_VALUE = new ParseField("partitionFieldValue");
    public static final ParseField FUNCTION = new ParseField("function");
    public static final ParseField FUNCTION_DESCRIPTION = new ParseField("functionDescription");
    public static final ParseField TYPICAL = new ParseField("typical");
    public static final ParseField ACTUAL = new ParseField("actual");
    public static final ParseField IS_INTERIM = new ParseField("isInterim");
    public static final ParseField INFLUENCERS = new ParseField("influencers");
    public static final ParseField BUCKET_SPAN = new ParseField("bucketSpan");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("records");

    /**
     * Metric Results (including population metrics)
     */
    public static final ParseField FIELD_NAME = new ParseField("fieldName");

    /**
     * Population results
     */
    public static final ParseField OVER_FIELD_NAME = new ParseField("overFieldName");
    public static final ParseField OVER_FIELD_VALUE = new ParseField("overFieldValue");
    public static final ParseField CAUSES = new ParseField("causes");

    /**
     * Normalisation
     */
    public static final ParseField ANOMALY_SCORE = new ParseField("anomalyScore");
    public static final ParseField NORMALIZED_PROBABILITY = new ParseField("normalizedProbability");
    public static final ParseField INITIAL_NORMALIZED_PROBABILITY = new ParseField("initialNormalizedProbability");

    public static final ConstructingObjectParser<AnomalyRecord, ParseFieldMatcherSupplier> PARSER =
            new ConstructingObjectParser<>(RESULT_TYPE_VALUE, a -> new AnomalyRecord((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), JOB_ID);
        PARSER.declareString((anomalyRecord, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareDouble(AnomalyRecord::setProbability, PROBABILITY);
        PARSER.declareDouble(AnomalyRecord::setAnomalyScore, ANOMALY_SCORE);
        PARSER.declareDouble(AnomalyRecord::setNormalizedProbability, NORMALIZED_PROBABILITY);
        PARSER.declareDouble(AnomalyRecord::setInitialNormalizedProbability, INITIAL_NORMALIZED_PROBABILITY);
        PARSER.declareLong(AnomalyRecord::setBucketSpan, BUCKET_SPAN);
        PARSER.declareInt(AnomalyRecord::setDetectorIndex, DETECTOR_INDEX);
        PARSER.declareBoolean(AnomalyRecord::setInterim, IS_INTERIM);
        PARSER.declareField(AnomalyRecord::setTimestamp, p -> {
            if (p.currentToken() == Token.VALUE_NUMBER) {
                return new Date(p.longValue());
            } else if (p.currentToken() == Token.VALUE_STRING) {
                return new Date(TimeUtils.dateStringToEpoch(p.text()));
            }
            throw new IllegalArgumentException("unexpected token [" + p.currentToken() + "] for [" + TIMESTAMP.getPreferredName() + "]");
        }, TIMESTAMP, ValueType.VALUE);
        PARSER.declareString(AnomalyRecord::setByFieldName, BY_FIELD_NAME);
        PARSER.declareString(AnomalyRecord::setByFieldValue, BY_FIELD_VALUE);
        PARSER.declareString(AnomalyRecord::setCorrelatedByFieldValue, CORRELATED_BY_FIELD_VALUE);
        PARSER.declareString(AnomalyRecord::setPartitionFieldName, PARTITION_FIELD_NAME);
        PARSER.declareString(AnomalyRecord::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        PARSER.declareString(AnomalyRecord::setFunction, FUNCTION);
        PARSER.declareString(AnomalyRecord::setFunctionDescription, FUNCTION_DESCRIPTION);
        PARSER.declareDoubleArray(AnomalyRecord::setTypical, TYPICAL);
        PARSER.declareDoubleArray(AnomalyRecord::setActual, ACTUAL);
        PARSER.declareString(AnomalyRecord::setFieldName, FIELD_NAME);
        PARSER.declareString(AnomalyRecord::setOverFieldName, OVER_FIELD_NAME);
        PARSER.declareString(AnomalyRecord::setOverFieldValue, OVER_FIELD_VALUE);
        PARSER.declareObjectArray(AnomalyRecord::setCauses, AnomalyCause.PARSER, CAUSES);
        PARSER.declareObjectArray(AnomalyRecord::setInfluencers, Influence.PARSER, INFLUENCERS);
    }

    private final String jobId;
    private String id;
    private int detectorIndex;
    private double probability;
    private String byFieldName;
    private String byFieldValue;
    private String correlatedByFieldValue;
    private String partitionFieldName;
    private String partitionFieldValue;
    private String function;
    private String functionDescription;
    private List<Double> typical;
    private List<Double> actual;
    private boolean isInterim;

    private String fieldName;

    private String overFieldName;
    private String overFieldValue;
    private List<AnomalyCause> causes;

    private double anomalyScore;
    private double normalizedProbability;

    private double initialNormalizedProbability;

    private Date timestamp;
    private long bucketSpan;

    private List<Influence> influencers;

    private boolean hadBigNormalisedUpdate;

    public AnomalyRecord(String jobId) {
        this.jobId = jobId;
    }

    @SuppressWarnings("unchecked")
    public AnomalyRecord(StreamInput in) throws IOException {
        jobId = in.readString();
        id = in.readOptionalString();
        detectorIndex = in.readInt();
        probability = in.readDouble();
        byFieldName = in.readOptionalString();
        byFieldValue = in.readOptionalString();
        correlatedByFieldValue = in.readOptionalString();
        partitionFieldName = in.readOptionalString();
        partitionFieldValue = in.readOptionalString();
        function = in.readOptionalString();
        functionDescription = in.readOptionalString();
        fieldName = in.readOptionalString();
        overFieldName = in.readOptionalString();
        overFieldValue = in.readOptionalString();
        if (in.readBoolean()) {
            typical = (List<Double>) in.readGenericValue();
        }
        if (in.readBoolean()) {
            actual = (List<Double>) in.readGenericValue();
        }
        isInterim = in.readBoolean();
        if (in.readBoolean()) {
            causes = in.readList(AnomalyCause::new);
        }
        anomalyScore = in.readDouble();
        normalizedProbability = in.readDouble();
        initialNormalizedProbability = in.readDouble();
        if (in.readBoolean()) {
            timestamp = new Date(in.readLong());
        }
        bucketSpan = in.readLong();
        if (in.readBoolean()) {
            influencers = in.readList(Influence::new);
        }
        hadBigNormalisedUpdate = in.readBoolean();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeOptionalString(id);
        out.writeInt(detectorIndex);
        out.writeDouble(probability);
        out.writeOptionalString(byFieldName);
        out.writeOptionalString(byFieldValue);
        out.writeOptionalString(correlatedByFieldValue);
        out.writeOptionalString(partitionFieldName);
        out.writeOptionalString(partitionFieldValue);
        out.writeOptionalString(function);
        out.writeOptionalString(functionDescription);
        out.writeOptionalString(fieldName);
        out.writeOptionalString(overFieldName);
        out.writeOptionalString(overFieldValue);
        boolean hasTypical = typical != null;
        out.writeBoolean(hasTypical);
        if (hasTypical) {
            out.writeGenericValue(typical);
        }
        boolean hasActual = actual != null;
        out.writeBoolean(hasActual);
        if (hasActual) {
            out.writeGenericValue(actual);
        }
        out.writeBoolean(isInterim);
        boolean hasCauses = causes != null;
        out.writeBoolean(hasCauses);
        if (hasCauses) {
            out.writeList(causes);
        }
        out.writeDouble(anomalyScore);
        out.writeDouble(normalizedProbability);
        out.writeDouble(initialNormalizedProbability);
        boolean hasTimestamp = timestamp != null;
        out.writeBoolean(hasTimestamp);
        if (hasTimestamp) {
            out.writeLong(timestamp.getTime());
        }
        out.writeLong(bucketSpan);
        boolean hasInfluencers = influencers != null;
        out.writeBoolean(hasInfluencers);
        if (hasInfluencers) {
            out.writeList(influencers);
        }
        out.writeBoolean(hadBigNormalisedUpdate);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(JOB_ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(PROBABILITY.getPreferredName(), probability);
        builder.field(ANOMALY_SCORE.getPreferredName(), anomalyScore);
        builder.field(NORMALIZED_PROBABILITY.getPreferredName(), normalizedProbability);
        builder.field(INITIAL_NORMALIZED_PROBABILITY.getPreferredName(), initialNormalizedProbability);
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(DETECTOR_INDEX.getPreferredName(), detectorIndex);
        builder.field(IS_INTERIM.getPreferredName(), isInterim);
        if (timestamp != null) {
            builder.field(TIMESTAMP.getPreferredName(), timestamp.getTime());
        }
        if (byFieldName != null) {
            builder.field(BY_FIELD_NAME.getPreferredName(), byFieldName);
        }
        if (byFieldValue != null) {
            builder.field(BY_FIELD_VALUE.getPreferredName(), byFieldValue);
        }
        if (correlatedByFieldValue != null) {
            builder.field(CORRELATED_BY_FIELD_VALUE.getPreferredName(), correlatedByFieldValue);
        }
        if (partitionFieldName != null) {
            builder.field(PARTITION_FIELD_NAME.getPreferredName(), partitionFieldName);
        }
        if (partitionFieldValue != null) {
            builder.field(PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue);
        }
        if (function != null) {
            builder.field(FUNCTION.getPreferredName(), function);
        }
        if (functionDescription != null) {
            builder.field(FUNCTION_DESCRIPTION.getPreferredName(), functionDescription);
        }
        if (typical != null) {
            builder.field(TYPICAL.getPreferredName(), typical);
        }
        if (actual != null) {
            builder.field(ACTUAL.getPreferredName(), actual);
        }
        if (fieldName != null) {
            builder.field(FIELD_NAME.getPreferredName(), fieldName);
        }
        if (overFieldName != null) {
            builder.field(OVER_FIELD_NAME.getPreferredName(), overFieldName);
        }
        if (overFieldValue != null) {
            builder.field(OVER_FIELD_VALUE.getPreferredName(), overFieldValue);
        }
        if (causes != null) {
            builder.field(CAUSES.getPreferredName(), causes);
        }
        if (influencers != null) {
            builder.field(INFLUENCERS.getPreferredName(), influencers);
        }
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return this.jobId;
    }

    /**
     * Data store ID of this record.  May be null for records that have not been
     * read from the data store.
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getDetectorIndex() {
        return detectorIndex;
    }

    public void setDetectorIndex(int detectorIndex) {
        this.detectorIndex = detectorIndex;
    }

    public double getAnomalyScore() {
        return anomalyScore;
    }

    public void setAnomalyScore(double anomalyScore) {
        this.anomalyScore = anomalyScore;
    }

    public double getNormalizedProbability() {
        return normalizedProbability;
    }

    public void setNormalizedProbability(double normalizedProbability) {
        this.normalizedProbability = normalizedProbability;
    }

    public double getInitialNormalizedProbability() {
        return initialNormalizedProbability;
    }

    public void setInitialNormalizedProbability(double initialNormalizedProbability) {
        this.initialNormalizedProbability = initialNormalizedProbability;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Bucketspan expressed in seconds
     */
    public long getBucketSpan() {
        return bucketSpan;
    }

    /**
     * Bucketspan expressed in seconds
     */
    public void setBucketSpan(long bucketSpan) {
        this.bucketSpan = bucketSpan;
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double value) {
        probability = value;
    }


    public String getByFieldName() {
        return byFieldName;
    }

    public void setByFieldName(String value) {
        byFieldName = value.intern();
    }

    public String getByFieldValue() {
        return byFieldValue;
    }

    public void setByFieldValue(String value) {
        byFieldValue = value.intern();
    }

    public String getCorrelatedByFieldValue() {
        return correlatedByFieldValue;
    }

    public void setCorrelatedByFieldValue(String value) {
        correlatedByFieldValue = value.intern();
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    public void setPartitionFieldName(String field) {
        partitionFieldName = field.intern();
    }

    public String getPartitionFieldValue() {
        return partitionFieldValue;
    }

    public void setPartitionFieldValue(String value) {
        partitionFieldValue = value.intern();
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String name) {
        function = name.intern();
    }

    public String getFunctionDescription() {
        return functionDescription;
    }

    public void setFunctionDescription(String functionDescription) {
        this.functionDescription = functionDescription.intern();
    }

    public List<Double> getTypical() {
        return typical;
    }

    public void setTypical(List<Double> typical) {
        this.typical = typical;
    }

    public List<Double> getActual() {
        return actual;
    }

    public void setActual(List<Double> actual) {
        this.actual = actual;
    }

    public boolean isInterim() {
        return isInterim;
    }

    public void setInterim(boolean isInterim) {
        this.isInterim = isInterim;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String field) {
        fieldName = field.intern();
    }

    public String getOverFieldName() {
        return overFieldName;
    }

    public void setOverFieldName(String name) {
        overFieldName = name.intern();
    }

    public String getOverFieldValue() {
        return overFieldValue;
    }

    public void setOverFieldValue(String value) {
        overFieldValue = value.intern();
    }

    public List<AnomalyCause> getCauses() {
        return causes;
    }

    public void setCauses(List<AnomalyCause> causes) {
        this.causes = causes;
    }

    public void addCause(AnomalyCause cause) {
        if (causes == null) {
            causes = new ArrayList<>();
        }
        causes.add(cause);
    }

    public List<Influence> getInfluencers() {
        return influencers;
    }

    public void setInfluencers(List<Influence> influencers) {
        this.influencers = influencers;
    }


    @Override
    public int hashCode() {
        // ID is NOT included in the hash, so that a record from the data store
        // will hash the same as a record representing the same anomaly that did
        // not come from the data store

        // hadBigNormalisedUpdate is also deliberately excluded from the hash

        return Objects.hash(detectorIndex, probability, anomalyScore, initialNormalizedProbability,
                normalizedProbability, typical, actual,
                function, functionDescription, fieldName, byFieldName, byFieldValue, correlatedByFieldValue,
                partitionFieldName, partitionFieldValue, overFieldName, overFieldValue,
                timestamp, isInterim, causes, influencers, jobId, RESULT_TYPE_VALUE);
    }


    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof AnomalyRecord == false) {
            return false;
        }

        AnomalyRecord that = (AnomalyRecord) other;

        // ID is NOT compared, so that a record from the data store will compare
        // equal to a record representing the same anomaly that did not come
        // from the data store

        // hadBigNormalisedUpdate is also deliberately excluded from the test
        return Objects.equals(this.jobId, that.jobId)
                && this.detectorIndex == that.detectorIndex
                && this.probability == that.probability
                && this.anomalyScore == that.anomalyScore
                && this.normalizedProbability == that.normalizedProbability
                && this.initialNormalizedProbability == that.initialNormalizedProbability
                && Objects.deepEquals(this.typical, that.typical)
                && Objects.deepEquals(this.actual, that.actual)
                && Objects.equals(this.function, that.function)
                && Objects.equals(this.functionDescription, that.functionDescription)
                && Objects.equals(this.fieldName, that.fieldName)
                && Objects.equals(this.byFieldName, that.byFieldName)
                && Objects.equals(this.byFieldValue, that.byFieldValue)
                && Objects.equals(this.correlatedByFieldValue, that.correlatedByFieldValue)
                && Objects.equals(this.partitionFieldName, that.partitionFieldName)
                && Objects.equals(this.partitionFieldValue, that.partitionFieldValue)
                && Objects.equals(this.overFieldName, that.overFieldName)
                && Objects.equals(this.overFieldValue, that.overFieldValue)
                && Objects.equals(this.timestamp, that.timestamp)
                && Objects.equals(this.isInterim, that.isInterim)
                && Objects.equals(this.causes, that.causes)
                && Objects.equals(this.influencers, that.influencers);
    }

    public boolean hadBigNormalisedUpdate() {
        return this.hadBigNormalisedUpdate;
    }

    public void resetBigNormalisedUpdateFlag() {
        hadBigNormalisedUpdate = false;
    }

    public void raiseBigNormalisedUpdateFlag() {
        hadBigNormalisedUpdate = true;
    }
}
