/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
    public static final ParseField DETECTOR_INDEX = new ParseField("detector_index");
    public static final ParseField SEQUENCE_NUM = new ParseField("sequence_num");
    public static final ParseField PROBABILITY = new ParseField("probability");
    public static final ParseField BY_FIELD_NAME = new ParseField("by_field_name");
    public static final ParseField BY_FIELD_VALUE = new ParseField("by_field_value");
    public static final ParseField CORRELATED_BY_FIELD_VALUE = new ParseField("correlated_by_field_value");
    public static final ParseField PARTITION_FIELD_NAME = new ParseField("partition_field_name");
    public static final ParseField PARTITION_FIELD_VALUE = new ParseField("partition_field_value");
    public static final ParseField FUNCTION = new ParseField("function");
    public static final ParseField FUNCTION_DESCRIPTION = new ParseField("function_description");
    public static final ParseField TYPICAL = new ParseField("typical");
    public static final ParseField ACTUAL = new ParseField("actual");
    public static final ParseField IS_INTERIM = new ParseField("is_interim");
    public static final ParseField INFLUENCERS = new ParseField("influencers");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("records");

    /**
     * Metric Results (including population metrics)
     */
    public static final ParseField FIELD_NAME = new ParseField("field_name");

    /**
     * Population results
     */
    public static final ParseField OVER_FIELD_NAME = new ParseField("over_field_name");
    public static final ParseField OVER_FIELD_VALUE = new ParseField("over_field_value");
    public static final ParseField CAUSES = new ParseField("causes");

    /**
     * Normalization
     */
    public static final ParseField ANOMALY_SCORE = new ParseField("anomaly_score");
    public static final ParseField NORMALIZED_PROBABILITY = new ParseField("normalized_probability");
    public static final ParseField INITIAL_NORMALIZED_PROBABILITY = new ParseField("initial_normalized_probability");

    public static final ConstructingObjectParser<AnomalyRecord, Void> PARSER =
            new ConstructingObjectParser<>(RESULT_TYPE_VALUE, true,
                    a -> new AnomalyRecord((String) a[0], (Date) a[1], (long) a[2], (int) a[3]));

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
        PARSER.declareString((anomalyRecord, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareDouble(AnomalyRecord::setProbability, PROBABILITY);
        PARSER.declareDouble(AnomalyRecord::setAnomalyScore, ANOMALY_SCORE);
        PARSER.declareDouble(AnomalyRecord::setNormalizedProbability, NORMALIZED_PROBABILITY);
        PARSER.declareDouble(AnomalyRecord::setInitialNormalizedProbability, INITIAL_NORMALIZED_PROBABILITY);
        PARSER.declareInt(AnomalyRecord::setDetectorIndex, DETECTOR_INDEX);
        PARSER.declareBoolean(AnomalyRecord::setInterim, IS_INTERIM);
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
    private final int sequenceNum;
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

    private final Date timestamp;
    private final long bucketSpan;

    private List<Influence> influences;

    public AnomalyRecord(String jobId, Date timestamp, long bucketSpan, int sequenceNum) {
        this.jobId = jobId;
        this.timestamp = ExceptionsHelper.requireNonNull(timestamp, TIMESTAMP.getPreferredName());
        this.bucketSpan = bucketSpan;
        this.sequenceNum = sequenceNum;
    }

    @SuppressWarnings("unchecked")
    public AnomalyRecord(StreamInput in) throws IOException {
        jobId = in.readString();
        sequenceNum = in.readInt();
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
        timestamp = new Date(in.readLong());
        bucketSpan = in.readLong();
        if (in.readBoolean()) {
            influences = in.readList(Influence::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeInt(sequenceNum);
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
        out.writeLong(timestamp.getTime());
        out.writeLong(bucketSpan);
        boolean hasInfluencers = influences != null;
        out.writeBoolean(hasInfluencers);
        if (hasInfluencers) {
            out.writeList(influences);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(PROBABILITY.getPreferredName(), probability);
        builder.field(ANOMALY_SCORE.getPreferredName(), anomalyScore);
        builder.field(NORMALIZED_PROBABILITY.getPreferredName(), normalizedProbability);
        builder.field(INITIAL_NORMALIZED_PROBABILITY.getPreferredName(), initialNormalizedProbability);
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(DETECTOR_INDEX.getPreferredName(), detectorIndex);
        builder.field(SEQUENCE_NUM.getPreferredName(), sequenceNum);
        builder.field(IS_INTERIM.getPreferredName(), isInterim);
        builder.field(TIMESTAMP.getPreferredName(), timestamp.getTime());
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
        if (influences != null) {
            builder.field(INFLUENCERS.getPreferredName(), influences);
        }

        Map<String, Set<String>> inputFields = inputFieldMap();
        for (String fieldName : inputFields.keySet()) {
            builder.field(fieldName, inputFields.get(fieldName));
        }

        builder.endObject();
        return builder;
    }

    private Map<String, Set<String>> inputFieldMap() {
        Map<String, Set<String>> result = new HashMap<>();

        addInputFieldsToMap(result, byFieldName, byFieldValue);
        addInputFieldsToMap(result, overFieldName, overFieldValue);
        addInputFieldsToMap(result, partitionFieldName, partitionFieldValue);

        if (influences != null) {
            for (Influence inf : influences) {
                String fieldName = inf.getInfluencerFieldName();
                for (String fieldValue : inf.getInfluencerFieldValues()) {
                    addInputFieldsToMap(result, fieldName, fieldValue);
                }
            }
        }
        return result;
    }

    private void addInputFieldsToMap(Map<String, Set<String>> inputFields, String fieldName, String fieldValue) {
        if (!Strings.isNullOrEmpty(fieldName) && fieldValue != null) {
            if (ReservedFieldNames.isValidFieldName(fieldName)) {
                inputFields.computeIfAbsent(fieldName, k -> new HashSet<String>()).add(fieldValue);
            }
        }
    }

    public String getJobId() {
        return this.jobId;
    }

    /**
     * Data store ID of this record.
     */
    public String getId() {
        return jobId + "_" + timestamp.getTime() + "_" + bucketSpan + "_" + sequenceNum;
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

    /**
     * Bucketspan expressed in seconds
     */
    public long getBucketSpan() {
        return bucketSpan;
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
        return influences;
    }

    public void setInfluencers(List<Influence> influencers) {
        this.influences = influencers;
    }


    @Override
    public int hashCode() {
        return Objects.hash(jobId, detectorIndex, sequenceNum, bucketSpan, probability, anomalyScore,
                normalizedProbability, initialNormalizedProbability, typical, actual,
                function, functionDescription, fieldName, byFieldName, byFieldValue, correlatedByFieldValue,
                partitionFieldName, partitionFieldValue, overFieldName, overFieldValue,
                timestamp, isInterim, causes, influences, jobId);
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

        return Objects.equals(this.jobId, that.jobId)
                && this.detectorIndex == that.detectorIndex
                && this.sequenceNum == that.sequenceNum
                && this.bucketSpan == that.bucketSpan
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
                && Objects.equals(this.influences, that.influences);
    }
}
