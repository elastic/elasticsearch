/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.client.ml.job.config.DetectorFunction;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Anomaly Record POJO.
 * Uses the object wrappers Boolean and Double so <code>null</code> values
 * can be returned if the members have not been set.
 */
public class AnomalyRecord implements ToXContentObject {

    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "record";

    /**
     * Result fields (all detector types)
     */
    public static final ParseField PROBABILITY = new ParseField("probability");
    public static final ParseField MULTI_BUCKET_IMPACT = new ParseField("multi_bucket_impact");
    public static final ParseField DETECTOR_INDEX = new ParseField("detector_index");
    public static final ParseField BY_FIELD_NAME = new ParseField("by_field_name");
    public static final ParseField BY_FIELD_VALUE = new ParseField("by_field_value");
    public static final ParseField CORRELATED_BY_FIELD_VALUE = new ParseField("correlated_by_field_value");
    public static final ParseField PARTITION_FIELD_NAME = new ParseField("partition_field_name");
    public static final ParseField PARTITION_FIELD_VALUE = new ParseField("partition_field_value");
    public static final ParseField FUNCTION = new ParseField("function");
    public static final ParseField FUNCTION_DESCRIPTION = new ParseField("function_description");
    public static final ParseField TYPICAL = new ParseField("typical");
    public static final ParseField ACTUAL = new ParseField("actual");
    public static final ParseField INFLUENCERS = new ParseField("influencers");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");

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
    public static final ParseField RECORD_SCORE = new ParseField("record_score");
    public static final ParseField INITIAL_RECORD_SCORE = new ParseField("initial_record_score");

    public static final ConstructingObjectParser<AnomalyRecord, Void> PARSER =
        new ConstructingObjectParser<>(RESULT_TYPE_VALUE, true, a -> new AnomalyRecord((String) a[0], (Date) a[1], (long) a[2]));


    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p) -> TimeUtil.parseTimeField(p, Result.TIMESTAMP.getPreferredName()),
                Result.TIMESTAMP, ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        PARSER.declareString((anomalyRecord, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareDouble(AnomalyRecord::setProbability, PROBABILITY);
        PARSER.declareDouble(AnomalyRecord::setMultiBucketImpact, MULTI_BUCKET_IMPACT);
        PARSER.declareDouble(AnomalyRecord::setRecordScore, RECORD_SCORE);
        PARSER.declareDouble(AnomalyRecord::setInitialRecordScore, INITIAL_RECORD_SCORE);
        PARSER.declareInt(AnomalyRecord::setDetectorIndex, DETECTOR_INDEX);
        PARSER.declareBoolean(AnomalyRecord::setInterim, Result.IS_INTERIM);
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
    private int detectorIndex;
    private double probability;
    private Double multiBucketImpact;
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

    private double recordScore;

    private double initialRecordScore;

    private final Date timestamp;
    private final long bucketSpan;

    private List<Influence> influences;

    AnomalyRecord(String jobId, Date timestamp, long bucketSpan) {
        this.jobId = jobId;
        this.timestamp = Objects.requireNonNull(timestamp);
        this.bucketSpan = bucketSpan;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(PROBABILITY.getPreferredName(), probability);
        if (multiBucketImpact != null) {
            builder.field(MULTI_BUCKET_IMPACT.getPreferredName(), multiBucketImpact);
        }
        builder.field(RECORD_SCORE.getPreferredName(), recordScore);
        builder.field(INITIAL_RECORD_SCORE.getPreferredName(), initialRecordScore);
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(DETECTOR_INDEX.getPreferredName(), detectorIndex);
        builder.field(Result.IS_INTERIM.getPreferredName(), isInterim);
        builder.timeField(Result.TIMESTAMP.getPreferredName(), Result.TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
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
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return this.jobId;
    }

    public int getDetectorIndex() {
        return detectorIndex;
    }

    void setDetectorIndex(int detectorIndex) {
        this.detectorIndex = detectorIndex;
    }

    public double getRecordScore() {
        return recordScore;
    }

    void setRecordScore(double recordScore) {
        this.recordScore = recordScore;
    }

    public double getInitialRecordScore() {
        return initialRecordScore;
    }

    void setInitialRecordScore(double initialRecordScore) {
        this.initialRecordScore = initialRecordScore;
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

    void setProbability(double value) {
        probability = value;
    }

    public double getMultiBucketImpact() {
        return multiBucketImpact;
    }

    void setMultiBucketImpact(double value) {
        multiBucketImpact = value;
    }

    public String getByFieldName() {
        return byFieldName;
    }

    void setByFieldName(String value) {
        byFieldName = value;
    }

    public String getByFieldValue() {
        return byFieldValue;
    }

    void setByFieldValue(String value) {
        byFieldValue = value;
    }

    public String getCorrelatedByFieldValue() {
        return correlatedByFieldValue;
    }

    void setCorrelatedByFieldValue(String value) {
        correlatedByFieldValue = value;
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    void setPartitionFieldName(String field) {
        partitionFieldName = field;
    }

    public String getPartitionFieldValue() {
        return partitionFieldValue;
    }

    void setPartitionFieldValue(String value) {
        partitionFieldValue = value;
    }

    public String getFunction() {
        return function;
    }

    void setFunction(String name) {
        function = name;
    }

    public String getFunctionDescription() {
        return functionDescription;
    }

    void setFunctionDescription(String functionDescription) {
        this.functionDescription = functionDescription;
    }

    public List<Double> getTypical() {
        return typical;
    }

    void setTypical(List<Double> typical) {
        this.typical = Collections.unmodifiableList(typical);
    }

    public List<Double> getActual() {
        return actual;
    }

    void setActual(List<Double> actual) {
        this.actual = Collections.unmodifiableList(actual);
    }

    public boolean isInterim() {
        return isInterim;
    }

    void setInterim(boolean isInterim) {
        this.isInterim = isInterim;
    }

    public String getFieldName() {
        return fieldName;
    }

    void setFieldName(String field) {
        fieldName = field;
    }

    public String getOverFieldName() {
        return overFieldName;
    }

    void setOverFieldName(String name) {
        overFieldName = name;
    }

    public String getOverFieldValue() {
        return overFieldValue;
    }

    void setOverFieldValue(String value) {
        overFieldValue = value;
    }

    public List<AnomalyCause> getCauses() {
        return causes;
    }

    void setCauses(List<AnomalyCause> causes) {
        this.causes = Collections.unmodifiableList(causes);
    }

    public List<Influence> getInfluencers() {
        return influences;
    }

    void setInfluencers(List<Influence> influencers) {
        this.influences = Collections.unmodifiableList(influencers);
    }

    @Nullable
    public GeoPoint getTypicalGeoPoint() {
        if (DetectorFunction.LAT_LONG.getFullName().equals(function) == false || typical == null) {
            return null;
        }
        if (typical.size() == 2) {
            return new GeoPoint(typical.get(0), typical.get(1));
        }
        return null;
    }

    @Nullable
    public GeoPoint getActualGeoPoint() {
        if (DetectorFunction.LAT_LONG.getFullName().equals(function) == false || actual == null) {
            return null;
        }
        if (actual.size() == 2) {
            return new GeoPoint(actual.get(0), actual.get(1));
        }
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, detectorIndex, bucketSpan, probability, multiBucketImpact, recordScore,
                initialRecordScore, typical, actual,function, functionDescription, fieldName,
                byFieldName, byFieldValue, correlatedByFieldValue, partitionFieldName,
                partitionFieldValue, overFieldName, overFieldValue, timestamp, isInterim,
                causes, influences, jobId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        AnomalyRecord that = (AnomalyRecord) other;

        return Objects.equals(this.jobId, that.jobId)
            && this.detectorIndex == that.detectorIndex
            && this.bucketSpan == that.bucketSpan
            && this.probability == that.probability
            && Objects.equals(this.multiBucketImpact, that.multiBucketImpact)
            && this.recordScore == that.recordScore
            && this.initialRecordScore == that.initialRecordScore
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
