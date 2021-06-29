/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.client.ml.job.config.DetectorFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Anomaly Cause POJO.
 * Used as a nested level inside population anomaly records.
 */
public class AnomalyCause implements ToXContentObject {

    public static final ParseField ANOMALY_CAUSE = new ParseField("anomaly_cause");

    /**
     * Result fields
     */
    public static final ParseField PROBABILITY = new ParseField("probability");
    public static final ParseField OVER_FIELD_NAME = new ParseField("over_field_name");
    public static final ParseField OVER_FIELD_VALUE = new ParseField("over_field_value");
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

    /**
     * Metric Results
     */
    public static final ParseField FIELD_NAME = new ParseField("field_name");

    public static final ObjectParser<AnomalyCause, Void> PARSER =
        new ObjectParser<>(ANOMALY_CAUSE.getPreferredName(), true, AnomalyCause::new);

    static {
        PARSER.declareDouble(AnomalyCause::setProbability, PROBABILITY);
        PARSER.declareString(AnomalyCause::setByFieldName, BY_FIELD_NAME);
        PARSER.declareString(AnomalyCause::setByFieldValue, BY_FIELD_VALUE);
        PARSER.declareString(AnomalyCause::setCorrelatedByFieldValue, CORRELATED_BY_FIELD_VALUE);
        PARSER.declareString(AnomalyCause::setPartitionFieldName, PARTITION_FIELD_NAME);
        PARSER.declareString(AnomalyCause::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        PARSER.declareString(AnomalyCause::setFunction, FUNCTION);
        PARSER.declareString(AnomalyCause::setFunctionDescription, FUNCTION_DESCRIPTION);
        PARSER.declareDoubleArray(AnomalyCause::setTypical, TYPICAL);
        PARSER.declareDoubleArray(AnomalyCause::setActual, ACTUAL);
        PARSER.declareString(AnomalyCause::setFieldName, FIELD_NAME);
        PARSER.declareString(AnomalyCause::setOverFieldName, OVER_FIELD_NAME);
        PARSER.declareString(AnomalyCause::setOverFieldValue, OVER_FIELD_VALUE);
        PARSER.declareObjectArray(AnomalyCause::setInfluencers, Influence.PARSER, INFLUENCERS);
    }

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
    private String fieldName;
    private String overFieldName;
    private String overFieldValue;

    private List<Influence> influencers;

    AnomalyCause() {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PROBABILITY.getPreferredName(), probability);
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
        if (influencers != null) {
            builder.field(INFLUENCERS.getPreferredName(), influencers);
        }
        builder.endObject();
        return builder;
    }

    public double getProbability() {
        return probability;
    }

    void setProbability(double value) {
        probability = value;
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

    public List<Influence> getInfluencers() {
        return influencers;
    }

    void setInfluencers(List<Influence> influencers) {
        this.influencers = Collections.unmodifiableList(influencers);
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
        return Objects.hash(probability, actual, typical, byFieldName, byFieldValue, correlatedByFieldValue, fieldName, function,
            functionDescription, overFieldName, overFieldValue, partitionFieldName, partitionFieldValue, influencers);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        AnomalyCause that = (AnomalyCause)other;

        return this.probability == that.probability &&
            Objects.equals(this.typical, that.typical) &&
            Objects.equals(this.actual, that.actual) &&
            Objects.equals(this.function, that.function) &&
            Objects.equals(this.functionDescription, that.functionDescription) &&
            Objects.equals(this.fieldName, that.fieldName) &&
            Objects.equals(this.byFieldName, that.byFieldName) &&
            Objects.equals(this.byFieldValue, that.byFieldValue) &&
            Objects.equals(this.correlatedByFieldValue, that.correlatedByFieldValue) &&
            Objects.equals(this.partitionFieldName, that.partitionFieldName) &&
            Objects.equals(this.partitionFieldValue, that.partitionFieldValue) &&
            Objects.equals(this.overFieldName, that.overFieldName) &&
            Objects.equals(this.overFieldValue, that.overFieldValue) &&
            Objects.equals(this.influencers, that.influencers);
    }
}
