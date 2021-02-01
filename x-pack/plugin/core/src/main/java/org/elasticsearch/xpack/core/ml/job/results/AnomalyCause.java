/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Anomaly Cause POJO.
 * Used as a nested level inside population anomaly records.
 */
public class AnomalyCause implements ToXContentObject, Writeable {
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
    public static final ParseField GEO_RESULTS = new ParseField("geo_results");

    /**
     * Metric Results
     */
    public static final ParseField FIELD_NAME = new ParseField("field_name");

    public static final ObjectParser<AnomalyCause, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<AnomalyCause, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<AnomalyCause, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<AnomalyCause, Void> parser = new ObjectParser<>(ANOMALY_CAUSE.getPreferredName(), ignoreUnknownFields,
                AnomalyCause::new);

        parser.declareDouble(AnomalyCause::setProbability, PROBABILITY);
        parser.declareString(AnomalyCause::setByFieldName, BY_FIELD_NAME);
        parser.declareString(AnomalyCause::setByFieldValue, BY_FIELD_VALUE);
        parser.declareString(AnomalyCause::setCorrelatedByFieldValue, CORRELATED_BY_FIELD_VALUE);
        parser.declareString(AnomalyCause::setPartitionFieldName, PARTITION_FIELD_NAME);
        parser.declareString(AnomalyCause::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        parser.declareString(AnomalyCause::setFunction, FUNCTION);
        parser.declareString(AnomalyCause::setFunctionDescription, FUNCTION_DESCRIPTION);
        parser.declareDoubleArray(AnomalyCause::setTypical, TYPICAL);
        parser.declareDoubleArray(AnomalyCause::setActual, ACTUAL);
        parser.declareString(AnomalyCause::setFieldName, FIELD_NAME);
        parser.declareString(AnomalyCause::setOverFieldName, OVER_FIELD_NAME);
        parser.declareString(AnomalyCause::setOverFieldValue, OVER_FIELD_VALUE);
        parser.declareObjectArray(AnomalyCause::setInfluencers, ignoreUnknownFields ? Influence.LENIENT_PARSER : Influence.STRICT_PARSER,
                INFLUENCERS);
        parser.declareObject(AnomalyCause::setGeoResults,
            ignoreUnknownFields ? GeoResults.LENIENT_PARSER : GeoResults.STRICT_PARSER,
            GEO_RESULTS);

        return parser;
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
    private GeoResults geoResults;

    private String fieldName;

    private String overFieldName;
    private String overFieldValue;

    private List<Influence> influencers;

    public AnomalyCause() {
    }

    @SuppressWarnings("unchecked")
    public AnomalyCause(StreamInput in) throws IOException {
        probability = in.readDouble();
        byFieldName = in.readOptionalString();
        byFieldValue = in.readOptionalString();
        correlatedByFieldValue = in.readOptionalString();
        partitionFieldName = in.readOptionalString();
        partitionFieldValue = in.readOptionalString();
        function = in.readOptionalString();
        functionDescription = in.readOptionalString();
        if (in.readBoolean()) {
            typical = (List<Double>) in.readGenericValue();
        }
        if (in.readBoolean()) {
            actual = (List<Double>) in.readGenericValue();
        }
        fieldName = in.readOptionalString();
        overFieldName = in.readOptionalString();
        overFieldValue = in.readOptionalString();
        if (in.readBoolean()) {
            influencers = in.readList(Influence::new);
        }
        geoResults = in.readOptionalWriteable(GeoResults::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(probability);
        out.writeOptionalString(byFieldName);
        out.writeOptionalString(byFieldValue);
        out.writeOptionalString(correlatedByFieldValue);
        out.writeOptionalString(partitionFieldName);
        out.writeOptionalString(partitionFieldValue);
        out.writeOptionalString(function);
        out.writeOptionalString(functionDescription);
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
        out.writeOptionalString(fieldName);
        out.writeOptionalString(overFieldName);
        out.writeOptionalString(overFieldValue);
        boolean hasInfluencers = influencers != null;
        out.writeBoolean(hasInfluencers);
        if (hasInfluencers) {
            out.writeList(influencers);
        }
        out.writeOptionalWriteable(geoResults);
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
        if (geoResults != null) {
            builder.field(GEO_RESULTS.getPreferredName(), geoResults);
        }
        builder.endObject();
        return builder;
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

    public List<Influence> getInfluencers() {
        return influencers;
    }

    public void setInfluencers(List<Influence> influencers) {
        this.influencers = influencers;
    }

    public GeoResults getGeoResults() {
        return geoResults;
    }

    public void setGeoResults(GeoResults geoResults) {
        this.geoResults = geoResults;
    }

    @Override
    public int hashCode() {
        return Objects.hash(probability,
                actual,
                typical,
                byFieldName,
                byFieldValue,
                correlatedByFieldValue,
                fieldName,
                function,
                functionDescription,
                overFieldName,
                overFieldValue,
                partitionFieldName,
                partitionFieldValue,
                influencers,
                geoResults);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof AnomalyCause == false) {
            return false;
        }

        AnomalyCause that = (AnomalyCause)other;

        return this.probability == that.probability &&
                Objects.deepEquals(this.typical, that.typical) &&
                Objects.deepEquals(this.actual, that.actual) &&
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
                Objects.equals(this.geoResults, that.geoResults) &&
                Objects.equals(this.influencers, that.influencers);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

}
