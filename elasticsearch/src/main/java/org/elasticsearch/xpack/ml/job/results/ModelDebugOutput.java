/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

/**
 * Model Debug POJO.
 * Some of the fields being with the word "debug".  This avoids creation of
 * reserved words that are likely to clash with fields in the input data (due to
 * the restrictions on Elasticsearch mappings).
 */
public class ModelDebugOutput extends ToXContentToBytes implements Writeable {
    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "model_debug_output";
    public static final ParseField RESULTS_FIELD = new ParseField(RESULT_TYPE_VALUE);

    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField PARTITION_FIELD_NAME = new ParseField("partition_field_name");
    public static final ParseField PARTITION_FIELD_VALUE = new ParseField("partition_field_value");
    public static final ParseField OVER_FIELD_NAME = new ParseField("over_field_name");
    public static final ParseField OVER_FIELD_VALUE = new ParseField("over_field_value");
    public static final ParseField BY_FIELD_NAME = new ParseField("by_field_name");
    public static final ParseField BY_FIELD_VALUE = new ParseField("by_field_value");
    public static final ParseField DEBUG_FEATURE = new ParseField("debug_feature");
    public static final ParseField DEBUG_LOWER = new ParseField("debug_lower");
    public static final ParseField DEBUG_UPPER = new ParseField("debug_upper");
    public static final ParseField DEBUG_MEDIAN = new ParseField("debug_median");
    public static final ParseField ACTUAL = new ParseField("actual");

    public static final ConstructingObjectParser<ModelDebugOutput, Void> PARSER =
            new ConstructingObjectParser<>(RESULT_TYPE_VALUE, a -> new ModelDebugOutput((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareString((modelDebugOutput, s) -> {}, Result.RESULT_TYPE);
        PARSER.declareField(ModelDebugOutput::setTimestamp, p -> {
            if (p.currentToken() == Token.VALUE_NUMBER) {
                return new Date(p.longValue());
            } else if (p.currentToken() == Token.VALUE_STRING) {
                return new Date(TimeUtils.dateStringToEpoch(p.text()));
            }
            throw new IllegalArgumentException("unexpected token [" + p.currentToken() + "] for [" + TIMESTAMP.getPreferredName() + "]");
        }, TIMESTAMP, ValueType.VALUE);
        PARSER.declareString(ModelDebugOutput::setPartitionFieldName, PARTITION_FIELD_NAME);
        PARSER.declareString(ModelDebugOutput::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        PARSER.declareString(ModelDebugOutput::setOverFieldName, OVER_FIELD_NAME);
        PARSER.declareString(ModelDebugOutput::setOverFieldValue, OVER_FIELD_VALUE);
        PARSER.declareString(ModelDebugOutput::setByFieldName, BY_FIELD_NAME);
        PARSER.declareString(ModelDebugOutput::setByFieldValue, BY_FIELD_VALUE);
        PARSER.declareString(ModelDebugOutput::setDebugFeature, DEBUG_FEATURE);
        PARSER.declareDouble(ModelDebugOutput::setDebugLower, DEBUG_LOWER);
        PARSER.declareDouble(ModelDebugOutput::setDebugUpper, DEBUG_UPPER);
        PARSER.declareDouble(ModelDebugOutput::setDebugMedian, DEBUG_MEDIAN);
        PARSER.declareDouble(ModelDebugOutput::setActual, ACTUAL);
    }

    private final String jobId;
    private Date timestamp;
    private String id;
    private String partitionFieldName;
    private String partitionFieldValue;
    private String overFieldName;
    private String overFieldValue;
    private String byFieldName;
    private String byFieldValue;
    private String debugFeature;
    private double debugLower;
    private double debugUpper;
    private double debugMedian;
    private double actual;

    public ModelDebugOutput(String jobId) {
        this.jobId = jobId;
    }

    public ModelDebugOutput(StreamInput in) throws IOException {
        jobId = in.readString();
        if (in.readBoolean()) {
            timestamp = new Date(in.readLong());
        }
        id = in.readOptionalString();
        partitionFieldName = in.readOptionalString();
        partitionFieldValue = in.readOptionalString();
        overFieldName = in.readOptionalString();
        overFieldValue = in.readOptionalString();
        byFieldName = in.readOptionalString();
        byFieldValue = in.readOptionalString();
        debugFeature = in.readOptionalString();
        debugLower = in.readDouble();
        debugUpper = in.readDouble();
        debugMedian = in.readDouble();
        actual = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        boolean hasTimestamp = timestamp != null;
        out.writeBoolean(hasTimestamp);
        if (hasTimestamp) {
            out.writeLong(timestamp.getTime());
        }
        out.writeOptionalString(id);
        out.writeOptionalString(partitionFieldName);
        out.writeOptionalString(partitionFieldValue);
        out.writeOptionalString(overFieldName);
        out.writeOptionalString(overFieldValue);
        out.writeOptionalString(byFieldName);
        out.writeOptionalString(byFieldValue);
        out.writeOptionalString(debugFeature);
        out.writeDouble(debugLower);
        out.writeDouble(debugUpper);
        out.writeDouble(debugMedian);
        out.writeDouble(actual);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        if (timestamp != null) {
            builder.field(TIMESTAMP.getPreferredName(), timestamp.getTime());
        }
        if (partitionFieldName != null) {
            builder.field(PARTITION_FIELD_NAME.getPreferredName(), partitionFieldName);
        }
        if (partitionFieldValue != null) {
            builder.field(PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue);
        }
        if (overFieldName != null) {
            builder.field(OVER_FIELD_NAME.getPreferredName(), overFieldName);
        }
        if (overFieldValue != null) {
            builder.field(OVER_FIELD_VALUE.getPreferredName(), overFieldValue);
        }
        if (byFieldName != null) {
            builder.field(BY_FIELD_NAME.getPreferredName(), byFieldName);
        }
        if (byFieldValue != null) {
            builder.field(BY_FIELD_VALUE.getPreferredName(), byFieldValue);
        }
        if (debugFeature != null) {
            builder.field(DEBUG_FEATURE.getPreferredName(), debugFeature);
        }
        builder.field(DEBUG_LOWER.getPreferredName(), debugLower);
        builder.field(DEBUG_UPPER.getPreferredName(), debugUpper);
        builder.field(DEBUG_MEDIAN.getPreferredName(), debugMedian);
        builder.field(ACTUAL.getPreferredName(), actual);
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    public void setPartitionFieldName(String partitionFieldName) {
        this.partitionFieldName = partitionFieldName;
    }

    public String getPartitionFieldValue() {
        return partitionFieldValue;
    }

    public void setPartitionFieldValue(String partitionFieldValue) {
        this.partitionFieldValue = partitionFieldValue;
    }

    public String getOverFieldName() {
        return overFieldName;
    }

    public void setOverFieldName(String overFieldName) {
        this.overFieldName = overFieldName;
    }

    public String getOverFieldValue() {
        return overFieldValue;
    }

    public void setOverFieldValue(String overFieldValue) {
        this.overFieldValue = overFieldValue;
    }

    public String getByFieldName() {
        return byFieldName;
    }

    public void setByFieldName(String byFieldName) {
        this.byFieldName = byFieldName;
    }

    public String getByFieldValue() {
        return byFieldValue;
    }

    public void setByFieldValue(String byFieldValue) {
        this.byFieldValue = byFieldValue;
    }

    public String getDebugFeature() {
        return debugFeature;
    }

    public void setDebugFeature(String debugFeature) {
        this.debugFeature = debugFeature;
    }

    public double getDebugLower() {
        return debugLower;
    }

    public void setDebugLower(double debugLower) {
        this.debugLower = debugLower;
    }

    public double getDebugUpper() {
        return debugUpper;
    }

    public void setDebugUpper(double debugUpper) {
        this.debugUpper = debugUpper;
    }

    public double getDebugMedian() {
        return debugMedian;
    }

    public void setDebugMedian(double debugMedian) {
        this.debugMedian = debugMedian;
    }

    public double getActual() {
        return actual;
    }

    public void setActual(double actual) {
        this.actual = actual;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof ModelDebugOutput == false) {
            return false;
        }
        // id excluded here as it is generated by the datastore
        ModelDebugOutput that = (ModelDebugOutput) other;
        return Objects.equals(this.jobId, that.jobId) &&
                Objects.equals(this.timestamp, that.timestamp) &&
                Objects.equals(this.partitionFieldValue, that.partitionFieldValue) &&
                Objects.equals(this.partitionFieldName, that.partitionFieldName) &&
                Objects.equals(this.overFieldValue, that.overFieldValue) &&
                Objects.equals(this.overFieldName, that.overFieldName) &&
                Objects.equals(this.byFieldValue, that.byFieldValue) &&
                Objects.equals(this.byFieldName, that.byFieldName) &&
                Objects.equals(this.debugFeature, that.debugFeature) &&
                this.debugLower == that.debugLower &&
                this.debugUpper == that.debugUpper &&
                this.debugMedian == that.debugMedian &&
                this.actual == that.actual;
    }

    @Override
    public int hashCode() {
        // id excluded here as it is generated by the datastore
        return Objects.hash(jobId, timestamp, partitionFieldName, partitionFieldValue,
                overFieldName, overFieldValue, byFieldName, byFieldValue,
                debugFeature, debugLower, debugUpper, debugMedian, actual);
    }
}
