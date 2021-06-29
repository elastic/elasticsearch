/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

/**
 * Model Plot POJO.
 */
public class ModelPlot implements ToXContentObject, Writeable {
    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "model_plot";
    public static final ParseField RESULTS_FIELD = new ParseField(RESULT_TYPE_VALUE);

    public static final ParseField PARTITION_FIELD_NAME = new ParseField("partition_field_name");
    public static final ParseField PARTITION_FIELD_VALUE = new ParseField("partition_field_value");
    public static final ParseField OVER_FIELD_NAME = new ParseField("over_field_name");
    public static final ParseField OVER_FIELD_VALUE = new ParseField("over_field_value");
    public static final ParseField BY_FIELD_NAME = new ParseField("by_field_name");
    public static final ParseField BY_FIELD_VALUE = new ParseField("by_field_value");
    public static final ParseField MODEL_FEATURE = new ParseField("model_feature");
    public static final ParseField MODEL_LOWER = new ParseField("model_lower");
    public static final ParseField MODEL_UPPER = new ParseField("model_upper");
    public static final ParseField MODEL_MEDIAN = new ParseField("model_median");
    public static final ParseField ACTUAL = new ParseField("actual");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField DETECTOR_INDEX = new ParseField("detector_index");

    public static final ConstructingObjectParser<ModelPlot, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<ModelPlot, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<ModelPlot, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<ModelPlot, Void> parser = new ConstructingObjectParser<>(RESULT_TYPE_VALUE, ignoreUnknownFields,
                a -> new ModelPlot((String) a[0], (Date) a[1], (long) a[2], (int) a[3]));

        parser.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        parser.declareField(ConstructingObjectParser.constructorArg(),
                p -> TimeUtils.parseTimeField(p, Result.TIMESTAMP.getPreferredName()), Result.TIMESTAMP, ValueType.VALUE);
        parser.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        parser.declareInt(ConstructingObjectParser.constructorArg(), DETECTOR_INDEX);
        parser.declareString((modelPlot, s) -> {}, Result.RESULT_TYPE);
        parser.declareString(ModelPlot::setPartitionFieldName, PARTITION_FIELD_NAME);
        parser.declareString(ModelPlot::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        parser.declareString(ModelPlot::setOverFieldName, OVER_FIELD_NAME);
        parser.declareString(ModelPlot::setOverFieldValue, OVER_FIELD_VALUE);
        parser.declareString(ModelPlot::setByFieldName, BY_FIELD_NAME);
        parser.declareString(ModelPlot::setByFieldValue, BY_FIELD_VALUE);
        parser.declareString(ModelPlot::setModelFeature, MODEL_FEATURE);
        parser.declareDouble(ModelPlot::setModelLower, MODEL_LOWER);
        parser.declareDouble(ModelPlot::setModelUpper, MODEL_UPPER);
        parser.declareDouble(ModelPlot::setModelMedian, MODEL_MEDIAN);
        parser.declareDouble(ModelPlot::setActual, ACTUAL);

        return parser;
    }

    private final String jobId;
    private final Date timestamp;
    private final long bucketSpan;
    private int detectorIndex;
    private String partitionFieldName;
    private String partitionFieldValue;
    private String overFieldName;
    private String overFieldValue;
    private String byFieldName;
    private String byFieldValue;
    private String modelFeature;
    private double modelLower;
    private double modelUpper;
    private double modelMedian;
    /**
     * This can be <code>null</code> because buckets where no values were observed will still have a model, but no actual
     */
    private Double actual;

    public ModelPlot(String jobId, Date timestamp, long bucketSpan, int detectorIndex) {
        this.jobId = jobId;
        this.timestamp = timestamp;
        this.bucketSpan = bucketSpan;
        this.detectorIndex = detectorIndex;
    }

    public ModelPlot(StreamInput in) throws IOException {
        jobId = in.readString();
        timestamp = new Date(in.readLong());
        partitionFieldName = in.readOptionalString();
        partitionFieldValue = in.readOptionalString();
        overFieldName = in.readOptionalString();
        overFieldValue = in.readOptionalString();
        byFieldName = in.readOptionalString();
        byFieldValue = in.readOptionalString();
        modelFeature = in.readOptionalString();
        modelLower = in.readDouble();
        modelUpper = in.readDouble();
        modelMedian = in.readDouble();
        actual = in.readOptionalDouble();
        bucketSpan = in.readLong();
        detectorIndex = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeLong(timestamp.getTime());
        out.writeOptionalString(partitionFieldName);
        out.writeOptionalString(partitionFieldValue);
        out.writeOptionalString(overFieldName);
        out.writeOptionalString(overFieldValue);
        out.writeOptionalString(byFieldName);
        out.writeOptionalString(byFieldValue);
        out.writeOptionalString(modelFeature);
        out.writeDouble(modelLower);
        out.writeDouble(modelUpper);
        out.writeDouble(modelMedian);
        out.writeOptionalDouble(actual);
        out.writeLong(bucketSpan);
        out.writeInt(detectorIndex);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(DETECTOR_INDEX.getPreferredName(), detectorIndex);

        if (timestamp != null) {
            builder.timeField(Result.TIMESTAMP.getPreferredName(),
                    Result.TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
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
        if (modelFeature != null) {
            builder.field(MODEL_FEATURE.getPreferredName(), modelFeature);
        }
        builder.field(MODEL_LOWER.getPreferredName(), modelLower);
        builder.field(MODEL_UPPER.getPreferredName(), modelUpper);
        builder.field(MODEL_MEDIAN.getPreferredName(), modelMedian);
        if (actual != null) {
            builder.field(ACTUAL.getPreferredName(), actual);
        }
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public String getId() {
        return jobId + "_model_plot_" + timestamp.getTime() + "_" + bucketSpan
                + "_" + detectorIndex + "_" + MachineLearningField.valuesToId(byFieldValue, overFieldValue, partitionFieldValue);
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public long getBucketSpan() {
        return bucketSpan;
    }

    public int getDetectorIndex() {
        return detectorIndex;
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

    public String getModelFeature() {
        return modelFeature;
    }

    public void setModelFeature(String modelFeature) {
        this.modelFeature = modelFeature;
    }

    public double getModelLower() {
        return modelLower;
    }

    public void setModelLower(double modelLower) {
        this.modelLower = modelLower;
    }

    public double getModelUpper() {
        return modelUpper;
    }

    public void setModelUpper(double modelUpper) {
        this.modelUpper = modelUpper;
    }

    public double getModelMedian() {
        return modelMedian;
    }

    public void setModelMedian(double modelMedian) {
        this.modelMedian = modelMedian;
    }

    public Double getActual() {
        return actual;
    }

    public void setActual(Double actual) {
        this.actual = actual;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof ModelPlot == false) {
            return false;
        }
        ModelPlot that = (ModelPlot) other;
        return Objects.equals(this.jobId, that.jobId) &&
                Objects.equals(this.timestamp, that.timestamp) &&
                Objects.equals(this.partitionFieldValue, that.partitionFieldValue) &&
                Objects.equals(this.partitionFieldName, that.partitionFieldName) &&
                Objects.equals(this.overFieldValue, that.overFieldValue) &&
                Objects.equals(this.overFieldName, that.overFieldName) &&
                Objects.equals(this.byFieldValue, that.byFieldValue) &&
                Objects.equals(this.byFieldName, that.byFieldName) &&
                Objects.equals(this.modelFeature, that.modelFeature) &&
                this.modelLower == that.modelLower &&
                this.modelUpper == that.modelUpper &&
                this.modelMedian == that.modelMedian &&
                Objects.equals(this.actual, that.actual) &&
                this.bucketSpan ==  that.bucketSpan &&
                this.detectorIndex == that.detectorIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, partitionFieldName, partitionFieldValue,
                overFieldName, overFieldValue, byFieldName, byFieldValue,
                modelFeature, modelLower, modelUpper, modelMedian, actual, bucketSpan, detectorIndex);
    }
}
