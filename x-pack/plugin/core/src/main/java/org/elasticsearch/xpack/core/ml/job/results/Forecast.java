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

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

/**
 * Model Forecast POJO.
 */
public class Forecast implements ToXContentObject, Writeable {
    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "model_forecast";
    public static final ParseField RESULTS_FIELD = new ParseField(RESULT_TYPE_VALUE);

    public static final ParseField FORECAST_ID = new ParseField("forecast_id");
    public static final ParseField PARTITION_FIELD_NAME = new ParseField("partition_field_name");
    public static final ParseField PARTITION_FIELD_VALUE = new ParseField("partition_field_value");
    public static final ParseField BY_FIELD_NAME = new ParseField("by_field_name");
    public static final ParseField BY_FIELD_VALUE = new ParseField("by_field_value");
    public static final ParseField MODEL_FEATURE = new ParseField("model_feature");
    public static final ParseField FORECAST_LOWER = new ParseField("forecast_lower");
    public static final ParseField FORECAST_UPPER = new ParseField("forecast_upper");
    public static final ParseField FORECAST_PREDICTION = new ParseField("forecast_prediction");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField DETECTOR_INDEX = new ParseField("detector_index");

    public static final ConstructingObjectParser<Forecast, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<Forecast, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Forecast, Void> parser = new ConstructingObjectParser<>(
            RESULT_TYPE_VALUE,
            ignoreUnknownFields,
            a -> new Forecast((String) a[0], (String) a[1], (Date) a[2], (long) a[3], (int) a[4])
        );

        parser.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        parser.declareString(ConstructingObjectParser.constructorArg(), FORECAST_ID);
        parser.declareField(
            ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeField(p, Result.TIMESTAMP.getPreferredName()),
            Result.TIMESTAMP,
            ValueType.VALUE
        );
        parser.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        parser.declareInt(ConstructingObjectParser.constructorArg(), DETECTOR_INDEX);
        parser.declareString((modelForecast, s) -> {}, Result.RESULT_TYPE);
        parser.declareString(Forecast::setPartitionFieldName, PARTITION_FIELD_NAME);
        parser.declareString(Forecast::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        parser.declareString(Forecast::setByFieldName, BY_FIELD_NAME);
        parser.declareString(Forecast::setByFieldValue, BY_FIELD_VALUE);
        parser.declareString(Forecast::setModelFeature, MODEL_FEATURE);
        parser.declareDouble(Forecast::setForecastLower, FORECAST_LOWER);
        parser.declareDouble(Forecast::setForecastUpper, FORECAST_UPPER);
        parser.declareDouble(Forecast::setForecastPrediction, FORECAST_PREDICTION);

        return parser;
    }

    private final String jobId;
    private final String forecastId;
    private final Date timestamp;
    private final long bucketSpan;
    private int detectorIndex;
    private String partitionFieldName;
    private String partitionFieldValue;
    private String byFieldName;
    private String byFieldValue;
    private String modelFeature;
    private double forecastLower;
    private double forecastUpper;
    private double forecastPrediction;

    public Forecast(String jobId, String forecastId, Date timestamp, long bucketSpan, int detectorIndex) {
        this.jobId = Objects.requireNonNull(jobId);
        this.forecastId = Objects.requireNonNull(forecastId);
        this.timestamp = timestamp;
        this.bucketSpan = bucketSpan;
        this.detectorIndex = detectorIndex;
    }

    public Forecast(StreamInput in) throws IOException {
        jobId = in.readString();
        forecastId = in.readString();
        timestamp = new Date(in.readLong());
        partitionFieldName = in.readOptionalString();
        partitionFieldValue = in.readOptionalString();
        byFieldName = in.readOptionalString();
        byFieldValue = in.readOptionalString();
        modelFeature = in.readOptionalString();
        forecastLower = in.readDouble();
        forecastUpper = in.readDouble();
        forecastPrediction = in.readDouble();
        bucketSpan = in.readLong();
        detectorIndex = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeString(forecastId);
        out.writeLong(timestamp.getTime());
        out.writeOptionalString(partitionFieldName);
        out.writeOptionalString(partitionFieldValue);
        out.writeOptionalString(byFieldName);
        out.writeOptionalString(byFieldValue);
        out.writeOptionalString(modelFeature);
        out.writeDouble(forecastLower);
        out.writeDouble(forecastUpper);
        out.writeDouble(forecastPrediction);
        out.writeLong(bucketSpan);
        out.writeInt(detectorIndex);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(FORECAST_ID.getPreferredName(), forecastId);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(DETECTOR_INDEX.getPreferredName(), detectorIndex);
        if (timestamp != null) {
            builder.timestampFieldsFromUnixEpochMillis(
                Result.TIMESTAMP.getPreferredName(),
                Result.TIMESTAMP.getPreferredName() + "_string",
                timestamp.getTime()
            );
        }
        if (partitionFieldName != null) {
            builder.field(PARTITION_FIELD_NAME.getPreferredName(), partitionFieldName);
        }
        if (partitionFieldValue != null) {
            builder.field(PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue);
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
        builder.field(FORECAST_LOWER.getPreferredName(), forecastLower);
        builder.field(FORECAST_UPPER.getPreferredName(), forecastUpper);
        builder.field(FORECAST_PREDICTION.getPreferredName(), forecastPrediction);
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public String getForecastId() {
        return forecastId;
    }

    public String getId() {
        return jobId
            + "_model_forecast_"
            + forecastId
            + "_"
            + timestamp.getTime()
            + "_"
            + bucketSpan
            + "_"
            + detectorIndex
            + "_"
            + MachineLearningField.valuesToId(byFieldValue, partitionFieldValue);
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public long getBucketSpan() {
        return bucketSpan;
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    public void setPartitionFieldName(String partitionFieldName) {
        this.partitionFieldName = partitionFieldName;
    }

    public int getDetectorIndex() {
        return detectorIndex;
    }

    public String getPartitionFieldValue() {
        return partitionFieldValue;
    }

    public void setPartitionFieldValue(String partitionFieldValue) {
        this.partitionFieldValue = partitionFieldValue;
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

    public double getForecastLower() {
        return forecastLower;
    }

    public void setForecastLower(double forecastLower) {
        this.forecastLower = forecastLower;
    }

    public double getForecastUpper() {
        return forecastUpper;
    }

    public void setForecastUpper(double forecastUpper) {
        this.forecastUpper = forecastUpper;
    }

    public double getForecastPrediction() {
        return forecastPrediction;
    }

    public void setForecastPrediction(double forecastPrediction) {
        this.forecastPrediction = forecastPrediction;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof Forecast == false) {
            return false;
        }
        Forecast that = (Forecast) other;
        return Objects.equals(this.jobId, that.jobId)
            && Objects.equals(this.forecastId, that.forecastId)
            && Objects.equals(this.timestamp, that.timestamp)
            && Objects.equals(this.partitionFieldValue, that.partitionFieldValue)
            && Objects.equals(this.partitionFieldName, that.partitionFieldName)
            && Objects.equals(this.byFieldValue, that.byFieldValue)
            && Objects.equals(this.byFieldName, that.byFieldName)
            && Objects.equals(this.modelFeature, that.modelFeature)
            && this.forecastLower == that.forecastLower
            && this.forecastUpper == that.forecastUpper
            && this.forecastPrediction == that.forecastPrediction
            && this.bucketSpan == that.bucketSpan
            && this.detectorIndex == that.detectorIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            forecastId,
            timestamp,
            partitionFieldName,
            partitionFieldValue,
            byFieldName,
            byFieldValue,
            modelFeature,
            forecastLower,
            forecastUpper,
            forecastPrediction,
            bucketSpan,
            detectorIndex
        );
    }
}
