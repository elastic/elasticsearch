/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class OutlierDetection implements DataFrameAnalysis {

    public static final ParseField NAME = new ParseField("outlier_detection");

    public static final ParseField N_NEIGHBORS = new ParseField("n_neighbors");
    public static final ParseField METHOD = new ParseField("method");
    public static final ParseField FEATURE_INFLUENCE_THRESHOLD = new ParseField("feature_influence_threshold");
    public static final ParseField COMPUTE_FEATURE_INFLUENCE = new ParseField("compute_feature_influence");
    public static final ParseField OUTLIER_FRACTION = new ParseField("outlier_fraction");
    public static final ParseField STANDARDIZATION_ENABLED = new ParseField("standardization_enabled");

    private static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Builder, Void> createParser(boolean lenient) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(NAME.getPreferredName(), lenient, Builder::new);
        parser.declareInt(Builder::setNNeighbors, N_NEIGHBORS);
        parser.declareString(Builder::setMethod, Method::fromString, METHOD);
        parser.declareDouble(Builder::setFeatureInfluenceThreshold, FEATURE_INFLUENCE_THRESHOLD);
        parser.declareBoolean(Builder::setComputeFeatureInfluence, COMPUTE_FEATURE_INFLUENCE);
        parser.declareDouble(Builder::setOutlierFraction, OUTLIER_FRACTION);
        parser.declareBoolean(Builder::setStandardizationEnabled, STANDARDIZATION_ENABLED);
        return parser;
    }

    public static OutlierDetection fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null).build() : STRICT_PARSER.apply(parser, null).build();
    }

    private static final List<String> PROGRESS_PHASES = Collections.singletonList("computing_outliers");

    static final Map<String, Object> FEATURE_INFLUENCE_MAPPING;
    static {
        Map<String, Object> properties = new HashMap<>();
        properties.put("feature_name", Collections.singletonMap("type", KeywordFieldMapper.CONTENT_TYPE));
        properties.put("influence", Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("dynamic", false);
        mapping.put("type", NestedObjectMapper.CONTENT_TYPE);
        mapping.put("properties", properties);

        FEATURE_INFLUENCE_MAPPING = Collections.unmodifiableMap(mapping);
    }

    /**
     * The number of neighbors. Leave unspecified for dynamic detection.
     */
    @Nullable
    private final Integer nNeighbors;

    /**
     * The method. Leave unspecified for a dynamic mixture of methods.
     */
    @Nullable
    private final Method method;

    /**
     * The min outlier score required to calculate feature influence. Defaults to 0.1.
     */
    @Nullable
    private final Double featureInfluenceThreshold;

    /**
     * Whether to compute feature influence or not. Defaults to true.
     */
    private final boolean computeFeatureInfluence;

    /**
     * The proportion of data assumed to be outlying prior to outlier detection. Defaults to 0.05.
     */
    private final double outlierFraction;

    /**
     * Whether to perform standardization.
     */
    private final boolean standardizationEnabled;

    private OutlierDetection(
        Integer nNeighbors,
        Method method,
        Double featureInfluenceThreshold,
        boolean computeFeatureInfluence,
        double outlierFraction,
        boolean standardizationEnabled
    ) {
        if (nNeighbors != null && nNeighbors <= 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a positive integer", N_NEIGHBORS.getPreferredName());
        }

        if (featureInfluenceThreshold != null && (featureInfluenceThreshold < 0.0 || featureInfluenceThreshold > 1.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be in [0, 1]", FEATURE_INFLUENCE_THRESHOLD.getPreferredName());
        }

        if (outlierFraction < 0.0 || outlierFraction > 1.0) {
            throw ExceptionsHelper.badRequestException("[{}] must be in [0, 1]", OUTLIER_FRACTION.getPreferredName());
        }

        this.nNeighbors = nNeighbors;
        this.method = method;
        this.featureInfluenceThreshold = featureInfluenceThreshold;
        this.computeFeatureInfluence = computeFeatureInfluence;
        this.outlierFraction = outlierFraction;
        this.standardizationEnabled = standardizationEnabled;
    }

    public OutlierDetection(StreamInput in) throws IOException {
        nNeighbors = in.readOptionalVInt();
        method = in.readBoolean() ? in.readEnum(Method.class) : null;
        featureInfluenceThreshold = in.readOptionalDouble();
        computeFeatureInfluence = in.readBoolean();
        outlierFraction = in.readDouble();
        standardizationEnabled = in.readBoolean();
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(nNeighbors);

        if (method != null) {
            out.writeBoolean(true);
            out.writeEnum(method);
        } else {
            out.writeBoolean(false);
        }

        out.writeOptionalDouble(featureInfluenceThreshold);

        out.writeBoolean(computeFeatureInfluence);
        out.writeDouble(outlierFraction);
        out.writeBoolean(standardizationEnabled);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (nNeighbors != null) {
            builder.field(N_NEIGHBORS.getPreferredName(), nNeighbors);
        }
        if (method != null) {
            builder.field(METHOD.getPreferredName(), method);
        }
        if (featureInfluenceThreshold != null) {
            builder.field(FEATURE_INFLUENCE_THRESHOLD.getPreferredName(), featureInfluenceThreshold);
        }
        builder.field(COMPUTE_FEATURE_INFLUENCE.getPreferredName(), computeFeatureInfluence);
        builder.field(OUTLIER_FRACTION.getPreferredName(), outlierFraction);
        builder.field(STANDARDIZATION_ENABLED.getPreferredName(), standardizationEnabled);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutlierDetection that = (OutlierDetection) o;
        return Objects.equals(nNeighbors, that.nNeighbors)
            && Objects.equals(method, that.method)
            && Objects.equals(featureInfluenceThreshold, that.featureInfluenceThreshold)
            && computeFeatureInfluence == that.computeFeatureInfluence
            && outlierFraction == that.outlierFraction
            && standardizationEnabled == that.standardizationEnabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            nNeighbors,
            method,
            featureInfluenceThreshold,
            computeFeatureInfluence,
            outlierFraction,
            standardizationEnabled
        );
    }

    @Override
    public Map<String, Object> getParams(FieldInfo fieldInfo) {
        Map<String, Object> params = new HashMap<>();
        if (nNeighbors != null) {
            params.put(N_NEIGHBORS.getPreferredName(), nNeighbors);
        }
        if (method != null) {
            params.put(METHOD.getPreferredName(), method);
        }
        if (featureInfluenceThreshold != null) {
            params.put(FEATURE_INFLUENCE_THRESHOLD.getPreferredName(), featureInfluenceThreshold);
        }
        params.put(COMPUTE_FEATURE_INFLUENCE.getPreferredName(), computeFeatureInfluence);
        params.put(OUTLIER_FRACTION.getPreferredName(), outlierFraction);
        params.put(STANDARDIZATION_ENABLED.getPreferredName(), standardizationEnabled);
        return params;
    }

    @Override
    public boolean supportsCategoricalFields() {
        return false;
    }

    @Override
    public Set<String> getAllowedCategoricalTypes(String fieldName) {
        return Collections.emptySet();
    }

    @Override
    public List<RequiredField> getRequiredFields() {
        return Collections.emptyList();
    }

    @Override
    public List<FieldCardinalityConstraint> getFieldCardinalityConstraints() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Object> getResultMappings(String resultsFieldName, FieldCapabilitiesResponse fieldCapabilitiesResponse) {
        Map<String, Object> additionalProperties = new HashMap<>();
        additionalProperties.put(
            resultsFieldName + ".outlier_score",
            Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName())
        );
        additionalProperties.put(resultsFieldName + ".feature_influence", FEATURE_INFLUENCE_MAPPING);
        return additionalProperties;
    }

    @Override
    public boolean supportsMissingValues() {
        return false;
    }

    @Override
    public boolean persistsState() {
        return false;
    }

    @Override
    public String getStateDocIdPrefix(String jobId) {
        throw new UnsupportedOperationException("Outlier detection does not support state");
    }

    @Override
    public List<String> getProgressPhases() {
        return PROGRESS_PHASES;
    }

    @Override
    public InferenceConfig inferenceConfig(FieldInfo fieldInfo) {
        return null;
    }

    @Override
    public boolean supportsInference() {
        return false;
    }

    public enum Method {
        LOF,
        LDOF,
        DISTANCE_KTH_NN,
        DISTANCE_KNN;

        public static Method fromString(String value) {
            return Method.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static class Builder {

        private Integer nNeighbors;
        private Method method;
        private Double featureInfluenceThreshold;
        private boolean computeFeatureInfluence = true;
        private double outlierFraction = 0.05;
        private boolean standardizationEnabled = true;

        public Builder() {}

        public Builder(OutlierDetection other) {
            this.nNeighbors = other.nNeighbors;
            this.method = other.method;
            this.featureInfluenceThreshold = other.featureInfluenceThreshold;
            this.computeFeatureInfluence = other.computeFeatureInfluence;
            this.outlierFraction = other.outlierFraction;
            this.standardizationEnabled = other.standardizationEnabled;
        }

        public Builder setNNeighbors(Integer nNeighbors) {
            this.nNeighbors = nNeighbors;
            return this;
        }

        public Builder setMethod(Method method) {
            this.method = method;
            return this;
        }

        public Builder setFeatureInfluenceThreshold(Double featureInfluenceThreshold) {
            this.featureInfluenceThreshold = featureInfluenceThreshold;
            return this;
        }

        public Builder setComputeFeatureInfluence(boolean computeFeatureInfluence) {
            this.computeFeatureInfluence = computeFeatureInfluence;
            return this;
        }

        public Builder setOutlierFraction(double outlierFraction) {
            this.outlierFraction = outlierFraction;
            return this;
        }

        public Builder setStandardizationEnabled(boolean standardizationEnabled) {
            this.standardizationEnabled = standardizationEnabled;
            return this;
        }

        public OutlierDetection build() {
            return new OutlierDetection(
                nNeighbors,
                method,
                featureInfluenceThreshold,
                computeFeatureInfluence,
                outlierFraction,
                standardizationEnabled
            );
        }
    }
}
