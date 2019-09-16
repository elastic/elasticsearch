/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class OutlierDetection implements DataFrameAnalysis {

    public static final ParseField NAME = new ParseField("outlier_detection");

    public static final ParseField N_NEIGHBORS = new ParseField("n_neighbors");
    public static final ParseField METHOD = new ParseField("method");
    public static final ParseField FEATURE_INFLUENCE_THRESHOLD = new ParseField("feature_influence_threshold");

    private static final ConstructingObjectParser<OutlierDetection, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<OutlierDetection, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<OutlierDetection, Void> createParser(boolean lenient) {
        ConstructingObjectParser<OutlierDetection, Void> parser = new ConstructingObjectParser<>(NAME.getPreferredName(), lenient,
            a -> new OutlierDetection((Integer) a[0], (Method) a[1], (Double) a[2]));
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), N_NEIGHBORS);
        parser.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Method.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, METHOD, ObjectParser.ValueType.STRING);
        parser.declareDouble(ConstructingObjectParser.optionalConstructorArg(), FEATURE_INFLUENCE_THRESHOLD);
        return parser;
    }

    public static OutlierDetection fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final Integer nNeighbors;
    private final Method method;
    private final Double featureInfluenceThreshold;

    /**
     * Constructs the outlier detection configuration
     * @param nNeighbors The number of neighbors. Leave unspecified for dynamic detection.
     * @param method The method. Leave unspecified for a dynamic mixture of methods.
     * @param featureInfluenceThreshold The min outlier score required to calculate feature influence. Defaults to 0.1.
     */
    public OutlierDetection(@Nullable Integer nNeighbors, @Nullable Method method, @Nullable Double featureInfluenceThreshold) {
        if (nNeighbors != null && nNeighbors <= 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a positive integer", N_NEIGHBORS.getPreferredName());
        }

        if (featureInfluenceThreshold != null && (featureInfluenceThreshold < 0.0 || featureInfluenceThreshold > 1.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be in [0, 1]", FEATURE_INFLUENCE_THRESHOLD.getPreferredName());
        }

        this.nNeighbors = nNeighbors;
        this.method = method;
        this.featureInfluenceThreshold = featureInfluenceThreshold;
    }

    /**
     * Constructs the default outlier detection configuration
     */
    public OutlierDetection() {
        this(null, null, null);
    }

    public OutlierDetection(StreamInput in) throws IOException {
        nNeighbors = in.readOptionalVInt();
        method = in.readBoolean() ? in.readEnum(Method.class) : null;
        featureInfluenceThreshold = in.readOptionalDouble();
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
            && Objects.equals(featureInfluenceThreshold, that.featureInfluenceThreshold);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nNeighbors, method, featureInfluenceThreshold);
    }

    @Override
    public Map<String, Object> getParams() {
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
        return params;
    }

    @Override
    public boolean supportsCategoricalFields() {
        return false;
    }

    @Override
    public List<RequiredField> getRequiredFields() {
        return Collections.emptyList();
    }

    @Override
    public boolean supportsMissingValues() {
        return false;
    }

    public enum Method {
        LOF, LDOF, DISTANCE_KTH_NN, DISTANCE_KNN;

        public static Method fromString(String value) {
            return Method.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
