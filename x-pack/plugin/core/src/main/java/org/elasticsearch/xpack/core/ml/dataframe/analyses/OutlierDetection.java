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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class OutlierDetection implements DataFrameAnalysis {

    public static final ParseField NAME = new ParseField("outlier_detection");

    public static final ParseField K = new ParseField("k");
    public static final ParseField METHOD = new ParseField("method");

    private static final ConstructingObjectParser<OutlierDetection, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<OutlierDetection, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<OutlierDetection, Void> createParser(boolean lenient) {
        ConstructingObjectParser<OutlierDetection, Void> parser = new ConstructingObjectParser<>(NAME.getPreferredName(), lenient,
            a -> new OutlierDetection((Integer) a[0], (Method) a[1]));
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), K);
        parser.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Method.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, METHOD, ObjectParser.ValueType.STRING);
        return parser;
    }

    public static OutlierDetection fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final Integer k;
    private final Method method;

    /**
     * Constructs the outlier detection configuration
     * @param k The number of neighbors. Leave unspecified for dynamic detection.
     * @param method The method. Leave unspecified for a dynamic mixture of methods.
     */
    public OutlierDetection(@Nullable Integer k, @Nullable Method method) {
        if (k != null && k <= 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a positive integer", K.getPreferredName());
        }

        this.k = k;
        this.method = method;
    }

    /**
     * Constructs the default outlier detection configuration
     */
    public OutlierDetection() {
        this(null, null);
    }

    public OutlierDetection(StreamInput in) throws IOException {
        k = in.readOptionalVInt();
        method = in.readBoolean() ? in.readEnum(Method.class) : null;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(k);

        if (method != null) {
            out.writeBoolean(true);
            out.writeEnum(method);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (k != null) {
            builder.field(K.getPreferredName(), k);
        }
        if (method != null) {
            builder.field(METHOD.getPreferredName(), method);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutlierDetection that = (OutlierDetection) o;
        return Objects.equals(k, that.k) && Objects.equals(method, that.method);
    }

    @Override
    public int hashCode() {
        return Objects.hash(k, method);
    }

    @Override
    public Map<String, Object> getParams() {
        Map<String, Object> params = new HashMap<>();
        if (k != null) {
            // TODO change this to the constant NEIGHBORS when c++ is updated to match
            params.put(K.getPreferredName(), k);
        }
        if (method != null) {
            params.put(METHOD.getPreferredName(), method);
        }
        return params;
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
