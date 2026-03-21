/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;

import java.io.IOException;
import java.util.Map;

/**
 * Settings for query approximation.
 * @param rows The number of rows used for query approximation.
 *             If {@code null}, the default will be used.
 * @param confidenceLevel The confidence level of the confidence intervals returned by query approximation.
 *                       If {@code null}, the default will be used.
 */
public record ApproximationSettings(Integer rows, Double confidenceLevel) implements Writeable {

    /**
     * Default confidence level to use for query approximation if none is specified.
     */
    private static final double DEFAULT_CONFIDENCE_LEVEL = 0.90;

    /**
     * Default approximation settings, when it's enabled but no specific settings are provided.
     */
    public static final ApproximationSettings DEFAULT = new ApproximationSettings(null, DEFAULT_CONFIDENCE_LEVEL);

    /**
     * Sentinel approximation settings representing an explicit NULL, meaning "revert to null" during merge.
     */
    public static final ApproximationSettings EXPLICIT_NULL = new ApproximationSettings(
        Builder.EXPLICIT_NULL,
        (double) Builder.EXPLICIT_NULL
    );

    /**
     * Returns the number of rows to be used for query approximation.
     * If null, the framework tries to pick a suitable number for the query.
     */
    @Override
    public Integer rows() {
        return rows == null || rows == Builder.EXPLICIT_NULL ? null : rows;
    }

    /**
     * Returns the request confidence level.
     * If none is set, the default level is returned.
     * If null is explicitly set, no confidence intervals should be computed.
     */
    @Override
    public Double confidenceLevel() {
        return confidenceLevel == null || confidenceLevel == Builder.EXPLICIT_NULL ? null : confidenceLevel;
    }

    private static final ObjectParser<Builder, Void> X_CONTENT_PARSER = new ObjectParser<>("approximation", () -> new Builder(true));

    static {
        X_CONTENT_PARSER.declareIntOrNull(Builder::rows, Builder.EXPLICIT_NULL, new ParseField("rows"));
        X_CONTENT_PARSER.declareDoubleOrNull(Builder::confidenceLevel, Builder.EXPLICIT_NULL, new ParseField("confidence_level"));
    }

    public static ApproximationSettings fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
            return parser.booleanValue() ? DEFAULT : EXPLICIT_NULL;
        } else {
            return X_CONTENT_PARSER.apply(parser, null).build();
        }
    }

    public static ApproximationSettings parse(Expression expression) {
        return switch (expression) {
            case Literal literal when literal.value() == null -> EXPLICIT_NULL;
            case Literal literal when literal.value() instanceof Boolean value -> value ? DEFAULT : EXPLICIT_NULL;
            case MapExpression mapExpression -> parse(mapExpression);
            default -> throw new IllegalArgumentException("Invalid approximation configuration [" + expression + "]");
        };
    }

    private static ApproximationSettings parse(MapExpression expression) {
        Map<String, Object> map;
        try {
            map = expression.toFoldedMap(FoldContext.small());
        } catch (IllegalStateException ex) {
            throw new IllegalArgumentException("Approximation configuration must be a constant value [" + expression + "]");
        }

        Builder builder = new Builder(true);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            switch (entry.getKey()) {
                case "rows":
                    if (entry.getValue() != null && entry.getValue() instanceof Integer == false) {
                        throw new IllegalArgumentException("Approximation configuration [rows] must be an integer value");
                    }
                    builder.rows((Integer) entry.getValue());
                    break;
                case "confidence_level":
                    if (entry.getValue() != null && entry.getValue() instanceof Double == false) {
                        throw new IllegalArgumentException("Approximation configuration [confidence_level] must be a double value");
                    }
                    builder.confidenceLevel((Double) entry.getValue());
                    break;
                default:
                    throw new IllegalArgumentException("Approximation configuration contains unknown key [" + entry.getKey() + "]");
            }
        }
        return builder.build();
    }

    public ApproximationSettings(StreamInput in) throws IOException {
        this(in.readOptionalInt(), in.readOptionalDouble());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(rows());
        out.writeOptionalDouble(confidenceLevel());
    }

    public static class Builder {
        /**
         * Sentinel value representing an explicit NULL for a field,
         * meaning "revert to null" during merge.
         */
        private static final int EXPLICIT_NULL = -1;

        private boolean enabled;
        private Integer rows = null;
        private Double confidenceLevel = 0.90;

        public Builder(boolean enabled) {
            this.enabled = enabled;
        }

        public Builder rows(Integer rows) {
            if (rows == null || rows == EXPLICIT_NULL) {
                this.rows = EXPLICIT_NULL;
            } else if (rows < 10000) {
                throw new IllegalArgumentException("Approximation configuration [rows] must be at least 10000");
            } else {
                this.rows = rows;
            }
            return this;
        }

        public Builder confidenceLevel(Double confidenceLevel) {
            if (confidenceLevel == null || confidenceLevel == EXPLICIT_NULL) {
                this.confidenceLevel = (double) EXPLICIT_NULL;
            } else if (confidenceLevel < 0.5 || confidenceLevel > 0.95) {
                throw new IllegalArgumentException("Approximation configuration [confidence_level] must be between 0.5 and 0.95");
            } else {
                this.confidenceLevel = confidenceLevel;
            }
            return this;
        }

        /**
         * Override with the provided settings' non-null fields.
         */
        public Builder merge(ApproximationSettings override) {
            if (override == null) {
                return this;
            }
            enabled = (override != ApproximationSettings.EXPLICIT_NULL);
            if (override.rows != null) {
                rows = override.rows;
            }
            if (override.confidenceLevel != null) {
                confidenceLevel = override.confidenceLevel;
            }
            return this;
        }

        public ApproximationSettings build() {
            return enabled ? new ApproximationSettings(rows, confidenceLevel) : null;
        }
    }
}
