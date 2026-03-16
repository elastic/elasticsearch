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
 * @param enabled Whether approximation is enabled.
 * @param rows The number of rows used for query approximation.
 *             If {@code null}, the default will be used.
 * @param confidenceLevel The confidence level of the confidence intervals returned by query approximation.
 *                       If {@code null}, the default will be used.
 */
public record ApproximationSettings(boolean enabled, Integer rows, Double confidenceLevel) implements Writeable {
    public static final ApproximationSettings ENABLED = new ApproximationSettings(true, null, null);
    public static final ApproximationSettings DISABLED = new ApproximationSettings(false, null, null);

    /**
     * The default confidence level to use for query approximation if none is specified.
     */
    private static final double DEFAULT_CONFIDENCE_LEVEL = 0.90;

    /**
     * Sentinel value representing an explicit NULL for a field,
     * meaning "revert to null" during merge.
     */
    private static final int EXPLICIT_NULL = -1;

    /**
     * Returns the number of rows to be used for query approximation.
     * If null, the framework tries to pick a suitable number for the query.
     */
    @Override
    public Integer rows() {
        return rows != null && rows == EXPLICIT_NULL ? null : rows;
    }

    /**
     * Returns the request confidence level.
     * If none is set, the default level is returned.
     * If null is set, no confidence intervals should be computed.
     */
    @Override
    public Double confidenceLevel() {
        if (confidenceLevel == null) {
            return DEFAULT_CONFIDENCE_LEVEL;
        } else {
            return confidenceLevel == EXPLICIT_NULL ? null : confidenceLevel;
        }
    }

    private static final ObjectParser<ApproximationSettingsBuilder, Void> X_CONTENT_PARSER = new ObjectParser<>(
        "approximation",
        ApproximationSettingsBuilder::new
    );

    static {
        X_CONTENT_PARSER.declareIntOrNull(ApproximationSettingsBuilder::rows, EXPLICIT_NULL, new ParseField("rows"));
        X_CONTENT_PARSER.declareDoubleOrNull(
            ApproximationSettingsBuilder::confidenceLevel,
            EXPLICIT_NULL,
            new ParseField("confidence_level")
        );
    }

    public static ApproximationSettings fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
            return parser.booleanValue() ? ENABLED : DISABLED;
        } else {
            return X_CONTENT_PARSER.apply(parser, null).build();
        }
    }

    public static ApproximationSettings parse(Expression expression) {
        return switch (expression) {
            case Literal literal when literal.value() == null -> DISABLED;
            case Literal literal when literal.value() instanceof Boolean value -> value ? ENABLED : DISABLED;
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

        ApproximationSettingsBuilder builder = new ApproximationSettingsBuilder();
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

    /**
     * Returns a new settings with {@code override}'s non-null fields applied
     * on top of this instance.
     */
    public ApproximationSettings merge(ApproximationSettings override) {
        if (override == null) {
            return this;
        }
        return new ApproximationSettings(
            override.enabled,
            override.rows != null ? override.rows : this.rows,
            override.confidenceLevel != null ? override.confidenceLevel : this.confidenceLevel
        );
    }

    public ApproximationSettings(StreamInput in) throws IOException {
        this(in.readBoolean(), in.readOptionalInt(), in.readOptionalDouble());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeOptionalInt(rows());
        out.writeOptionalDouble(confidenceLevel());
    }

    private static class ApproximationSettingsBuilder {
        private Integer rows;
        private Double confidenceLevel;

        void rows(Integer rows) {
            if (rows == null || rows == EXPLICIT_NULL) {
                this.rows = EXPLICIT_NULL;
                return;
            }
            if (rows < 10000) {
                throw new IllegalArgumentException("Approximation configuration [rows] must be at least 10000");
            }
            this.rows = rows;
        }

        void confidenceLevel(Double confidenceLevel) {
            if (confidenceLevel == null || confidenceLevel == EXPLICIT_NULL) {
                this.confidenceLevel = (double) EXPLICIT_NULL;
                return;
            }
            if (confidenceLevel < 0.5 || confidenceLevel > 0.95) {
                throw new IllegalArgumentException("Approximation configuration [confidence_level] must be between 0.5 and 0.95");
            }
            this.confidenceLevel = confidenceLevel;
        }

        ApproximationSettings build() {
            return new ApproximationSettings(true, rows, confidenceLevel);
        }
    }
}
