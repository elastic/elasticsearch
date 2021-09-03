/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The configuration object for the metrics portion of a rollup job config
 *
 * {
 *     "metrics": [
 *        {
 *            "field": "foo",
 *            "metrics": [ "min", "max", "sum"]
 *        },
 *        {
 *            "field": "bar",
 *            "metrics": [ "max" ]
 *        }
 *     ]
 * }
 */
public class MetricConfig implements Writeable, ToXContentObject {

    // TODO: replace these with an enum
    public static final ParseField MIN = new ParseField("min");
    public static final ParseField MAX = new ParseField("max");
    public static final ParseField SUM = new ParseField("sum");
    public static final ParseField AVG = new ParseField("avg");
    public static final ParseField VALUE_COUNT = new ParseField("value_count");
    public static final String NAME = "metrics";

    private static final String FIELD = "field";
    private static final String METRICS = "metrics";
    private static final ConstructingObjectParser<MetricConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, args -> {
            @SuppressWarnings("unchecked") List<String> metrics = (List<String>) args[1];
            return new MetricConfig((String) args[0], metrics);
        });
        PARSER.declareString(constructorArg(), new ParseField(FIELD));
        PARSER.declareStringArray(constructorArg(), new ParseField(METRICS));
    }

    private final String field;
    private final List<String> metrics;

    public MetricConfig(final String field, final List<String> metrics) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("Field must be a non-null, non-empty string");
        }
        if (metrics == null || metrics.isEmpty()) {
            throw new IllegalArgumentException("Metrics must be a non-null, non-empty array of strings");
        }
        metrics.forEach(m -> {
            if (RollupField.SUPPORTED_METRICS.contains(m) == false) {
                throw new IllegalArgumentException("Unsupported metric [" + m + "]. " +
                    "Supported metrics include: " + RollupField.SUPPORTED_METRICS);
            }
        });
        this.field = field;
        this.metrics = metrics;
    }

    public MetricConfig(final StreamInput in) throws IOException {
        field = in.readString();
        metrics = in.readStringList();
    }

    /**
     * @return the name of the field used in the metric configuration. Never {@code null}.
     */
    public String getField() {
        return field;
    }

    /**
     * @return the names of the metrics used in the metric configuration. Never {@code null}.
     */
    public List<String> getMetrics() {
        return metrics;
    }

    public void validateMappings(Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
                                 ActionRequestValidationException validationException) {

        Map<String, FieldCapabilities> fieldCaps = fieldCapsResponse.get(field);
        if (fieldCaps != null && fieldCaps.isEmpty() == false) {
            fieldCaps.forEach((key, value) -> {
                if (value.isAggregatable() == false) {
                    validationException.addValidationError("The field [" + field + "] must be aggregatable across all indices, " +
                        "but is not.");
                }
                if (RollupField.NUMERIC_FIELD_MAPPER_TYPES.contains(key)) {
                    // nothing to do as all metrics are supported by SUPPORTED_NUMERIC_METRICS currently
                } else if (RollupField.DATE_FIELD_MAPPER_TYPES.contains(key)) {
                    if (RollupField.SUPPORTED_DATE_METRICS.containsAll(metrics) == false) {
                        validationException.addValidationError(
                            buildSupportedMetricError(key, RollupField.SUPPORTED_DATE_METRICS));
                    }
                } else {
                    validationException.addValidationError("The field referenced by a metric group must be a [numeric] or [" +
                        Strings.collectionToCommaDelimitedString(RollupField.DATE_FIELD_MAPPER_TYPES) + "] type, " +
                        "but found " + fieldCaps.keySet().toString() + " for field [" + field + "]");
                }
            });
        } else {
            validationException.addValidationError("Could not find a [numeric] or [" +
                Strings.collectionToCommaDelimitedString(RollupField.DATE_FIELD_MAPPER_TYPES) +
                "] field with name [" + field + "] in any of the " + "indices matching the index pattern.");
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(FIELD, field);
            builder.field(METRICS, metrics);
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeStringCollection(metrics);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final MetricConfig that = (MetricConfig) other;
        return Objects.equals(field, that.field) && Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, metrics);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static MetricConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private String buildSupportedMetricError(String type, List<String> supportedMetrics) {
        List<String> unsupportedMetrics = new ArrayList<>(metrics);
        unsupportedMetrics.removeAll(supportedMetrics);
        return "Only the metrics " + supportedMetrics + " are supported for [" + type + "] types," +
            " but unsupported metrics " + unsupportedMetrics + " supplied for field [" + field + "]";
    }
}
