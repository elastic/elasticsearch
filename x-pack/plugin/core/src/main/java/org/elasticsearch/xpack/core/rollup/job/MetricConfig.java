/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
    private static final ParseField MIN = new ParseField("min");
    private static final ParseField MAX = new ParseField("max");
    private static final ParseField SUM = new ParseField("sum");
    private static final ParseField AVG = new ParseField("avg");
    private static final ParseField VALUE_COUNT = new ParseField("value_count");

    private static final String NAME = "metrics";
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

    MetricConfig(final StreamInput in) throws IOException {
        field = in.readString();
        metrics = in.readList(StreamInput::readString);
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

    /**
     * This returns a set of aggregation builders which represent the configured
     * set of metrics.  Used by the rollup indexer to iterate over historical data
     */
    public List<ValuesSourceAggregationBuilder.LeafOnly> toBuilders() {
        if (metrics.size() == 0) {
            return Collections.emptyList();
        }

        List<ValuesSourceAggregationBuilder.LeafOnly> aggs = new ArrayList<>(metrics.size());
        for (String metric : metrics) {
            ValuesSourceAggregationBuilder.LeafOnly newBuilder;
            if (metric.equals(MIN.getPreferredName())) {
                newBuilder = new MinAggregationBuilder(RollupField.formatFieldName(field, MinAggregationBuilder.NAME, RollupField.VALUE));
            } else if (metric.equals(MAX.getPreferredName())) {
                newBuilder = new MaxAggregationBuilder(RollupField.formatFieldName(field, MaxAggregationBuilder.NAME, RollupField.VALUE));
            } else if (metric.equals(AVG.getPreferredName())) {
                // Avgs are sum + count
                newBuilder = new SumAggregationBuilder(RollupField.formatFieldName(field, AvgAggregationBuilder.NAME, RollupField.VALUE));
                ValuesSourceAggregationBuilder.LeafOnly countBuilder
                        = new ValueCountAggregationBuilder(
                                RollupField.formatFieldName(field, AvgAggregationBuilder.NAME, RollupField.COUNT_FIELD), ValueType.NUMERIC);
                countBuilder.field(field);
                aggs.add(countBuilder);
            } else if (metric.equals(SUM.getPreferredName())) {
                newBuilder = new SumAggregationBuilder(RollupField.formatFieldName(field, SumAggregationBuilder.NAME, RollupField.VALUE));
            } else if (metric.equals(VALUE_COUNT.getPreferredName())) {
                // TODO allow non-numeric value_counts.
                // Hardcoding this is fine for now since the job validation guarantees that all metric fields are numerics
                newBuilder = new ValueCountAggregationBuilder(
                        RollupField.formatFieldName(field, ValueCountAggregationBuilder.NAME, RollupField.VALUE), ValueType.NUMERIC);
            } else {
                throw new IllegalArgumentException("Unsupported metric type [" + metric + "]");
            }
            newBuilder.field(field);
            aggs.add(newBuilder);
        }
        return aggs;
    }

    /**
     * @return A map representing this config object as a RollupCaps aggregation object
     */
    public List<Map<String, Object>> toAggCap() {
        return metrics.stream().map(metric -> Collections.singletonMap("agg", (Object)metric)).collect(Collectors.toList());
    }

    public void validateMappings(Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
                                 ActionRequestValidationException validationException) {

        Map<String, FieldCapabilities> fieldCaps = fieldCapsResponse.get(field);
        if (fieldCaps != null && fieldCaps.isEmpty() == false) {
            fieldCaps.forEach((key, value) -> {
                if (RollupField.NUMERIC_FIELD_MAPPER_TYPES.contains(key)) {
                    if (value.isAggregatable() == false) {
                        validationException.addValidationError("The field [" + field + "] must be aggregatable across all indices, " +
                                "but is not.");
                    }
                } else {
                    validationException.addValidationError("The field referenced by a metric group must be a [numeric] type, but found " +
                            fieldCaps.keySet().toString() + " for field [" + field + "]");
                }
            });
        } else {
            validationException.addValidationError("Could not find a [numeric] field with name [" + field + "] in any of the " +
                    "indices matching the index pattern.");
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
        out.writeStringList(metrics);
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
}
