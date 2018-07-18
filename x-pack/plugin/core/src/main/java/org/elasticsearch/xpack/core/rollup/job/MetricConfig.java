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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
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
public class MetricConfig implements Writeable, ToXContentFragment {
    private static final String NAME = "metric_config";

    private String field;
    private List<String> metrics;

    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField METRICS = new ParseField("metrics");

    // TODO: replace these with an enum
    private static final ParseField MIN = new ParseField("min");
    private static final ParseField MAX = new ParseField("max");
    private static final ParseField SUM = new ParseField("sum");
    private static final ParseField AVG = new ParseField("avg");
    private static final ParseField VALUE_COUNT = new ParseField("value_count");

    public static final ObjectParser<MetricConfig.Builder, Void> PARSER = new ObjectParser<>(NAME, MetricConfig.Builder::new);

    static {
        PARSER.declareString(MetricConfig.Builder::setField, FIELD);
        PARSER.declareStringArray(MetricConfig.Builder::setMetrics, METRICS);
    }

    MetricConfig(String name, List<String> metrics) {
        this.field = name;
        this.metrics = metrics;
    }

    MetricConfig(StreamInput in) throws IOException {
        field = in.readString();
        metrics = in.readList(StreamInput::readString);
    }

    public String getField() {
        return field;
    }

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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD.getPreferredName(), field);
        builder.field(METRICS.getPreferredName(), metrics);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeStringList(metrics);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        MetricConfig that = (MetricConfig) other;

        return Objects.equals(this.field, that.field)
                && Objects.equals(this.metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, metrics);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }


    public static class Builder {
        private String field;
        private List<String> metrics;

        public Builder() {
        }

        public Builder(MetricConfig config) {
            this.field = config.getField();
            this.metrics = config.getMetrics();
        }

        public String getField() {
            return field;
        }

        public MetricConfig.Builder setField(String field) {
            this.field = field;
            return this;
        }

        public List<String> getMetrics() {
            return metrics;
        }

        public MetricConfig.Builder setMetrics(List<String> metrics) {
            this.metrics = metrics;
            return this;
        }

        public MetricConfig build() {
            if (Strings.isNullOrEmpty(field) == true) {
                throw new IllegalArgumentException("Parameter [" + FIELD.getPreferredName() + "] must be a non-null, non-empty string.");
            }
            if (metrics == null || metrics.isEmpty()) {
                throw new IllegalArgumentException("Parameter [" + METRICS.getPreferredName()
                        + "] must be a non-null, non-empty array of strings.");
            }
            metrics.forEach(m -> {
                if (RollupField.SUPPORTED_METRICS.contains(m) == false) {
                    throw new IllegalArgumentException("Unsupported metric [" + m + "].  " +
                            "Supported metrics include: " + RollupField.SUPPORTED_METRICS);
                }
            });
            return new MetricConfig(field, metrics);
        }
    }
}
