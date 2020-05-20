/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class InternalBoxplot extends InternalNumericMetricsAggregation.MultiValue implements Boxplot {

    enum Metrics {

        MIN, MAX, Q1, Q2, Q3;

        public static Metrics resolve(String name) {
            return Metrics.valueOf(name.toUpperCase(Locale.ROOT));
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }

        double value(InternalBoxplot boxplot) {
            switch (this) {
                case MIN:
                    return boxplot.getMin();
                case MAX:
                    return boxplot.getMax();
                case Q1:
                    return boxplot.getQ1();
                case Q2:
                    return boxplot.getQ2();
                case Q3:
                    return boxplot.getQ3();
                default:
                    throw new IllegalArgumentException("Unknown value [" + this.value() + "] in the boxplot aggregation");
            }
        }

        double value(TDigestState state) {
            switch (this) {
                case MIN:
                    return state == null ? Double.NEGATIVE_INFINITY : state.getMin();
                case MAX:
                    return state == null ? Double.POSITIVE_INFINITY : state.getMax();
                case Q1:
                    return state == null ? Double.NaN : state.quantile(0.25);
                case Q2:
                    return state == null ? Double.NaN : state.quantile(0.5);
                case Q3:
                    return state == null ? Double.NaN : state.quantile(0.75);
                default:
                    throw new IllegalArgumentException("Unknown value [" + this.value() + "] in the boxplot aggregation");
            }
        }
    }

    private final TDigestState state;

    InternalBoxplot(String name, TDigestState state, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, metadata);
        this.state = state;
        this.format = formatter;
    }

    /**
     * Read from a stream.
     */
    public InternalBoxplot(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        state = TDigestState.read(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        TDigestState.write(state, out);
    }

    @Override
    public String getWriteableName() {
        return BoxplotAggregationBuilder.NAME;
    }

    @Override
    public double getMin() {
        return state.getMin();
    }

    @Override
    public double getMax() {
        return state.getMax();
    }

    @Override
    public double getQ1() {
        return state.quantile(0.25);
    }

    @Override
    public double getQ2() {
        return state.quantile(0.5);
    }

    @Override
    public double getQ3() {
        return state.quantile(0.75);
    }

    @Override
    public String getMinAsString() {
        return valueAsString(Metrics.MIN.name());
    }

    @Override
    public String getMaxAsString() {
        return valueAsString(Metrics.MAX.name());
    }

    @Override
    public String getQ1AsString() {
        return valueAsString(Metrics.Q1.name());
    }

    @Override
    public String getQ2AsString() {
        return valueAsString(Metrics.Q2.name());
    }

    @Override
    public String getQ3AsString() {
        return valueAsString(Metrics.Q3.name());
    }

    @Override
    public double value(String name) {
        return Metrics.resolve(name).value(this);
    }

    // for testing only
    DocValueFormat format() {
        return format;
    }

    // for testing only
    TDigestState state() {
        return state;
    }

    @Override
    public InternalBoxplot reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        TDigestState merged = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalBoxplot percentiles = (InternalBoxplot) aggregation;
            if (merged == null) {
                merged = new TDigestState(percentiles.state.compression());
            }
            merged.add(percentiles.state);
        }
        return new InternalBoxplot(name, merged, format, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("min", getMin());
        builder.field("max", getMax());
        builder.field("q1", getQ1());
        builder.field("q2", getQ2());
        builder.field("q3", getQ3());
        if (format != DocValueFormat.RAW) {
            builder.field("min_as_string", format.format(getMin()));
            builder.field("max_as_string", format.format(getMax()));
            builder.field("q1_as_string", format.format(getQ1()));
            builder.field("q2_as_string", format.format(getQ2()));
            builder.field("q3_as_string", format.format(getQ3()));
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), state);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalBoxplot that = (InternalBoxplot) obj;
        return Objects.equals(state, that.state);
    }
}

