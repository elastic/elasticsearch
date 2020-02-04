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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalBoxplot extends InternalNumericMetricsAggregation.MultiValue implements Boxplot {

    enum Metrics {

        min, max, q1, q2, q3;

        public static Metrics resolve(String name) {
            return Metrics.valueOf(name);
        }
    }

    private final TDigestState state;

    InternalBoxplot(String name, TDigestState state, DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
                    Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
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
        return valueAsString(Metrics.min.name());
    }

    @Override
    public String getMaxAsString() {
        return valueAsString(Metrics.max.name());
    }

    @Override
    public String getQ1AsString() {
        return valueAsString(Metrics.q1.name());
    }

    @Override
    public String getQ2AsString() {
        return valueAsString(Metrics.q2.name());
    }

    @Override
    public String getQ3AsString() {
        return valueAsString(Metrics.q3.name());
    }

    @Override
    public double value(String name) {
        Metrics metrics = Metrics.valueOf(name);
        switch (metrics) {
            case min:
                return getMin();
            case max:
                return getMax();
            case q1:
                return getQ1();
            case q2:
                return getQ2();
            case q3:
                return getQ3();
            default:
                throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
        }
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
        return new InternalBoxplot(name, merged, format, pipelineAggregators(), metaData);
    }

    static class Fields {
        public static final String MIN = "min";
        public static final String MIN_AS_STRING = "min_as_string";
        public static final String MAX = "max";
        public static final String MAX_AS_STRING = "max_as_string";
        public static final String Q1 = "q1";
        public static final String Q1_AS_STRING = "q1_as_string";
        public static final String Q2 = "q2";
        public static final String Q2_AS_STRING = "q2_as_string";
        public static final String Q3 = "q3";
        public static final String Q3_AS_STRING = "q3_as_string";
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.MIN, getMin());
        builder.field(Fields.MAX, getMax());
        builder.field(Fields.Q1, getQ1());
        builder.field(Fields.Q2, getQ2());
        builder.field(Fields.Q3, getQ3());
        if (format != DocValueFormat.RAW) {
            builder.field(Fields.MIN_AS_STRING, format.format(getMin()));
            builder.field(Fields.MAX_AS_STRING, format.format(getMax()));
            builder.field(Fields.Q1_AS_STRING, format.format(getQ1()));
            builder.field(Fields.Q2_AS_STRING, format.format(getQ2()));
            builder.field(Fields.Q3_AS_STRING, format.format(getQ3()));
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

