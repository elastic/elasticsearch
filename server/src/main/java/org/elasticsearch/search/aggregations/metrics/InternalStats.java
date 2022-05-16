/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalStats extends InternalNumericMetricsAggregation.MultiValue implements Stats {
    enum Metrics {

        count,
        sum,
        min,
        max,
        avg;

        public static Metrics resolve(String name) {
            return Metrics.valueOf(name);
        }
    }

    static final Set<String> METRIC_NAMES = Collections.unmodifiableSet(
        Stream.of(Metrics.values()).map(Metrics::name).collect(Collectors.toSet())
    );

    protected final long count;
    protected final double min;
    protected final double max;
    protected final double sum;

    public InternalStats(
        String name,
        long count,
        double sum,
        double min,
        double max,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, formatter, metadata);
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
    }

    /**
     * Read from a stream.
     */
    public InternalStats(StreamInput in) throws IOException {
        super(in);
        count = in.readVLong();
        min = in.readDouble();
        max = in.readDouble();
        sum = in.readDouble();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeVLong(count);
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeDouble(sum);
        writeOtherStatsTo(out);
    }

    protected void writeOtherStatsTo(StreamOutput out) throws IOException {}

    @Override
    public String getWriteableName() {
        return StatsAggregationBuilder.NAME;
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public double getMin() {
        return min;
    }

    @Override
    public double getMax() {
        return max;
    }

    @Override
    public double getAvg() {
        return sum / count;
    }

    @Override
    public double getSum() {
        return sum;
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
    public String getAvgAsString() {
        return valueAsString(Metrics.avg.name());
    }

    @Override
    public String getSumAsString() {
        return valueAsString(Metrics.sum.name());
    }

    @Override
    public double value(String name) {
        Metrics metrics = Metrics.valueOf(name);
        return switch (metrics) {
            case min -> this.min;
            case max -> this.max;
            case avg -> this.getAvg();
            case count -> this.count;
            case sum -> this.sum;
        };
    }

    @Override
    public Iterable<String> valueNames() {
        return METRIC_NAMES;
    }

    @Override
    public InternalStats reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        long count = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);

        for (InternalAggregation aggregation : aggregations) {
            InternalStats stats = (InternalStats) aggregation;
            count += stats.getCount();
            min = Math.min(min, stats.getMin());
            max = Math.max(max, stats.getMax());
            // Compute the sum of double values with Kahan summation algorithm which is more
            // accurate than naive summation.
            kahanSummation.add(stats.getSum());
        }
        return new InternalStats(name, count, kahanSummation.value(), min, max, format, getMetadata());
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalStats(name, samplingContext.scaleUp(count), samplingContext.scaleUp(sum), min, max, format, getMetadata());
    }

    static class Fields {
        public static final String COUNT = "count";
        public static final String MIN = "min";
        public static final String MIN_AS_STRING = "min_as_string";
        public static final String MAX = "max";
        public static final String MAX_AS_STRING = "max_as_string";
        public static final String AVG = "avg";
        public static final String AVG_AS_STRING = "avg_as_string";
        public static final String SUM = "sum";
        public static final String SUM_AS_STRING = "sum_as_string";
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.COUNT, count);
        if (count != 0) {
            builder.field(Fields.MIN, min);
            builder.field(Fields.MAX, max);
            builder.field(Fields.AVG, getAvg());
            builder.field(Fields.SUM, sum);
            if (format != DocValueFormat.RAW) {
                builder.field(Fields.MIN_AS_STRING, format.format(min));
                builder.field(Fields.MAX_AS_STRING, format.format(max));
                builder.field(Fields.AVG_AS_STRING, format.format(getAvg()));
                builder.field(Fields.SUM_AS_STRING, format.format(sum));
            }
        } else {
            builder.nullField(Fields.MIN);
            builder.nullField(Fields.MAX);
            builder.nullField(Fields.AVG);
            builder.field(Fields.SUM, 0.0d);
        }
        otherStatsToXContent(builder, params);
        return builder;
    }

    protected XContentBuilder otherStatsToXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), count, min, max, sum);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalStats other = (InternalStats) obj;
        return count == other.count
            && Double.compare(min, other.min) == 0
            && Double.compare(max, other.max) == 0
            && Double.compare(sum, other.sum) == 0;
    }
}
