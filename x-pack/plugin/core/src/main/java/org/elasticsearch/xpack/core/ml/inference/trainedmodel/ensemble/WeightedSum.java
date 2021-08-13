/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;


import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class WeightedSum implements StrictlyParsedOutputAggregator, LenientlyParsedOutputAggregator {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(WeightedSum.class);
    public static final ParseField NAME = new ParseField("weighted_sum");
    public static final ParseField WEIGHTS = new ParseField("weights");

    private static final ConstructingObjectParser<WeightedSum, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<WeightedSum, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<WeightedSum, Void> createParser(boolean lenient) {
        ConstructingObjectParser<WeightedSum, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new WeightedSum((List<Double>)a[0]));
        parser.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), WEIGHTS);
        return parser;
    }

    public static WeightedSum fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static WeightedSum fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private final double[] weights;

    WeightedSum() {
        this((List<Double>) null);
    }

    private WeightedSum(List<Double> weights) {
        this(weights == null ? null : weights.stream().mapToDouble(Double::valueOf).toArray());
    }

    public WeightedSum(double[] weights) {
        this.weights = weights;
    }

    public WeightedSum(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            this.weights = in.readDoubleArray();
        } else {
            this.weights = null;
        }
    }

    @Override
    public double[] processValues(double[][] values) {
        Objects.requireNonNull(values, "values must not be null");
        assert values[0].length == 1;
        if (weights == null) {
            return Arrays.stream(values).mapToDouble(v -> v[0]).toArray();
        }
        if (values.length != weights.length) {
            throw new IllegalArgumentException("values must be the same length as weights.");
        }
        return IntStream.range(0, weights.length).mapToDouble(i -> values[i][0] * weights[i]).toArray();
    }

    @Override
    public double aggregate(double[] values) {
        Objects.requireNonNull(values, "values must not be null");
        if (values.length == 0) {
            throw new IllegalArgumentException("values must not be empty");
        }
        return Arrays.stream(values).sum();
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(weights != null);
        if (weights != null) {
            out.writeDoubleArray(weights);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (weights != null) {
            builder.field(WEIGHTS.getPreferredName(), weights);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedSum that = (WeightedSum) o;
        return Arrays.equals(weights, that.weights);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(weights);
    }

    @Override
    public Integer expectedValueSize() {
        return weights == null ? null : this.weights.length;
    }

    @Override
    public boolean compatibleWith(TargetType targetType) {
        return TargetType.REGRESSION.equals(targetType);
    }

    @Override
    public long ramBytesUsed() {
        long weightSize = weights == null ? 0L : RamUsageEstimator.sizeOf(weights);
        return SHALLOW_SIZE + weightSize;
    }
}
