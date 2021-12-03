/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Exponent implements StrictlyParsedOutputAggregator, LenientlyParsedOutputAggregator {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Exponent.class);
    public static final ParseField NAME = new ParseField("exponent");
    public static final ParseField WEIGHTS = new ParseField("weights");

    private static final ConstructingObjectParser<Exponent, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<Exponent, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<Exponent, Void> createParser(boolean lenient) {
        ConstructingObjectParser<Exponent, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new Exponent((List<Double>) a[0])
        );
        parser.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), WEIGHTS);
        return parser;
    }

    public static Exponent fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static Exponent fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private final double[] weights;

    Exponent() {
        this((List<Double>) null);
    }

    private Exponent(List<Double> weights) {
        this(weights == null ? null : weights.stream().mapToDouble(Double::valueOf).toArray());
    }

    public Exponent(double[] weights) {
        this.weights = weights;
    }

    public Exponent(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            this.weights = in.readDoubleArray();
        } else {
            this.weights = null;
        }
    }

    @Override
    public Integer expectedValueSize() {
        return this.weights == null ? null : this.weights.length;
    }

    @Override
    public double[] processValues(double[][] values) {
        Objects.requireNonNull(values, "values must not be null");
        if (weights != null && values.length != weights.length) {
            throw new IllegalArgumentException("values must be the same length as weights.");
        }
        assert values[0].length == 1;
        double[] processed = new double[values.length];
        for (int i = 0; i < values.length; ++i) {
            if (weights != null) {
                processed[i] = weights[i] * values[i][0];
            } else {
                processed[i] = values[i][0];
            }
        }
        return processed;
    }

    @Override
    public double aggregate(double[] values) {
        Objects.requireNonNull(values, "values must not be null");
        double sum = 0.0;
        for (double val : values) {
            if (Double.isFinite(val)) {
                sum += val;
            }
        }
        return Math.exp(sum);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public boolean compatibleWith(TargetType targetType) {
        return TargetType.REGRESSION.equals(targetType);
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
        Exponent that = (Exponent) o;
        return Arrays.equals(weights, that.weights);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(weights);
    }

    @Override
    public long ramBytesUsed() {
        long weightSize = weights == null ? 0L : RamUsageEstimator.sizeOf(weights);
        return SHALLOW_SIZE + weightSize;
    }
}
