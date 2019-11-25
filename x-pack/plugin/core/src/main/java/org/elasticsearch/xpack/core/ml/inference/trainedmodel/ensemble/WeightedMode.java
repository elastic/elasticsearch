/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;


import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.utils.Statistics.softMax;

public class WeightedMode implements StrictlyParsedOutputAggregator, LenientlyParsedOutputAggregator {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(WeightedMode.class);
    public static final ParseField NAME = new ParseField("weighted_mode");
    public static final ParseField WEIGHTS = new ParseField("weights");

    private static final ConstructingObjectParser<WeightedMode, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<WeightedMode, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<WeightedMode, Void> createParser(boolean lenient) {
        ConstructingObjectParser<WeightedMode, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new WeightedMode((List<Double>)a[0]));
        parser.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), WEIGHTS);
        return parser;
    }

    public static WeightedMode fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static WeightedMode fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private final double[] weights;

    WeightedMode() {
        this((List<Double>) null);
    }

    private WeightedMode(List<Double> weights) {
        this(weights == null ? null : weights.stream().mapToDouble(Double::valueOf).toArray());
    }

    public WeightedMode(double[] weights) {
        this.weights = weights;
    }

    public WeightedMode(StreamInput in) throws IOException {
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
    public List<Double> processValues(List<Double> values) {
        Objects.requireNonNull(values, "values must not be null");
        if (weights != null && values.size() != weights.length) {
            throw new IllegalArgumentException("values must be the same length as weights.");
        }
        List<Integer> freqArray = new ArrayList<>();
        Integer maxVal = 0;
        for (Double value : values) {
            if (value == null) {
                throw new IllegalArgumentException("values must not contain null values");
            }
            if (Double.isNaN(value) || Double.isInfinite(value) || value < 0.0 || value != Math.rint(value)) {
                throw new IllegalArgumentException("values must be whole, non-infinite, and positive");
            }
            Integer integerValue = value.intValue();
            freqArray.add(integerValue);
            if (integerValue > maxVal) {
                maxVal = integerValue;
            }
        }
        List<Double> frequencies = new ArrayList<>(Collections.nCopies(maxVal + 1, Double.NEGATIVE_INFINITY));
        for (int i = 0; i < freqArray.size(); i++) {
            Double weight = weights == null ? 1.0 : weights[i];
            Integer value = freqArray.get(i);
            Double frequency = frequencies.get(value) == Double.NEGATIVE_INFINITY ? weight : frequencies.get(value) + weight;
            frequencies.set(value, frequency);
        }
        return softMax(frequencies);
    }

    @Override
    public double aggregate(List<Double> values) {
        Objects.requireNonNull(values, "values must not be null");
        int bestValue = 0;
        double bestFreq = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) == null) {
                throw new IllegalArgumentException("values must not contain null values");
            }
            if (values.get(i) > bestFreq) {
                bestFreq = values.get(i);
                bestValue = i;
            }
        }
        return bestValue;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public boolean compatibleWith(TargetType targetType) {
        return true;
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
        WeightedMode that = (WeightedMode) o;
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
