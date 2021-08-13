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

import static org.elasticsearch.xpack.core.ml.inference.utils.Statistics.sigmoid;
import static org.elasticsearch.xpack.core.ml.inference.utils.Statistics.softMax;

public class LogisticRegression implements StrictlyParsedOutputAggregator, LenientlyParsedOutputAggregator {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(LogisticRegression.class);
    public static final ParseField NAME = new ParseField("logistic_regression");
    public static final ParseField WEIGHTS = new ParseField("weights");

    private static final ConstructingObjectParser<LogisticRegression, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<LogisticRegression, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<LogisticRegression, Void> createParser(boolean lenient) {
        ConstructingObjectParser<LogisticRegression, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new LogisticRegression((List<Double>)a[0]));
        parser.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), WEIGHTS);
        return parser;
    }

    public static LogisticRegression fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static LogisticRegression fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private final double[] weights;

    LogisticRegression() {
        this((List<Double>) null);
    }

    private LogisticRegression(List<Double> weights) {
        this(weights == null ? null : weights.stream().mapToDouble(Double::valueOf).toArray());
    }

    public LogisticRegression(double[] weights) {
        this.weights = weights;
    }

    public LogisticRegression(StreamInput in) throws IOException {
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
        double[] sumOnAxis1 = new double[values[0].length];
        for (int j = 0; j < values.length; j++) {
            double[] value = values[j];
            double weight = weights == null ? 1.0 : weights[j];
            for(int i = 0; i < value.length; i++) {
                if (i >= sumOnAxis1.length) {
                    throw new IllegalArgumentException("value entries must have the same dimensions");
                }
                sumOnAxis1[i] += (value[i] * weight);
            }
        }
        if (sumOnAxis1.length > 1) {
            return softMax(sumOnAxis1);
        }

        double probOfClassOne = sigmoid(sumOnAxis1[0]);
        assert 0.0 <= probOfClassOne && probOfClassOne <= 1.0;
        return new double[] {1.0 - probOfClassOne, probOfClassOne};
    }

    @Override
    public double aggregate(double[] values) {
        Objects.requireNonNull(values, "values must not be null");
        int bestValue = 0;
        double bestProb = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < values.length; i++) {
            if (values[i] > bestProb) {
                bestProb = values[i];
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
        LogisticRegression that = (LogisticRegression) o;
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
