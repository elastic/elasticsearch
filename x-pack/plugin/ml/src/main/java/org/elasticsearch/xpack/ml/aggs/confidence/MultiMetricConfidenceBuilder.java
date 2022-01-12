/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

abstract class MultiMetricConfidenceBuilder extends ConfidenceBuilder {

    @FunctionalInterface
    interface ValueExtractor {
        double[] extract(InternalAggregation internalAggregation);
    }

    @FunctionalInterface
    interface KeyExtractor {
        String[] extract(InternalAggregation internalAggregation);
    }

    @FunctionalInterface
    interface CountExtractor {
        long[] extract(InternalAggregation internalAggregation);
    }

    static double percentile(double p, double value, double[][] values, int index) {
        final double r = Math.max(p, 1 - p);
        final int n = (int) Math.floor(r * values.length);
        double alpha = Math.abs(r - (n + 1.0) / values.length);
        double beta = Math.abs(r - n / (double) values.length);
        final double z = alpha + beta;
        alpha /= z;
        beta /= z;
        final double shift;
        if (p < 0.5) {
            shift = n < values.length
                ? alpha * values[values.length - n][index] + beta * values[values.length - n - 1][index]
                : values[0][index];
        } else {
            shift = n + 1 < values.length ? alpha * values[n - 1][index] + beta * values[n][index] : values[values.length - 1][index];
        }
        return value * 2.0 - shift;
    }

    protected final ValueExtractor valueExtractor;
    protected final KeyExtractor keyExtractor;

    MultiMetricConfidenceBuilder(
        String name,
        InternalAggregation calculatedValue,
        ValueExtractor valueExtractor,
        KeyExtractor keyExtractor
    ) {
        super(name, calculatedValue);
        this.valueExtractor = valueExtractor;
        this.keyExtractor = keyExtractor;
    }

    static class MeanMultiMetricConfidenceBuilder extends MultiMetricConfidenceBuilder {

        MeanMultiMetricConfidenceBuilder(
            String name,
            InternalAggregation calculatedValue,
            ValueExtractor valueExtractor,
            KeyExtractor keyExtractor
        ) {
            super(name, calculatedValue, valueExtractor, keyExtractor);
        }

        @Override
        InternalConfidenceAggregation.ConfidenceBucket build(double probability, double pUpper, double pLower, boolean keyed) {
            double[] calculated = valueExtractor.extract(calculatedValue);
            double[][] extractedValues = new double[this.values.size()][];
            int i = 0;
            for (InternalAggregation agg : this.values) {
                extractedValues[i++] = valueExtractor.extract(agg);
            }
            Arrays.sort(extractedValues, Comparator.comparingDouble((double[] o) -> o[0]).reversed());

            double[] upper = new double[calculated.length];
            double[] lower = new double[calculated.length];
            double[] center = new double[calculated.length];
            for (int j = 0; j < calculated.length; j++) {
                upper[j] = probability * calculated[j] + (1 - probability) * percentile(pUpper, calculated[j], extractedValues, j);
                lower[j] = probability * calculated[j] + (1 - probability) * percentile(pLower, calculated[j], extractedValues, j);
                center[j] = probability * calculated[j] + (1 - probability) * percentile(0.5, calculated[j], extractedValues, j);
            }
            String[] keys = keyExtractor.extract(calculatedValue);
            return new InternalConfidenceAggregation.ConfidenceBucket(
                name,
                keyed,
                new MultiMetricConfidenceValue(upper, keys),
                new MultiMetricConfidenceValue(lower, keys),
                new MultiMetricConfidenceValue(center, keys)
            );
        }
    }

    static class MultiMetricConfidenceValue implements ConfidenceValue {
        static final String NAME = "multi_metric_confidence";

        private final double[] value;
        private final String[] keys;

        MultiMetricConfidenceValue(double[] value, String[] keys) {
            this.value = value;
            this.keys = keys;
        }

        MultiMetricConfidenceValue(StreamInput in) throws IOException {
            this.value = in.readDoubleArray();
            this.keys = in.readStringArray();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (int i = 0; i < keys.length; i++) {
                builder.field(keys[i], value[i]);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDoubleArray(value);
            out.writeStringArray(keys);
        }

    }
}
