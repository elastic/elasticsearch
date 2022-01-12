/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.math.LongBinomialDistribution;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

abstract class SingleMetricConfidenceBuilder extends ConfidenceBuilder {

    SingleMetricConfidenceBuilder(String name, InternalAggregation calculatedValue) {
        super(name, calculatedValue);
    }

    double calculatedValue() {
        return extractValue(calculatedValue);
    }

    double extractValue(InternalAggregation aggregation) {
        return ((InternalNumericMetricsAggregation.SingleValue) aggregation).value();
    }

    static SingleMetricConfidenceValue constructConfidenceValue(double value) {
        return new SingleMetricConfidenceValue(value);
    }

    protected Double[] extractValues() {
        Double[] extractedValues = new Double[this.values.size()];
        int i = 0;
        for (InternalAggregation agg : this.values) {
            extractedValues[i++] = extractValue(agg);
        }
        Arrays.sort(extractedValues, Collections.reverseOrder());
        return extractedValues;
    }

    static ConfidenceBuilder factory(InternalNumericMetricsAggregation.SingleValue internalAggregation, String name, long docCount) {
        if (internalAggregation instanceof Sum) {
            return new SumSingleMetricConfidenceBuilder(name, internalAggregation, docCount);
        } else if (internalAggregation instanceof ValueCount) {
            return new CountSingleMetricConfidenceBuilder(name, internalAggregation);
        }
        return new MeanSingleMetricConfidenceBuilder(name, internalAggregation);
    }

    static double percentile(double p, double value, Double[] values) {
        final double r = Math.max(p, 1 - p);
        final int n = (int) Math.floor(r * values.length);
        double alpha = Math.abs(r - (n + 1.0) / values.length);
        double beta = Math.abs(r - n / (double) values.length);
        final double z = alpha + beta;
        alpha /= z;
        beta /= z;
        final double shift;
        if (p < 0.5) {
            shift = n < values.length ? alpha * values[values.length - n] + beta * values[values.length - n - 1] : values[0];
        } else {
            shift = n + 1 < values.length ? alpha * values[n - 1] + beta * values[n] : values[values.length - 1];
        }
        return value * 2.0 - shift;
    }

    static InternalConfidenceAggregation.ConfidenceBucket fromCount(
        String name,
        double calculated,
        double probability,
        double pUpper,
        double pLower,
        boolean keyed
    ) {
        double upper = new LongBinomialDistribution((long) (calculated / probability), probability).inverseCumulativeProbability(pUpper);
        double lower = new LongBinomialDistribution((long) (calculated / probability), probability).inverseCumulativeProbability(pLower);
        double scale = (1 - probability) / probability;
        SingleMetricConfidenceValue upperValue = constructConfidenceValue(calculated + scale * upper);
        SingleMetricConfidenceValue lowerValue = constructConfidenceValue(calculated + scale * lower);
        SingleMetricConfidenceValue centerValue = constructConfidenceValue((scale + 1) * calculated);
        return new InternalConfidenceAggregation.ConfidenceBucket(name, keyed, upperValue, lowerValue, centerValue);
    }

    static class MeanSingleMetricConfidenceBuilder extends SingleMetricConfidenceBuilder {

        MeanSingleMetricConfidenceBuilder(String name, InternalAggregation calculatedValue) {
            super(name, calculatedValue);
        }

        @Override
        InternalConfidenceAggregation.ConfidenceBucket build(double probability, double pUpper, double pLower, boolean keyed) {
            double calculated = calculatedValue();
            Double[] extractedValues = extractValues();
            double upper = percentile(pUpper, calculated, extractedValues);
            double center = percentile(0.5, calculated, extractedValues);
            double lower = percentile(pLower, calculated, extractedValues);
            return new InternalConfidenceAggregation.ConfidenceBucket(
                name,
                keyed,
                constructConfidenceValue(probability * calculated + (1 - probability) * upper),
                constructConfidenceValue(probability * calculated + (1 - probability) * lower),
                constructConfidenceValue(probability * calculated + (1 - probability) * center)
            );
        }

    }

    static class CountSingleMetricConfidenceBuilder extends SingleMetricConfidenceBuilder {

        CountSingleMetricConfidenceBuilder(String name, InternalAggregation calculatedValue) {
            super(name, calculatedValue);
        }

        @Override
        InternalConfidenceAggregation.ConfidenceBucket build(double probability, double pUpper, double pLower, boolean keyed) {
            return fromCount(name, calculatedValue(), probability, pUpper, pLower, keyed);
        }
    }

    static class SumSingleMetricConfidenceBuilder extends SingleMetricConfidenceBuilder {

        private final long docCount;

        SumSingleMetricConfidenceBuilder(String name, InternalAggregation calculatedValue, long docCount) {
            super(name, calculatedValue);
            this.docCount = docCount;
        }

        @Override
        InternalConfidenceAggregation.ConfidenceBucket build(double probability, double pUpper, double pLower, boolean keyed) {
            double calculatedValue = extractValue(this.calculatedValue);
            Double[] samples = extractValues();
            double scale = (1.0 - probability) / probability;
            double upperErrorCount = scale * (new LongBinomialDistribution((long) (docCount / probability), probability)
                .inverseCumulativeProbability(pUpper)) + calculatedValue / docCount - calculatedValue;
            double lowerErrorCount = scale * (new LongBinomialDistribution((long) (docCount / probability), probability)
                .inverseCumulativeProbability(pLower)) + calculatedValue / docCount - calculatedValue;
            double sampleMedian = percentile(0.5, calculatedValue, samples);
            double upperErrorMetric = scale * (percentile(pUpper, calculatedValue, samples) - sampleMedian);
            double lowerErrorMetric = scale * (percentile(pLower, calculatedValue, samples) - sampleMedian);
            return new InternalConfidenceAggregation.ConfidenceBucket(
                name,
                keyed,
                new SingleMetricConfidenceValue(
                    (1 + scale) * calculatedValue + Math.sqrt(Math.pow(upperErrorMetric, 2) + Math.pow(upperErrorCount, 2))
                ),
                new SingleMetricConfidenceValue(
                    (1 + scale) * calculatedValue - Math.sqrt(Math.pow(lowerErrorMetric, 2) + Math.pow(lowerErrorCount, 2))
                ),
                new SingleMetricConfidenceValue((1 + scale) * calculatedValue)
            );
        }
    }

    static class SingleMetricConfidenceValue implements ConfidenceValue {
        static final String NAME = "single_metric_confidence";

        private final double value;

        protected SingleMetricConfidenceValue(double value) {
            this.value = value;
        }

        SingleMetricConfidenceValue(StreamInput in) throws IOException {
            this.value = in.readDouble();
        }

        double value() {
            return value;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Aggregation.CommonFields.VALUE.getPreferredName(), value);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(value);
        }
    }
}
