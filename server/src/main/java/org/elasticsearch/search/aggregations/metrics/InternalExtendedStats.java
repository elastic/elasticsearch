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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalExtendedStats extends InternalStats implements ExtendedStats {
    enum Metrics {

        count, sum, min, max, avg, sum_of_squares, variance, variance_population, variance_sampling,
        std_deviation, std_deviation_population, std_deviation_sampling, std_upper, std_lower, std_upper_population, std_lower_population,
        std_upper_sampling, std_lower_sampling;

        public static Metrics resolve(String name) {
            return Metrics.valueOf(name);
        }
    }

    private static final Set<String> METRIC_NAMES = Collections.unmodifiableSet(
        Stream.of(Metrics.values()).map(Metrics::name).collect(Collectors.toSet())
    );

    private final double sumOfSqrs;
    private final double sigma;

    public InternalExtendedStats(String name, long count, double sum, double min, double max, double sumOfSqrs, double sigma,
                                 DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, count, sum, min, max, formatter, metadata);
        this.sumOfSqrs = sumOfSqrs;
        this.sigma = sigma;
    }

    /**
     * Read from a stream.
     */
    public InternalExtendedStats(StreamInput in) throws IOException {
        super(in);
        sumOfSqrs = in.readDouble();
        sigma = in.readDouble();
    }

    @Override
    protected void writeOtherStatsTo(StreamOutput out) throws IOException {
        out.writeDouble(sumOfSqrs);
        out.writeDouble(sigma);
    }

    @Override
    public String getWriteableName() {
        return ExtendedStatsAggregationBuilder.NAME;
    }

    @Override
    public double value(String name) {
        if ("sum_of_squares".equals(name)) {
            return sumOfSqrs;
        }
        if ("variance".equals(name)) {
            return getVariance();
        }
        if ("variance_population".equals(name)) {
            return getVariancePopulation();
        }
        if ("variance_sampling".equals(name)) {
            return getVarianceSampling();
        }
        if ("std_deviation".equals(name)) {
            return getStdDeviation();
        }
        if ("std_deviation_population".equals(name)) {
            return getStdDeviationPopulation();
        }
        if ("std_deviation_sampling".equals(name)) {
            return getStdDeviationSampling();
        }
        if ("std_upper".equals(name)) {
            return getStdDeviationBound(Bounds.UPPER);
        }
        if ("std_lower".equals(name)) {
            return getStdDeviationBound(Bounds.LOWER);
        }
        if ("std_upper_population".equals(name)) {
            return getStdDeviationBound(Bounds.UPPER_POPULATION);
        }
        if ("std_lower_population".equals(name)) {
            return getStdDeviationBound(Bounds.LOWER_POPULATION);
        }
        if ("std_upper_sampling".equals(name)) {
            return getStdDeviationBound(Bounds.UPPER_SAMPLING);
        }
        if ("std_lower_sampling".equals(name)) {
            return getStdDeviationBound(Bounds.LOWER_SAMPLING);
        }
        return super.value(name);
    }

    @Override
    public Iterable<String> valueNames() {
        return METRIC_NAMES;
    }

    public double getSigma() {
        return this.sigma;
    }

    @Override
    public double getSumOfSquares() {
        return sumOfSqrs;
    }

    @Override
    public double getVariance() {
        return getVariancePopulation();
    }

    @Override
    public double getVariancePopulation() {
        double variance =  (sumOfSqrs - ((sum * sum) / count)) / count;
        return variance < 0  ? 0 : variance;
    }

    @Override
    public double getVarianceSampling() {
        double variance =  (sumOfSqrs - ((sum * sum) / count)) / (count - 1);
        return variance < 0  ? 0 : variance;
    }

    @Override
    public double getStdDeviation() {
        return getStdDeviationPopulation();
    }

    @Override
    public double getStdDeviationPopulation() {
        return Math.sqrt(getVariancePopulation());
    }

    @Override
    public double getStdDeviationSampling() {
        return Math.sqrt(getVarianceSampling());
    }

    @Override
    public double getStdDeviationBound(Bounds bound) {
        switch (bound) {
            case UPPER:
            case UPPER_POPULATION:
                return getAvg() + (getStdDeviationPopulation() * sigma);
            case UPPER_SAMPLING:
                return getAvg() + (getStdDeviationSampling() * sigma);
            case LOWER:
            case LOWER_POPULATION:
                return getAvg() - (getStdDeviationPopulation() * sigma);
            case LOWER_SAMPLING:
                return getAvg() - (getStdDeviationSampling() * sigma);
            default:
                throw new IllegalArgumentException("Unknown bounds type " + bound);
        }
    }

    @Override
    public String getSumOfSquaresAsString() {
        return valueAsString(Metrics.sum_of_squares.name());
    }

    @Override
    public String getVarianceAsString() {
        return valueAsString(Metrics.variance.name());
    }

    @Override
    public String getVariancePopulationAsString() {
        return valueAsString(Metrics.variance_population.name());
    }

    @Override
    public String getVarianceSamplingAsString() {
        return valueAsString(Metrics.variance_sampling.name());
    }

    @Override
    public String getStdDeviationAsString() {
        return valueAsString(Metrics.std_deviation.name());
    }

    @Override
    public String getStdDeviationPopulationAsString() {
        return valueAsString(Metrics.std_deviation_population.name());
    }

    @Override
    public String getStdDeviationSamplingAsString() {
        return valueAsString(Metrics.std_deviation_sampling.name());
    }

    @Override
    public String getStdDeviationBoundAsString(Bounds bound) {
        switch (bound) {
            case UPPER:
                return valueAsString(Metrics.std_upper.name());
            case LOWER:
                return valueAsString(Metrics.std_lower.name());
            case UPPER_POPULATION:
                return valueAsString(Metrics.std_upper_population.name());
            case LOWER_POPULATION:
                return valueAsString(Metrics.std_lower_population.name());
            case UPPER_SAMPLING:
                return valueAsString(Metrics.std_upper_sampling.name());
            case LOWER_SAMPLING:
                return valueAsString(Metrics.std_lower_sampling.name());
            default:
                throw new IllegalArgumentException("Unknown bounds type " + bound);
        }
    }

    @Override
    public InternalExtendedStats reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double sumOfSqrs = 0;
        double compensationOfSqrs = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalExtendedStats stats = (InternalExtendedStats) aggregation;
            if (stats.sigma != sigma) {
                throw new IllegalStateException("Cannot reduce other stats aggregations that have a different sigma");
            }
            double value = stats.getSumOfSquares();
            if (Double.isFinite(value) == false) {
                sumOfSqrs += value;
            } else if (Double.isFinite(sumOfSqrs)) {
                double correctedOfSqrs = value - compensationOfSqrs;
                double newSumOfSqrs = sumOfSqrs + correctedOfSqrs;
                compensationOfSqrs = (newSumOfSqrs - sumOfSqrs) - correctedOfSqrs;
                sumOfSqrs = newSumOfSqrs;
            }
        }
        final InternalStats stats = super.reduce(aggregations, reduceContext);
        return new InternalExtendedStats(name, stats.getCount(), stats.getSum(), stats.getMin(), stats.getMax(), sumOfSqrs, sigma,
            format, getMetadata());
    }

    static class Fields {
        public static final String SUM_OF_SQRS = "sum_of_squares";
        public static final String SUM_OF_SQRS_AS_STRING = "sum_of_squares_as_string";
        public static final String VARIANCE = "variance";
        public static final String VARIANCE_AS_STRING = "variance_as_string";
        public static final String VARIANCE_POPULATION = "variance_population";
        public static final String VARIANCE_POPULATION_AS_STRING = "variance_population_as_string";
        public static final String VARIANCE_SAMPLING = "variance_sampling";
        public static final String VARIANCE_SAMPLING_AS_STRING = "variance_sampling_as_string";
        public static final String STD_DEVIATION = "std_deviation";
        public static final String STD_DEVIATION_AS_STRING = "std_deviation_as_string";
        public static final String STD_DEVIATION_POPULATION = "std_deviation_population";
        public static final String STD_DEVIATION_POPULATION_AS_STRING = "std_deviation_population_as_string";
        public static final String STD_DEVIATION_SAMPLING = "std_deviation_sampling";
        public static final String STD_DEVIATION_SAMPLING_AS_STRING = "std_deviation_sampling_as_string";
        public static final String STD_DEVIATION_BOUNDS = "std_deviation_bounds";
        public static final String STD_DEVIATION_BOUNDS_AS_STRING = "std_deviation_bounds_as_string";
        public static final String UPPER = "upper";
        public static final String LOWER = "lower";
        public static final String UPPER_POPULATION = "upper_population";
        public static final String LOWER_POPULATION = "lower_population";
        public static final String UPPER_SAMPLING = "upper_sampling";
        public static final String LOWER_SAMPLING = "lower_sampling";
    }

    @Override
    protected XContentBuilder otherStatsToXContent(XContentBuilder builder, Params params) throws IOException {
        if (count != 0) {
            builder.field(Fields.SUM_OF_SQRS, sumOfSqrs);
            builder.field(Fields.VARIANCE, getVariance());
            builder.field(Fields.VARIANCE_POPULATION, getVariancePopulation());
            builder.field(Fields.VARIANCE_SAMPLING, getVarianceSampling());
            builder.field(Fields.STD_DEVIATION, getStdDeviation());
            builder.field(Fields.STD_DEVIATION_POPULATION, getStdDeviationPopulation());
            builder.field(Fields.STD_DEVIATION_SAMPLING, getStdDeviationSampling());
            builder.startObject(Fields.STD_DEVIATION_BOUNDS);
            {
                builder.field(Fields.UPPER, getStdDeviationBound(Bounds.UPPER));
                builder.field(Fields.LOWER, getStdDeviationBound(Bounds.LOWER));
                builder.field(Fields.UPPER_POPULATION, getStdDeviationBound(Bounds.UPPER_POPULATION));
                builder.field(Fields.LOWER_POPULATION, getStdDeviationBound(Bounds.LOWER_POPULATION));
                builder.field(Fields.UPPER_SAMPLING, getStdDeviationBound(Bounds.UPPER_SAMPLING));
                builder.field(Fields.LOWER_SAMPLING, getStdDeviationBound(Bounds.LOWER_SAMPLING));
            }
            builder.endObject();
            if (format != DocValueFormat.RAW) {
                builder.field(Fields.SUM_OF_SQRS_AS_STRING, format.format(sumOfSqrs));
                builder.field(Fields.VARIANCE_AS_STRING, format.format(getVariance()));
                builder.field(Fields.VARIANCE_POPULATION_AS_STRING, format.format(getVariancePopulation()));
                builder.field(Fields.VARIANCE_SAMPLING_AS_STRING, format.format(getVarianceSampling()));
                builder.field(Fields.STD_DEVIATION_AS_STRING, getStdDeviationAsString());
                builder.field(Fields.STD_DEVIATION_POPULATION_AS_STRING, getStdDeviationPopulationAsString());
                builder.field(Fields.STD_DEVIATION_SAMPLING_AS_STRING, getStdDeviationSamplingAsString());
                builder.startObject(Fields.STD_DEVIATION_BOUNDS_AS_STRING);
                {
                    builder.field(Fields.UPPER, getStdDeviationBoundAsString(Bounds.UPPER));
                    builder.field(Fields.LOWER, getStdDeviationBoundAsString(Bounds.LOWER));
                    builder.field(Fields.UPPER_POPULATION, getStdDeviationBoundAsString(Bounds.UPPER_POPULATION));
                    builder.field(Fields.LOWER_POPULATION, getStdDeviationBoundAsString(Bounds.LOWER_POPULATION));
                    builder.field(Fields.UPPER_SAMPLING, getStdDeviationBoundAsString(Bounds.UPPER_SAMPLING));
                    builder.field(Fields.LOWER_SAMPLING, getStdDeviationBoundAsString(Bounds.LOWER_SAMPLING));
                }
                builder.endObject();
            }
        } else {
            builder.nullField(Fields.SUM_OF_SQRS);
            builder.nullField(Fields.VARIANCE);
            builder.nullField(Fields.VARIANCE_POPULATION);
            builder.nullField(Fields.VARIANCE_SAMPLING);
            builder.nullField(Fields.STD_DEVIATION);
            builder.nullField(Fields.STD_DEVIATION_POPULATION);
            builder.nullField(Fields.STD_DEVIATION_SAMPLING);
            builder.startObject(Fields.STD_DEVIATION_BOUNDS);
            {
                builder.nullField(Fields.UPPER);
                builder.nullField(Fields.LOWER);
                builder.nullField(Fields.UPPER_POPULATION);
                builder.nullField(Fields.LOWER_POPULATION);
                builder.nullField(Fields.UPPER_SAMPLING);
                builder.nullField(Fields.LOWER_SAMPLING);
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sumOfSqrs, sigma);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalExtendedStats other = (InternalExtendedStats) obj;
        return Double.compare(sumOfSqrs, other.sumOfSqrs) == 0 &&
            Double.compare(sigma, other.sigma) == 0;
    }
}
