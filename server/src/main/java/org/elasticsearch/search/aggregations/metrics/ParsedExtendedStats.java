/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStats.Fields;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ParsedExtendedStats extends ParsedStats implements ExtendedStats {

    protected double sumOfSquares;
    protected double variance;
    protected double variancePopulation;
    protected double varianceSampling;
    protected double stdDeviation;
    protected double stdDeviationPopulation;
    protected double stdDeviationSampling;
    protected double stdDeviationBoundUpper;
    protected double stdDeviationBoundLower;
    protected double stdDeviationBoundUpperPopulation;
    protected double stdDeviationBoundLowerPopulation;
    protected double stdDeviationBoundUpperSampling;
    protected double stdDeviationBoundLowerSampling;

    protected double sum;
    protected double avg;

    @Override
    public String getType() {
        return ExtendedStatsAggregationBuilder.NAME;
    }

    @Override
    public double getSumOfSquares() {
        return sumOfSquares;
    }

    @Override
    public double getVariance() {
        return variance;
    }

    @Override
    public double getVariancePopulation() {
        return variancePopulation;
    }

    @Override
    public double getVarianceSampling() {
        return varianceSampling;
    }

    @Override
    public double getStdDeviation() {
        return stdDeviation;
    }

    @Override
    public double getStdDeviationPopulation() {
        return stdDeviationPopulation;
    }

    @Override
    public double getStdDeviationSampling() {
        return stdDeviationSampling;
    }

    private void setStdDeviationBounds(List<Double> bounds) {
        this.stdDeviationBoundUpper = bounds.get(0);
        this.stdDeviationBoundLower = bounds.get(1);
        this.stdDeviationBoundUpperPopulation = bounds.get(2) == null ? 0 : bounds.get(2);
        this.stdDeviationBoundLowerPopulation = bounds.get(3) == null ? 0 : bounds.get(3);
        this.stdDeviationBoundUpperSampling = bounds.get(4) == null ? 0 : bounds.get(4);
        this.stdDeviationBoundLowerSampling = bounds.get(5) == null ? 0 : bounds.get(5);
    }

    @Override
    public double getStdDeviationBound(Bounds bound) {
        switch (bound) {
            case UPPER:
                return stdDeviationBoundUpper;
            case UPPER_POPULATION:
                return stdDeviationBoundUpperPopulation;
            case UPPER_SAMPLING:
                return stdDeviationBoundUpperSampling;
            case LOWER:
                return stdDeviationBoundLower;
            case LOWER_POPULATION:
                return stdDeviationBoundLowerPopulation;
            case LOWER_SAMPLING:
                return stdDeviationBoundLowerSampling;
            default:
                throw new IllegalArgumentException("Unknown bounds type " + bound);
        }
    }

    private void setStdDeviationBoundsAsString(List<String> boundsAsString) {
        this.valueAsString.put(Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_upper", boundsAsString.get(0));
        this.valueAsString.put(Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_lower", boundsAsString.get(1));
        if (boundsAsString.get(2) != null) {
            this.valueAsString.put(Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_upper_population", boundsAsString.get(2));
        }
        if (boundsAsString.get(3) != null) {
            this.valueAsString.put(Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_lower_population", boundsAsString.get(3));
        }
        if (boundsAsString.get(4) != null) {
            this.valueAsString.put(Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_upper_sampling", boundsAsString.get(4));
        }
        if (boundsAsString.get(5) != null) {
            this.valueAsString.put(Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_lower_sampling", boundsAsString.get(5));
        }
    }

    @Override
    public String getStdDeviationAsString() {
        return valueAsString.getOrDefault(Fields.STD_DEVIATION_AS_STRING, Double.toString(stdDeviation));
    }

    @Override
    public String getStdDeviationPopulationAsString() {
        return valueAsString.getOrDefault(Fields.STD_DEVIATION_POPULATION_AS_STRING, Double.toString(stdDeviationPopulation));
    }

    @Override
    public String getStdDeviationSamplingAsString() {
        return valueAsString.getOrDefault(Fields.STD_DEVIATION_SAMPLING_AS_STRING, Double.toString(stdDeviationSampling));
    }

    @Override
    public String getStdDeviationBoundAsString(Bounds bound) {
        switch (bound) {
            case UPPER:
                return valueAsString.getOrDefault(
                    Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_upper",
                    Double.toString(stdDeviationBoundUpper)
                );
            case UPPER_POPULATION:
                return valueAsString.getOrDefault(
                    Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_upper_population",
                    Double.toString(stdDeviationBoundUpperPopulation)
                );
            case UPPER_SAMPLING:
                return valueAsString.getOrDefault(
                    Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_upper_sampling",
                    Double.toString(stdDeviationBoundUpperSampling)
                );
            case LOWER:
                return valueAsString.getOrDefault(
                    Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_lower",
                    Double.toString(stdDeviationBoundLower)
                );
            case LOWER_POPULATION:
                return valueAsString.getOrDefault(
                    Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_lower_population",
                    Double.toString(stdDeviationBoundLowerPopulation)
                );
            case LOWER_SAMPLING:
                return valueAsString.getOrDefault(
                    Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_lower_sampling",
                    Double.toString(stdDeviationBoundLowerSampling)
                );
            default:
                throw new IllegalArgumentException("Unknown bounds type " + bound);
        }
    }

    @Override
    public String getSumOfSquaresAsString() {
        return valueAsString.getOrDefault(Fields.SUM_OF_SQRS_AS_STRING, Double.toString(sumOfSquares));
    }

    @Override
    public String getVarianceAsString() {
        return valueAsString.getOrDefault(Fields.VARIANCE_AS_STRING, Double.toString(variance));
    }

    @Override
    public String getVariancePopulationAsString() {
        return valueAsString.getOrDefault(Fields.VARIANCE_POPULATION_AS_STRING, Double.toString(variancePopulation));
    }

    @Override
    public String getVarianceSamplingAsString() {
        return valueAsString.getOrDefault(Fields.VARIANCE_SAMPLING_AS_STRING, Double.toString(varianceSampling));
    }

    @Override
    protected XContentBuilder otherStatsToXContent(XContentBuilder builder, Params params) throws IOException {
        if (count != 0) {
            builder.field(Fields.SUM_OF_SQRS, sumOfSquares);
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
            if (valueAsString.containsKey(Fields.SUM_OF_SQRS_AS_STRING)) {
                builder.field(Fields.SUM_OF_SQRS_AS_STRING, getSumOfSquaresAsString());
                builder.field(Fields.VARIANCE_AS_STRING, getVarianceAsString());
                builder.field(Fields.VARIANCE_POPULATION_AS_STRING, getVariancePopulationAsString());
                builder.field(Fields.VARIANCE_SAMPLING_AS_STRING, getVarianceSamplingAsString());
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

    private static final ObjectParser<ParsedExtendedStats, Void> PARSER = new ObjectParser<>(
        ParsedExtendedStats.class.getSimpleName(),
        true,
        ParsedExtendedStats::new
    );

    private static final ConstructingObjectParser<List<Double>, Void> STD_BOUNDS_PARSER = new ConstructingObjectParser<>(
        ParsedExtendedStats.class.getSimpleName() + "_STD_BOUNDS",
        true,
        args -> Arrays.stream(args).map(d -> (Double) d).collect(Collectors.toList())
    );

    private static final ConstructingObjectParser<List<String>, Void> STD_BOUNDS_AS_STRING_PARSER = new ConstructingObjectParser<>(
        ParsedExtendedStats.class.getSimpleName() + "_STD_BOUNDS_AS_STRING",
        true,
        args -> Arrays.stream(args).map(d -> (String) d).collect(Collectors.toList())
    );

    static {
        STD_BOUNDS_PARSER.declareField(
            constructorArg(),
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.UPPER),
            ValueType.DOUBLE_OR_NULL
        );
        STD_BOUNDS_PARSER.declareField(
            constructorArg(),
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.LOWER),
            ValueType.DOUBLE_OR_NULL
        );
        STD_BOUNDS_PARSER.declareField(
            optionalConstructorArg(),
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.UPPER_POPULATION),
            ValueType.DOUBLE_OR_NULL
        );
        STD_BOUNDS_PARSER.declareField(
            optionalConstructorArg(),
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.LOWER_POPULATION),
            ValueType.DOUBLE_OR_NULL
        );
        STD_BOUNDS_PARSER.declareField(
            optionalConstructorArg(),
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.UPPER_SAMPLING),
            ValueType.DOUBLE_OR_NULL
        );
        STD_BOUNDS_PARSER.declareField(
            optionalConstructorArg(),
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.LOWER_SAMPLING),
            ValueType.DOUBLE_OR_NULL
        );
        STD_BOUNDS_AS_STRING_PARSER.declareString(constructorArg(), new ParseField(Fields.UPPER));
        STD_BOUNDS_AS_STRING_PARSER.declareString(constructorArg(), new ParseField(Fields.LOWER));
        STD_BOUNDS_AS_STRING_PARSER.declareString(optionalConstructorArg(), new ParseField(Fields.UPPER_POPULATION));
        STD_BOUNDS_AS_STRING_PARSER.declareString(optionalConstructorArg(), new ParseField(Fields.LOWER_POPULATION));
        STD_BOUNDS_AS_STRING_PARSER.declareString(optionalConstructorArg(), new ParseField(Fields.UPPER_SAMPLING));
        STD_BOUNDS_AS_STRING_PARSER.declareString(optionalConstructorArg(), new ParseField(Fields.LOWER_SAMPLING));
        declareExtendedStatsFields(PARSER);
    }

    protected static void declareExtendedStatsFields(ObjectParser<? extends ParsedExtendedStats, Void> objectParser) {
        declareStatsFields(objectParser);
        objectParser.declareField(
            (agg, value) -> agg.sumOfSquares = value,
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.SUM_OF_SQRS),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareField(
            (agg, value) -> agg.variance = value,
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.VARIANCE),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareField(
            (agg, value) -> agg.variancePopulation = value,
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.VARIANCE_POPULATION),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareField(
            (agg, value) -> agg.varianceSampling = value,
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.VARIANCE_SAMPLING),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareField(
            (agg, value) -> agg.stdDeviation = value,
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.STD_DEVIATION),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareField(
            (agg, value) -> agg.stdDeviationPopulation = value,
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.STD_DEVIATION_POPULATION),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareField(
            (agg, value) -> agg.stdDeviationSampling = value,
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.STD_DEVIATION_SAMPLING),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareObject(
            ParsedExtendedStats::setStdDeviationBounds,
            STD_BOUNDS_PARSER,
            new ParseField(Fields.STD_DEVIATION_BOUNDS)
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.SUM_OF_SQRS_AS_STRING, value),
            new ParseField(Fields.SUM_OF_SQRS_AS_STRING)
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.VARIANCE_AS_STRING, value),
            new ParseField(Fields.VARIANCE_AS_STRING)
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.VARIANCE_POPULATION_AS_STRING, value),
            new ParseField(Fields.VARIANCE_POPULATION_AS_STRING)
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.VARIANCE_SAMPLING_AS_STRING, value),
            new ParseField(Fields.VARIANCE_SAMPLING_AS_STRING)
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.STD_DEVIATION_AS_STRING, value),
            new ParseField(Fields.STD_DEVIATION_AS_STRING)
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.STD_DEVIATION_POPULATION_AS_STRING, value),
            new ParseField(Fields.STD_DEVIATION_POPULATION_AS_STRING)
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.STD_DEVIATION_SAMPLING_AS_STRING, value),
            new ParseField(Fields.STD_DEVIATION_SAMPLING_AS_STRING)
        );
        objectParser.declareObject(
            ParsedExtendedStats::setStdDeviationBoundsAsString,
            STD_BOUNDS_AS_STRING_PARSER,
            new ParseField(Fields.STD_DEVIATION_BOUNDS_AS_STRING)
        );
    }

    public static ParsedExtendedStats fromXContent(XContentParser parser, final String name) {
        ParsedExtendedStats parsedStats = PARSER.apply(parser, null);
        parsedStats.setName(name);
        return parsedStats;
    }
}
