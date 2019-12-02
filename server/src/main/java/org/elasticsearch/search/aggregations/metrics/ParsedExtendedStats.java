/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStats.Fields;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

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

    private void setStdDeviationBounds(Tuple<Double, Double> bounds) {
        this.stdDeviationBoundLower = bounds.v1();
        this.stdDeviationBoundUpper = bounds.v2();
    }

    @Override
    public double getStdDeviationBound(Bounds bound) {
        return (bound.equals(Bounds.LOWER)) ? stdDeviationBoundLower : stdDeviationBoundUpper;
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

    private void setStdDeviationBoundsAsString(Tuple<String, String> boundsAsString) {
        this.valueAsString.put(Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_lower", boundsAsString.v1());
        this.valueAsString.put(Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_upper", boundsAsString.v2());
    }

    @Override
    public String getStdDeviationBoundAsString(Bounds bound) {
        if (bound.equals(Bounds.LOWER)) {
            return valueAsString.getOrDefault(Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_lower", Double.toString(stdDeviationBoundLower));
        } else {
            return valueAsString.getOrDefault(Fields.STD_DEVIATION_BOUNDS_AS_STRING + "_upper", Double.toString(stdDeviationBoundUpper));
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
            }
            builder.endObject();
        }
        return builder;
    }

    private static final ObjectParser<ParsedExtendedStats, Void> PARSER = new ObjectParser<>(ParsedExtendedStats.class.getSimpleName(),
            true, ParsedExtendedStats::new);

    private static final ConstructingObjectParser<Tuple<Double, Double>, Void> STD_BOUNDS_PARSER = new ConstructingObjectParser<>(
            ParsedExtendedStats.class.getSimpleName() + "_STD_BOUNDS", true, args -> new Tuple<>((Double) args[0], (Double) args[1]));

    private static final ConstructingObjectParser<Tuple<String, String>, Void> STD_BOUNDS_AS_STRING_PARSER = new ConstructingObjectParser<>(
            ParsedExtendedStats.class.getSimpleName() + "_STD_BOUNDS_AS_STRING", true,
            args -> new Tuple<>((String) args[0], (String) args[1]));

    static {
        STD_BOUNDS_PARSER.declareField(constructorArg(), (parser, context) -> parseDouble(parser, 0),
                new ParseField(Fields.LOWER), ValueType.DOUBLE_OR_NULL);
        STD_BOUNDS_PARSER.declareField(constructorArg(), (parser, context) -> parseDouble(parser, 0),
                new ParseField(Fields.UPPER), ValueType.DOUBLE_OR_NULL);
        STD_BOUNDS_AS_STRING_PARSER.declareString(constructorArg(), new ParseField(Fields.LOWER));
        STD_BOUNDS_AS_STRING_PARSER.declareString(constructorArg(), new ParseField(Fields.UPPER));
        declareExtendedStatsFields(PARSER);
    }

    protected static void declareExtendedStatsFields(ObjectParser<? extends ParsedExtendedStats, Void> objectParser) {
        declareAggregationFields(objectParser);
        declareStatsFields(objectParser);
        objectParser.declareField((agg, value) -> agg.sumOfSquares = value, (parser, context) -> parseDouble(parser, 0),
                new ParseField(Fields.SUM_OF_SQRS), ValueType.DOUBLE_OR_NULL);
        objectParser.declareField((agg, value) -> agg.variance = value, (parser, context) -> parseDouble(parser, 0),
                new ParseField(Fields.VARIANCE), ValueType.DOUBLE_OR_NULL);
        objectParser.declareField((agg, value) -> agg.variancePopulation = value, (parser, context) -> parseDouble(parser, 0),
                new ParseField(Fields.VARIANCE_POPULATION), ValueType.DOUBLE_OR_NULL);
        objectParser.declareField((agg, value) -> agg.varianceSampling = value, (parser, context) -> parseDouble(parser, 0),
                new ParseField(Fields.VARIANCE_SAMPLING), ValueType.DOUBLE_OR_NULL);
        objectParser.declareField((agg, value) -> agg.stdDeviation = value, (parser, context) -> parseDouble(parser, 0),
                new ParseField(Fields.STD_DEVIATION), ValueType.DOUBLE_OR_NULL);
        objectParser.declareField((agg, value) -> agg.stdDeviationPopulation = value, (parser, context) -> parseDouble(parser, 0),
                new ParseField(Fields.STD_DEVIATION_POPULATION), ValueType.DOUBLE_OR_NULL);
        objectParser.declareField((agg, value) -> agg.stdDeviationSampling = value, (parser, context) -> parseDouble(parser, 0),
                new ParseField(Fields.STD_DEVIATION_SAMPLING), ValueType.DOUBLE_OR_NULL);
        objectParser.declareObject(ParsedExtendedStats::setStdDeviationBounds, STD_BOUNDS_PARSER,
                new ParseField(Fields.STD_DEVIATION_BOUNDS));
        objectParser.declareString((agg, value) -> agg.valueAsString.put(Fields.SUM_OF_SQRS_AS_STRING, value),
                new ParseField(Fields.SUM_OF_SQRS_AS_STRING));
        objectParser.declareString((agg, value) -> agg.valueAsString.put(Fields.VARIANCE_AS_STRING, value),
                new ParseField(Fields.VARIANCE_AS_STRING));
        objectParser.declareString((agg, value) -> agg.valueAsString.put(Fields.VARIANCE_POPULATION_AS_STRING, value),
                new ParseField(Fields.VARIANCE_POPULATION_AS_STRING));
        objectParser.declareString((agg, value) -> agg.valueAsString.put(Fields.VARIANCE_SAMPLING_AS_STRING, value),
                new ParseField(Fields.VARIANCE_SAMPLING_AS_STRING));
        objectParser.declareString((agg, value) -> agg.valueAsString.put(Fields.STD_DEVIATION_AS_STRING, value),
                new ParseField(Fields.STD_DEVIATION_AS_STRING));
        objectParser.declareString((agg, value) -> agg.valueAsString.put(Fields.STD_DEVIATION_POPULATION_AS_STRING, value),
                new ParseField(Fields.STD_DEVIATION_POPULATION_AS_STRING));
        objectParser.declareString((agg, value) -> agg.valueAsString.put(Fields.STD_DEVIATION_SAMPLING_AS_STRING, value),
                new ParseField(Fields.STD_DEVIATION_SAMPLING_AS_STRING));
        objectParser.declareObject(ParsedExtendedStats::setStdDeviationBoundsAsString, STD_BOUNDS_AS_STRING_PARSER,
                new ParseField(Fields.STD_DEVIATION_BOUNDS_AS_STRING));
    }

    public static ParsedExtendedStats fromXContent(XContentParser parser, final String name) {
        ParsedExtendedStats parsedStats = PARSER.apply(parser, null);
        parsedStats.setName(name);
        return parsedStats;
    }
}
