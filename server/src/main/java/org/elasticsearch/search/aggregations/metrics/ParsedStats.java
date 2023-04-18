/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalStats.Fields;
import org.elasticsearch.search.aggregations.metrics.InternalStats.Metrics;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.search.aggregations.metrics.InternalStats.METRIC_NAMES;

public class ParsedStats extends ParsedAggregation implements Stats {

    protected long count;
    protected double min;
    protected double max;
    protected double sum;
    protected double avg;

    protected final Map<String, String> valueAsString = new HashMap<>();

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
        return avg;
    }

    @Override
    public double getSum() {
        return sum;
    }

    @Override
    public String getMinAsString() {
        return valueAsString.getOrDefault(Fields.MIN_AS_STRING, Double.toString(min));
    }

    @Override
    public String getMaxAsString() {
        return valueAsString.getOrDefault(Fields.MAX_AS_STRING, Double.toString(max));
    }

    @Override
    public String getAvgAsString() {
        return valueAsString.getOrDefault(Fields.AVG_AS_STRING, Double.toString(avg));
    }

    @Override
    public String getSumAsString() {
        return valueAsString.getOrDefault(Fields.SUM_AS_STRING, Double.toString(sum));
    }

    @Override
    public double value(String name) {
        Metrics metrics = Metrics.valueOf(name);
        return switch (metrics) {
            case min -> min;
            case max -> max;
            case avg -> avg;
            case count -> count;
            case sum -> sum;
        };
    }

    @Override
    public Iterable<String> valueNames() {
        return METRIC_NAMES;
    }

    @Override
    public String getType() {
        return StatsAggregationBuilder.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.COUNT, count);
        if (count != 0) {
            builder.field(Fields.MIN, min);
            builder.field(Fields.MAX, max);
            builder.field(Fields.AVG, avg);
            builder.field(Fields.SUM, sum);
            if (valueAsString.get(Fields.MIN_AS_STRING) != null) {
                builder.field(Fields.MIN_AS_STRING, getMinAsString());
                builder.field(Fields.MAX_AS_STRING, getMaxAsString());
                builder.field(Fields.AVG_AS_STRING, getAvgAsString());
                builder.field(Fields.SUM_AS_STRING, getSumAsString());
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

    private static final ObjectParser<ParsedStats, Void> PARSER = new ObjectParser<>(
        ParsedStats.class.getSimpleName(),
        true,
        ParsedStats::new
    );

    static {
        declareStatsFields(PARSER);
    }

    protected static void declareStatsFields(ObjectParser<? extends ParsedStats, Void> objectParser) {
        declareAggregationFields(objectParser);
        objectParser.declareLong((agg, value) -> agg.count = value, new ParseField(Fields.COUNT));
        objectParser.declareField(
            (agg, value) -> agg.min = value,
            (parser, context) -> parseDouble(parser, Double.POSITIVE_INFINITY),
            new ParseField(Fields.MIN),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareField(
            (agg, value) -> agg.max = value,
            (parser, context) -> parseDouble(parser, Double.NEGATIVE_INFINITY),
            new ParseField(Fields.MAX),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareField(
            (agg, value) -> agg.avg = value,
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.AVG),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareField(
            (agg, value) -> agg.sum = value,
            (parser, context) -> parseDouble(parser, 0),
            new ParseField(Fields.SUM),
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.MIN_AS_STRING, value),
            new ParseField(Fields.MIN_AS_STRING)
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.MAX_AS_STRING, value),
            new ParseField(Fields.MAX_AS_STRING)
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.AVG_AS_STRING, value),
            new ParseField(Fields.AVG_AS_STRING)
        );
        objectParser.declareString(
            (agg, value) -> agg.valueAsString.put(Fields.SUM_AS_STRING, value),
            new ParseField(Fields.SUM_AS_STRING)
        );
    }

    public static ParsedStats fromXContent(XContentParser parser, final String name) {
        ParsedStats parsedStats = PARSER.apply(parser, null);
        parsedStats.setName(name);
        return parsedStats;
    }

    protected XContentBuilder otherStatsToXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
