/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.metrics.ParsedPercentiles;
import org.elasticsearch.search.aggregations.metrics.Percentiles;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class ParsedPercentilesBucket extends ParsedPercentiles implements Percentiles {

    @Override
    public String getType() {
        return PercentilesBucketPipelineAggregationBuilder.NAME;
    }

    @Override
    public double percentile(double percent) throws IllegalArgumentException {
        Double value = percentiles.get(percent);
        if (value == null) {
            throw new IllegalArgumentException(
                "Percent requested ["
                    + String.valueOf(percent)
                    + "] was not"
                    + " one of the computed percentiles. Available keys are: "
                    + percentiles.keySet()
            );
        }
        return value;
    }

    @Override
    public String percentileAsString(double percent) {
        double value = percentile(percent); // check availability as unformatted value
        String valueAsString = percentilesAsString.get(percent);
        if (valueAsString != null) {
            return valueAsString;
        } else {
            return Double.toString(value);
        }
    }

    @Override
    public double value(String name) {
        return percentile(Double.parseDouble(name));
    }

    @Override
    public Iterable<String> valueNames() {
        return percentiles.keySet().stream().map(d -> d.toString()).collect(Collectors.toList());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("values");
        for (Entry<Double, Double> percent : percentiles.entrySet()) {
            double value = percent.getValue();
            boolean hasValue = Double.isNaN(value) == false;
            Double key = percent.getKey();
            builder.field(Double.toString(key), hasValue ? value : null);
            String valueAsString = percentilesAsString.get(key);
            if (hasValue && valueAsString != null) {
                builder.field(key + "_as_string", valueAsString);
            }
        }
        builder.endObject();
        return builder;
    }

    private static final ObjectParser<ParsedPercentilesBucket, Void> PARSER = new ObjectParser<>(
        ParsedPercentilesBucket.class.getSimpleName(),
        true,
        ParsedPercentilesBucket::new
    );

    static {
        ParsedPercentiles.declarePercentilesFields(PARSER);
    }

    public static ParsedPercentilesBucket fromXContent(XContentParser parser, String name) throws IOException {
        ParsedPercentilesBucket aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}
