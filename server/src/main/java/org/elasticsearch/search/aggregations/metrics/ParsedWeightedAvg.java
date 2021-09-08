/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ParsedWeightedAvg extends ParsedSingleValueNumericMetricsAggregation implements WeightedAvg {

    @Override
    public double getValue() {
        return value();
    }

    @Override
    public String getType() {
        return WeightedAvgAggregationBuilder.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        // InternalWeightedAvg renders value only if the avg normalizer (count) is not 0.
        // We parse back `null` as Double.POSITIVE_INFINITY so we check for that value here to get the same xContent output
        boolean hasValue = value != Double.POSITIVE_INFINITY;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value : null);
        if (hasValue && valueAsString != null) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
        }
        return builder;
    }

    private static final ObjectParser<ParsedWeightedAvg, Void> PARSER = new ObjectParser<>(
        ParsedWeightedAvg.class.getSimpleName(),
        true,
        ParsedWeightedAvg::new
    );

    static {
        declareSingleValueFields(PARSER, Double.POSITIVE_INFINITY);
    }

    public static ParsedWeightedAvg fromXContent(XContentParser parser, final String name) {
        ParsedWeightedAvg avg = PARSER.apply(parser, null);
        avg.setName(name);
        return avg;
    }
}
