/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ParsedValueCount extends ParsedAggregation implements ValueCount {

    private long valueCount;

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return valueCount;
    }

    @Override
    public String getValueAsString() {
        // InternalValueCount doesn't print "value_as_string", but you can get a formatted value using
        // getValueAsString() using the raw formatter and converting the value to double
        return Double.toString(valueCount);
    }

    @Override
    public String getType() {
        return ValueCountAggregationBuilder.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), valueCount);
        return builder;
    }

    private static final ObjectParser<ParsedValueCount, Void> PARSER = new ObjectParser<>(
        ParsedValueCount.class.getSimpleName(),
        true,
        ParsedValueCount::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareLong((agg, value) -> agg.valueCount = value, CommonFields.VALUE);
    }

    public static ParsedValueCount fromXContent(XContentParser parser, final String name) {
        ParsedValueCount sum = PARSER.apply(parser, null);
        sum.setName(name);
        return sum;
    }
}
