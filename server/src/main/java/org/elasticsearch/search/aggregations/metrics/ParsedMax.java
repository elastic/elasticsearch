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

public class ParsedMax extends ParsedSingleValueNumericMetricsAggregation implements Max {

    @Override
    public double getValue() {
        return value();
    }

    @Override
    public String getType() {
        return MaxAggregationBuilder.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = Double.isInfinite(value) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value : null);
        if (hasValue && valueAsString != null) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
        }
        return builder;
    }

    private static final ObjectParser<ParsedMax, Void> PARSER = new ObjectParser<>(ParsedMax.class.getSimpleName(), true, ParsedMax::new);

    static {
        declareSingleValueFields(PARSER, Double.NEGATIVE_INFINITY);
    }

    public static ParsedMax fromXContent(XContentParser parser, final String name) {
        ParsedMax max = PARSER.apply(parser, null);
        max.setName(name);
        return max;
    }
}
