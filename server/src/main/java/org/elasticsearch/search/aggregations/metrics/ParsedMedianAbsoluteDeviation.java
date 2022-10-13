/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ParsedMedianAbsoluteDeviation extends ParsedSingleValueNumericMetricsAggregation implements MedianAbsoluteDeviation {

    private static final ObjectParser<ParsedMedianAbsoluteDeviation, Void> PARSER = new ObjectParser<>(
        ParsedMedianAbsoluteDeviation.class.getSimpleName(),
        true,
        ParsedMedianAbsoluteDeviation::new
    );

    static {
        declareSingleValueFields(PARSER, Double.NaN);
    }

    public static ParsedMedianAbsoluteDeviation fromXContent(XContentParser parser, String name) {
        ParsedMedianAbsoluteDeviation parsedMedianAbsoluteDeviation = PARSER.apply(parser, null);
        parsedMedianAbsoluteDeviation.setName(name);
        return parsedMedianAbsoluteDeviation;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final boolean hasValue = Double.isFinite(getMedianAbsoluteDeviation());
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? getMedianAbsoluteDeviation() : null);
        if (hasValue && valueAsString != null) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
        }
        return builder;
    }

    @Override
    public double getMedianAbsoluteDeviation() {
        return value();
    }

    @Override
    public String getType() {
        return MedianAbsoluteDeviationAggregationBuilder.NAME;
    }
}
