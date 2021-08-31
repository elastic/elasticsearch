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
import org.elasticsearch.search.aggregations.metrics.ParsedSingleValueNumericMetricsAggregation;

import java.io.IOException;

public class ParsedSimpleValue extends ParsedSingleValueNumericMetricsAggregation implements SimpleValue {

    @Override
    public String getType() {
        return InternalSimpleValue.NAME;
    }

    private static final ObjectParser<ParsedSimpleValue, Void> PARSER = new ObjectParser<>(
        ParsedSimpleValue.class.getSimpleName(),
        true,
        ParsedSimpleValue::new
    );

    static {
        declareSingleValueFields(PARSER, Double.NaN);
    }

    public static ParsedSimpleValue fromXContent(XContentParser parser, final String name) {
        ParsedSimpleValue simpleValue = PARSER.apply(parser, null);
        simpleValue.setName(name);
        return simpleValue;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = Double.isNaN(value) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value : null);
        if (hasValue && valueAsString != null) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
        }
        return builder;
    }
}
