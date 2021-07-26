/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ParsedDerivative extends ParsedSimpleValue implements Derivative {

    private double normalizedValue;
    private String normalizedAsString;
    private boolean hasNormalizationFactor;
    private static final ParseField NORMALIZED_AS_STRING = new ParseField("normalized_value_as_string");
    private static final ParseField NORMALIZED = new ParseField("normalized_value");

    @Override
    public double normalizedValue() {
        return this.normalizedValue;
    }

    @Override
    public String getType() {
        return DerivativePipelineAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedDerivative, Void> PARSER = new ObjectParser<>(ParsedDerivative.class.getSimpleName(), true,
            ParsedDerivative::new);

    static {
        declareSingleValueFields(PARSER, Double.NaN);
        PARSER.declareField((agg, normalized) -> {
            agg.normalizedValue = normalized;
            agg.hasNormalizationFactor = true;
        }, (parser, context) -> parseDouble(parser, Double.NaN), NORMALIZED, ValueType.DOUBLE_OR_NULL);
        PARSER.declareString((agg, normalAsString) -> agg.normalizedAsString = normalAsString, NORMALIZED_AS_STRING);
    }

    public static ParsedDerivative fromXContent(XContentParser parser, final String name) {
        ParsedDerivative derivative = PARSER.apply(parser, null);
        derivative.setName(name);
        return derivative;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        super.doXContentBody(builder, params);
        if (hasNormalizationFactor) {
            boolean hasValue = Double.isNaN(normalizedValue) == false;
            builder.field(NORMALIZED.getPreferredName(), hasValue ? normalizedValue : null);
            if (hasValue && normalizedAsString != null) {
                builder.field(NORMALIZED_AS_STRING.getPreferredName(), normalizedAsString);
            }
        }
        return builder;
    }
}
