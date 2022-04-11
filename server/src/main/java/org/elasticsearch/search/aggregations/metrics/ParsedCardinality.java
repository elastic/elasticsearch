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

public class ParsedCardinality extends ParsedAggregation implements Cardinality {

    private long cardinalityValue;

    @Override
    public String getValueAsString() {
        return Double.toString((double) cardinalityValue);
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return cardinalityValue;
    }

    @Override
    public String getType() {
        return CardinalityAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedCardinality, Void> PARSER = new ObjectParser<>(
        ParsedCardinality.class.getSimpleName(),
        true,
        ParsedCardinality::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareLong((agg, value) -> agg.cardinalityValue = value, CommonFields.VALUE);
    }

    public static ParsedCardinality fromXContent(XContentParser parser, final String name) {
        ParsedCardinality cardinality = PARSER.apply(parser, null);
        cardinality.setName(name);
        return cardinality;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), cardinalityValue);
        return builder;
    }
}
