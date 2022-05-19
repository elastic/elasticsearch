/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class ParsedKSTest extends ParsedAggregation {

    @SuppressWarnings("unchecked")
    public static ParsedKSTest fromXContent(XContentParser parser, final String name) throws IOException {
        Map<String, Object> values = parser.map();
        Map<String, Double> doubleValues = Maps.newMapWithExpectedSize(values.size());
        for (Alternative alternative : Alternative.values()) {
            Double value = (Double) values.get(alternative.toString());
            if (value != null) {
                doubleValues.put(alternative.toString(), value);
            }
        }
        ParsedKSTest parsed = new ParsedKSTest(
            doubleValues,
            (Map<String, Object>) values.get(InternalAggregation.CommonFields.META.getPreferredName())
        );
        parsed.setName(name);
        return parsed;
    }

    private final Map<String, Double> modes;

    ParsedKSTest(Map<String, Double> modes, Map<String, Object> metadata) {
        this.modes = modes;
        this.metadata = metadata;
    }

    Map<String, Double> getModes() {
        return modes;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, Double> kv : modes.entrySet()) {
            builder.field(kv.getKey(), kv.getValue());
        }
        return builder;
    }

    @Override
    public String getType() {
        return BucketCountKSTestAggregationBuilder.NAME.getPreferredName();
    }
}
