/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.aggregations.metrics.ParsedSingleValueNumericMetricsAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ParsedBucketMetricValue extends ParsedSingleValueNumericMetricsAggregation implements BucketMetricValue {

    private List<String> keys = Collections.emptyList();

    @Override
    public String[] keys() {
        return this.keys.toArray(new String[keys.size()]);
    }

    @Override
    public String getType() {
        return InternalBucketMetricValue.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = Double.isInfinite(value) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value : null);
        if (hasValue && valueAsString != null) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
        }
        builder.startArray(InternalBucketMetricValue.KEYS_FIELD.getPreferredName());
        for (String key : keys) {
            builder.value(key);
        }
        builder.endArray();
        return builder;
    }

    private static final ObjectParser<ParsedBucketMetricValue, Void> PARSER = new ObjectParser<>(
        ParsedBucketMetricValue.class.getSimpleName(),
        true,
        ParsedBucketMetricValue::new
    );

    static {
        declareSingleValueFields(PARSER, Double.NEGATIVE_INFINITY);
        PARSER.declareStringArray((agg, value) -> agg.keys = value, InternalBucketMetricValue.KEYS_FIELD);
    }

    public static ParsedBucketMetricValue fromXContent(XContentParser parser, final String name) {
        ParsedBucketMetricValue bucketMetricValue = PARSER.apply(parser, null);
        bucketMetricValue.setName(name);
        return bucketMetricValue;
    }
}
