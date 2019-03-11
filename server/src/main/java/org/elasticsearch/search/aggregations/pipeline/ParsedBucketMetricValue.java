/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.metrics.ParsedSingleValueNumericMetricsAggregation;

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
        boolean hasValue = !Double.isInfinite(value);
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
            ParsedBucketMetricValue.class.getSimpleName(), true, ParsedBucketMetricValue::new);

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
