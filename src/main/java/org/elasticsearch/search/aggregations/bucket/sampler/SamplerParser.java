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
package org.elasticsearch.search.aggregations.bucket.sampler;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class SamplerParser implements Aggregator.Parser {

    public static final int DEFAULT_SHARD_SAMPLE_SIZE = 100;
    public static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");
    public static final ParseField RANDOM_SAMPLE_FIELD = new ParseField("random_sample");
    public static final boolean DEFAULT_USE_RANDOM_SAMPLE = false;

    @Override
    public String type() {
        return InternalSampler.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        boolean useRandomSample = DEFAULT_USE_RANDOM_SAMPLE;
        int shardSize = DEFAULT_SHARD_SAMPLE_SIZE;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (SHARD_SIZE_FIELD.match(currentFieldName)) {
                    shardSize = parser.intValue();
                } else {
                    throw new SearchParseException(context, "Unsupported property \"" + currentFieldName + "\" for aggregation \""
                            + aggregationName);
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (RANDOM_SAMPLE_FIELD.match(currentFieldName)) {
                    useRandomSample = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unsupported property \"" + currentFieldName + "\" for aggregation \""
                            + aggregationName);
                }
            } else {
                throw new SearchParseException(context, "Unsupported property \"" + currentFieldName + "\" for aggregation \""
                        + aggregationName);
            }
        }
        return new SamplerAggregator.Factory(aggregationName, shardSize, useRandomSample);
    }

}
