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


import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;

/**
 *
 */
public class SamplerParser extends Aggregator.Parser {

    @Override
    protected InternalAggregation.Type type() {
        return InternalSampler.TYPE;
    }

    @Override
    public SamplerAggregatorBuilder parse(String aggregationName, XContentParser parser, QueryParseContext context)
            throws IOException {

        XContentParser.Token token;
        String currentFieldName = null;
        Integer shardSize = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (context.parseFieldMatcher().match(currentFieldName, SamplerAggregator.SHARD_SIZE_FIELD)) {
                    shardSize = parser.intValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unsupported property \"" + currentFieldName + "\" for aggregation \"" + aggregationName);
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unsupported property \"" + currentFieldName + "\" for aggregation \"" + aggregationName);
            }
        }

        SamplerAggregatorBuilder factory = new SamplerAggregatorBuilder(aggregationName);
        if (shardSize != null) {
            factory.shardSize(shardSize);
        }
        return factory;
    }

    @Override
    public AggregatorBuilder<?> read(StreamInput in) throws IOException {
        return new SamplerAggregatorBuilder(in);
    }
}
