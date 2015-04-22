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
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class SamplerParser implements Aggregator.Parser {

    public static final int DEFAULT_SHARD_SAMPLE_SIZE = 100;
    public static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");
    public static final ParseField MAX_DOCS_PER_VALUE_FIELD = new ParseField("max_docs_per_value");
    public static final ParseField EXECUTION_HINT_FIELD = new ParseField("execution_hint");
    public static final boolean DEFAULT_USE_GLOBAL_ORDINALS = false;
    public static final int MAX_DOCS_PER_VALUE_DEFAULT = 1;


    @Override
    public String type() {
        return InternalSampler.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        XContentParser.Token token;
        String currentFieldName = null;
        String executionHint = null;
        int shardSize = DEFAULT_SHARD_SAMPLE_SIZE;
        int maxDocsPerValue = MAX_DOCS_PER_VALUE_DEFAULT;
        ValuesSourceParser vsParser = null;
        boolean diversityChoiceMade = false;

        vsParser = ValuesSourceParser.any(aggregationName, InternalSampler.TYPE, context).scriptable(true).formattable(false).build();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (SHARD_SIZE_FIELD.match(currentFieldName)) {
                    shardSize = parser.intValue();
                } else if (MAX_DOCS_PER_VALUE_FIELD.match(currentFieldName)) {
                    diversityChoiceMade = true;
                    maxDocsPerValue = parser.intValue();
                } else {
                    throw new SearchParseException(context, "Unsupported property \"" + currentFieldName + "\" for aggregation \""
                            + aggregationName);
                }
            } else if (!vsParser.token(currentFieldName, token, parser)) {
                if (EXECUTION_HINT_FIELD.match(currentFieldName)) {
                    executionHint = parser.text();
                } else {
                    throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unsupported property \"" + currentFieldName + "\" for aggregation \""
                        + aggregationName);
            }
        }

        ValuesSourceConfig vsConfig = vsParser.config();
        if (vsConfig.valid()) {
            return new SamplerAggregator.DiversifiedFactory(aggregationName, shardSize, executionHint, vsConfig, maxDocsPerValue);
        } else {
            if (diversityChoiceMade) {
                throw new SearchParseException(context, "Sampler aggregation has " + MAX_DOCS_PER_VALUE_FIELD.getPreferredName()
                        + " setting but no \"field\" or \"script\" setting to provide values for aggregation \"" + aggregationName + "\"");

            }
            return new SamplerAggregator.Factory(aggregationName, shardSize);
        }
    }


}
