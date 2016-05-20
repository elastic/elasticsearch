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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.AnyValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DiversifiedSamplerParser extends AnyValuesSourceParser {
    public DiversifiedSamplerParser() {
        super(true, false);
    }

    @Override
    protected DiversifiedAggregationBuilder createFactory(String aggregationName, ValuesSourceType valuesSourceType,
                                                          ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        DiversifiedAggregationBuilder factory = new DiversifiedAggregationBuilder(aggregationName);
        Integer shardSize = (Integer) otherOptions.get(SamplerAggregator.SHARD_SIZE_FIELD);
        if (shardSize != null) {
            factory.shardSize(shardSize);
        }
        Integer maxDocsPerValue = (Integer) otherOptions.get(SamplerAggregator.MAX_DOCS_PER_VALUE_FIELD);
        if (maxDocsPerValue != null) {
            factory.maxDocsPerValue(maxDocsPerValue);
        }
        String executionHint = (String) otherOptions.get(SamplerAggregator.EXECUTION_HINT_FIELD);
        if (executionHint != null) {
            factory.executionHint(executionHint);
        }
        return factory;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, XContentParser.Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (token == XContentParser.Token.VALUE_NUMBER) {
            if (parseFieldMatcher.match(currentFieldName, SamplerAggregator.SHARD_SIZE_FIELD)) {
                int shardSize = parser.intValue();
                otherOptions.put(SamplerAggregator.SHARD_SIZE_FIELD, shardSize);
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, SamplerAggregator.MAX_DOCS_PER_VALUE_FIELD)) {
                int maxDocsPerValue = parser.intValue();
                otherOptions.put(SamplerAggregator.MAX_DOCS_PER_VALUE_FIELD, maxDocsPerValue);
                return true;
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            if (parseFieldMatcher.match(currentFieldName, SamplerAggregator.EXECUTION_HINT_FIELD)) {
                String executionHint = parser.text();
                otherOptions.put(SamplerAggregator.EXECUTION_HINT_FIELD, executionHint);
                return true;
            }
        }
        return false;
    }
}
