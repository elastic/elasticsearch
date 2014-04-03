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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class TermsParser implements Aggregator.Parser {

    @Override
    public String type() {
        return StringTerms.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        int requiredSize = 10;
        int shardSize = -1;
        String orderKey = "_count";
        boolean orderAsc = false;

        String executionHint = null;
        long minDocCount = 1;

        ValuesSourceParser vsParser = ValuesSourceParser.any(aggregationName, StringTerms.TYPE, context)
                .requiresSortedValues(true)
                .requiresUniqueValues(true)
                .formattable(true)
                .build();

        IncludeExclude.Parser incExcParser = new IncludeExclude.Parser(aggregationName, StringTerms.TYPE, context);

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (incExcParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("execution_hint".equals(currentFieldName) || "executionHint".equals(currentFieldName)) {
                    executionHint = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("size".equals(currentFieldName)) {
                    requiredSize = parser.intValue();
                } else if ("shard_size".equals(currentFieldName) || "shardSize".equals(currentFieldName)) {
                    shardSize = parser.intValue();
                } else if ("min_doc_count".equals(currentFieldName) || "minDocCount".equals(currentFieldName)) {
                    minDocCount = parser.intValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("order".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            orderKey = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            String dir = parser.text();
                            if ("asc".equalsIgnoreCase(dir)) {
                                orderAsc = true;
                            } else if ("desc".equalsIgnoreCase(dir)) {
                                orderAsc = false;
                            } else {
                                throw new SearchParseException(context, "Unknown terms order direction [" + dir + "] in terms aggregation [" + aggregationName + "]");
                            }
                        } else {
                            throw new SearchParseException(context, "Unexpected token " + token + " for [order] in [" + aggregationName + "].");
                        }
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (shardSize == 0) {
            shardSize = Integer.MAX_VALUE;
        }

        if (requiredSize == 0) {
            requiredSize = Integer.MAX_VALUE;
        }

        // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return <size>
        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }

        IncludeExclude includeExclude = incExcParser.includeExclude();
        InternalOrder order = resolveOrder(orderKey, orderAsc);
        return new TermsAggregatorFactory(aggregationName, vsParser.config(), order, requiredSize, shardSize, minDocCount, includeExclude, executionHint);
    }

    static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_term".equals(key)) {
            return asc ? InternalOrder.TERM_ASC : InternalOrder.TERM_DESC;
        }
        if ("_count".equals(key)) {
            return asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC;
        }
        return new InternalOrder.Aggregation(key, asc);
    }

}
