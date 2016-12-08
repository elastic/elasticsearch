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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.AnyValuesSourceParser;

import java.io.IOException;

public class TermsParser extends AnyValuesSourceParser {

    public static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");
    public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");

    private final ObjectParser<TermsAggregationBuilder, QueryParseContext> parser;

    public TermsParser() {
        parser = new ObjectParser<>(TermsAggregationBuilder.NAME);
        addFields(parser, true, true);

        parser.declareBoolean(TermsAggregationBuilder::showTermDocCountError,
                TermsAggregationBuilder.SHOW_TERM_DOC_COUNT_ERROR);

        parser.declareInt(TermsAggregationBuilder::shardSize, SHARD_SIZE_FIELD_NAME);

        parser.declareLong(TermsAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD_NAME);

        parser.declareLong(TermsAggregationBuilder::shardMinDocCount, SHARD_MIN_DOC_COUNT_FIELD_NAME);

        parser.declareInt(TermsAggregationBuilder::size, REQUIRED_SIZE_FIELD_NAME);

        parser.declareString(TermsAggregationBuilder::executionHint, EXECUTION_HINT_FIELD_NAME);

        parser.declareField(TermsAggregationBuilder::collectMode,
                (p, c) -> SubAggCollectionMode.parse(p.text(), c.getParseFieldMatcher()),
                SubAggCollectionMode.KEY, ObjectParser.ValueType.STRING);

        parser.declareObjectArray(TermsAggregationBuilder::order, TermsParser::parseOrderParam,
                TermsAggregationBuilder.ORDER_FIELD);

        parser.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
                IncludeExclude::parseInclude, IncludeExclude.INCLUDE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);

        parser.declareField((b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
                IncludeExclude::parseExclude, IncludeExclude.EXCLUDE_FIELD, ObjectParser.ValueType.STRING_ARRAY);
    }

    @Override
    public AggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        return parser.parse(context.parser(), new TermsAggregationBuilder(aggregationName, null), context);
    }

    private static Terms.Order parseOrderParam(XContentParser parser, QueryParseContext context) throws IOException {
        XContentParser.Token token;
        Terms.Order orderParam = null;
        String orderKey = null;
        boolean orderAsc = false;
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
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown terms order direction [" + dir + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unexpected token " + token + " for [order]");
            }
        }
        if (orderKey == null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "Must specify at least one field for [order]");
        } else {
            orderParam = resolveOrder(orderKey, orderAsc);
        }
        return orderParam;
    }

    static Terms.Order resolveOrder(String key, boolean asc) {
        if ("_term".equals(key)) {
            return Order.term(asc);
        }
        if ("_count".equals(key)) {
            return Order.count(asc);
        }
        return Order.aggregation(key, asc);
    }
}
