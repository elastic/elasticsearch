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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TermsParser extends AbstractTermsParser {
    @Override
    protected TermsAggregationBuilder doCreateFactory(String aggregationName, ValuesSourceType valuesSourceType,
                                                      ValueType targetValueType, BucketCountThresholds bucketCountThresholds,
                                                      SubAggCollectionMode collectMode, String executionHint,
                                                      IncludeExclude incExc, Map<ParseField, Object> otherOptions) {
        TermsAggregationBuilder factory = new TermsAggregationBuilder(aggregationName, targetValueType);
        @SuppressWarnings("unchecked")
        List<OrderElement> orderElements = (List<OrderElement>) otherOptions.get(TermsAggregationBuilder.ORDER_FIELD);
        if (orderElements != null) {
            List<Terms.Order> orders = new ArrayList<>(orderElements.size());
            for (OrderElement orderElement : orderElements) {
                orders.add(resolveOrder(orderElement.key(), orderElement.asc()));
            }
            factory.order(orders);
        }
        if (bucketCountThresholds != null) {
            factory.bucketCountThresholds(bucketCountThresholds);
        }
        if (collectMode != null) {
            factory.collectMode(collectMode);
        }
        if (executionHint != null) {
            factory.executionHint(executionHint);
        }
        if (incExc != null) {
            factory.includeExclude(incExc);
        }
        Boolean showTermDocCountError = (Boolean) otherOptions.get(TermsAggregationBuilder.SHOW_TERM_DOC_COUNT_ERROR);
        if (showTermDocCountError != null) {
            factory.showTermDocCountError(showTermDocCountError);
        }
        return factory;
    }

    @Override
    public boolean parseSpecial(String aggregationName, XContentParser parser, ParseFieldMatcher parseFieldMatcher, Token token,
            String currentFieldName, Map<ParseField, Object> otherOptions) throws IOException {
        if (token == XContentParser.Token.START_OBJECT) {
            if (parseFieldMatcher.match(currentFieldName, TermsAggregationBuilder.ORDER_FIELD)) {
                otherOptions.put(TermsAggregationBuilder.ORDER_FIELD, Collections.singletonList(parseOrderParam(aggregationName, parser)));
                return true;
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            if (parseFieldMatcher.match(currentFieldName, TermsAggregationBuilder.ORDER_FIELD)) {
                List<OrderElement> orderElements = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token == XContentParser.Token.START_OBJECT) {
                        OrderElement orderParam = parseOrderParam(aggregationName, parser);
                        orderElements.add(orderParam);
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                                "Order elements must be of type object in [" + aggregationName + "] found token of type [" + token + "].");
                    }
                }
                otherOptions.put(TermsAggregationBuilder.ORDER_FIELD, orderElements);
                return true;
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            if (parseFieldMatcher.match(currentFieldName, TermsAggregationBuilder.SHOW_TERM_DOC_COUNT_ERROR)) {
                otherOptions.put(TermsAggregationBuilder.SHOW_TERM_DOC_COUNT_ERROR, parser.booleanValue());
                return true;
            }
        }
        return false;
    }

    private OrderElement parseOrderParam(String aggregationName, XContentParser parser) throws IOException {
        XContentParser.Token token;
        OrderElement orderParam = null;
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
                            "Unknown terms order direction [" + dir + "] in terms aggregation [" + aggregationName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unexpected token " + token + " for [order] in [" + aggregationName + "].");
            }
        }
        if (orderKey == null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "Must specify at least one field for [order] in [" + aggregationName + "].");
        } else {
            orderParam = new OrderElement(orderKey, orderAsc);
        }
        return orderParam;
    }

    static class OrderElement {
        private final String key;
        private final boolean asc;

        public OrderElement(String key, boolean asc) {
            this.key = key;
            this.asc = asc;
        }

        public String key() {
            return key;
        }

        public boolean asc() {
            return asc;
        }

    }

    @Override
    public TermsAggregator.BucketCountThresholds getDefaultBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(TermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS);
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
