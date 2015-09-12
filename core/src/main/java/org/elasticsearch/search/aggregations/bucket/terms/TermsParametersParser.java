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
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class TermsParametersParser extends AbstractTermsParametersParser {

    private static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(1, 0, 10, -1);

    public List<OrderElement> getOrderElements() {
        return orderElements;
    }
    
    public boolean showTermDocCountError() {
        return showTermDocCountError;
    }

    List<OrderElement> orderElements;
    private boolean showTermDocCountError = false;

    public TermsParametersParser() {
        orderElements = new ArrayList<>(1);
        orderElements.add(new OrderElement("_count", false));
    }

    @Override
    public void parseSpecial(String aggregationName, XContentParser parser, SearchContext context, XContentParser.Token token, String currentFieldName) throws IOException {
        if (token == XContentParser.Token.START_OBJECT) {
            if ("order".equals(currentFieldName)) {
                this.orderElements = Collections.singletonList(parseOrderParam(aggregationName, parser, context));
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                        + currentFieldName + "].", parser.getTokenLocation());
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            if ("order".equals(currentFieldName)) {
                orderElements = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token == XContentParser.Token.START_OBJECT) {
                        OrderElement orderParam = parseOrderParam(aggregationName, parser, context);
                        orderElements.add(orderParam);
                    } else {
                        throw new SearchParseException(context, "Order elements must be of type object in [" + aggregationName + "].",
                                parser.getTokenLocation());
                    }
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                        + currentFieldName + "].", parser.getTokenLocation());
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            if (context.parseFieldMatcher().match(currentFieldName, SHOW_TERM_DOC_COUNT_ERROR)) {
                showTermDocCountError = parser.booleanValue();
            }
        } else {
            throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName
                    + "].", parser.getTokenLocation());
        }
    }

    private OrderElement parseOrderParam(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
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
                    throw new SearchParseException(context, "Unknown terms order direction [" + dir + "] in terms aggregation ["
                            + aggregationName + "]", parser.getTokenLocation());
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " for [order] in [" + aggregationName + "].",
                        parser.getTokenLocation());
            }
        }
        if (orderKey == null) {
            throw new SearchParseException(context, "Must specify at least one field for [order] in [" + aggregationName + "].",
                    parser.getTokenLocation());
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
        return new TermsAggregator.BucketCountThresholds(DEFAULT_BUCKET_COUNT_THRESHOLDS);
    }
}
