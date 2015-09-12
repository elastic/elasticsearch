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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.aggregations.support.format.ValueParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Parses the histogram request
 */
public class HistogramParser implements Aggregator.Parser {

    static final ParseField EXTENDED_BOUNDS = new ParseField("extended_bounds");

    @Override
    public String type() {
        return InternalHistogram.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.numeric(aggregationName, InternalHistogram.TYPE, context)
                .targetValueType(ValueType.NUMERIC)
                .formattable(true)
                .build();

        boolean keyed = false;
        long minDocCount = 0;
        InternalOrder order = (InternalOrder) InternalOrder.KEY_ASC;
        long interval = -1;
        ExtendedBounds extendedBounds = null;
        long offset = 0;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token.isValue()) {
                if ("interval".equals(currentFieldName)) {
                    interval = parser.longValue();
                } else if ("min_doc_count".equals(currentFieldName) || "minDocCount".equals(currentFieldName)) {
                    minDocCount = parser.longValue();
                } else if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else if ("offset".equals(currentFieldName)) {
                    offset = parser.longValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in aggregation [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("order".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            String dir = parser.text();
                            boolean asc = "asc".equals(dir);
                            if (!asc && !"desc".equals(dir)) {
                                throw new SearchParseException(context, "Unknown order direction [" + dir + "] in aggregation ["
                                        + aggregationName + "]. Should be either [asc] or [desc]", parser.getTokenLocation());
                            }
                            order = resolveOrder(currentFieldName, asc);
                        }
                    }
                } else if (context.parseFieldMatcher().match(currentFieldName, EXTENDED_BOUNDS)) {
                    extendedBounds = new ExtendedBounds();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if ("min".equals(currentFieldName)) {
                                extendedBounds.min = parser.longValue(true);
                            } else if ("max".equals(currentFieldName)) {
                                extendedBounds.max = parser.longValue(true);
                            } else {
                                throw new SearchParseException(context, "Unknown extended_bounds key for a " + token + " in aggregation ["
                                        + aggregationName + "]: [" + currentFieldName + "].", parser.getTokenLocation());
                            }
                        }
                    }

                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in aggregation [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in aggregation [" + aggregationName + "].",
                        parser.getTokenLocation());
            }
        }

        if (interval < 1) {
            throw new SearchParseException(context,
                    "Missing required field [interval] for histogram aggregation [" + aggregationName + "]", parser.getTokenLocation());
        }

        Rounding rounding = new Rounding.Interval(interval);
        if (offset != 0) {
            rounding = new Rounding.OffsetRounding((Rounding.Interval) rounding, offset);
        }

        if (extendedBounds != null) {
            // with numeric histogram, we can process here and fail fast if the bounds are invalid
            extendedBounds.processAndValidate(aggregationName, context, ValueParser.RAW);
        }

        return new HistogramAggregator.Factory(aggregationName, vsParser.config(), rounding, order, keyed, minDocCount, extendedBounds,
                new InternalHistogram.Factory());

    }

    static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        return new InternalOrder.Aggregation(key, asc);
    }
}
