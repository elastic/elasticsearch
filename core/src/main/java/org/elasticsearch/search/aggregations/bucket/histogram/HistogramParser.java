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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.NumericValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

/**
 * Parses the histogram request
 */
public class HistogramParser extends NumericValuesSourceParser {

    public HistogramParser() {
        super(true, true, false);
    }

    protected HistogramParser(boolean timezoneAware) {
        super(true, true, timezoneAware);
    }

    @Override
    protected AbstractHistogramBuilder<?> createFactory(String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        HistogramAggregationBuilder factory = new HistogramAggregationBuilder(aggregationName);
        Long interval = (Long) otherOptions.get(Rounding.Interval.INTERVAL_FIELD);
        if (interval == null) {
            throw new ParsingException(null, "Missing required field [interval] for histogram aggregation [" + aggregationName + "]");
        } else {
            factory.interval(interval);
        }
        Long offset = (Long) otherOptions.get(Rounding.OffsetRounding.OFFSET_FIELD);
        if (offset != null) {
            factory.offset(offset);
        }

        ExtendedBounds extendedBounds = (ExtendedBounds) otherOptions.get(ExtendedBounds.EXTENDED_BOUNDS_FIELD);
        if (extendedBounds != null) {
            factory.extendedBounds(extendedBounds);
        }
        Boolean keyed = (Boolean) otherOptions.get(HistogramAggregator.KEYED_FIELD);
        if (keyed != null) {
            factory.keyed(keyed);
        }
        Long minDocCount = (Long) otherOptions.get(HistogramAggregator.MIN_DOC_COUNT_FIELD);
        if (minDocCount != null) {
            factory.minDocCount(minDocCount);
        }
        InternalOrder order = (InternalOrder) otherOptions.get(HistogramAggregator.ORDER_FIELD);
        if (order != null) {
            factory.order(order);
        }
        return factory;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (token.isValue()) {
            if (parseFieldMatcher.match(currentFieldName, Rounding.Interval.INTERVAL_FIELD)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    otherOptions.put(Rounding.Interval.INTERVAL_FIELD, parseStringInterval(parser.text()));
                    return true;
                } else {
                    otherOptions.put(Rounding.Interval.INTERVAL_FIELD, parser.longValue());
                    return true;
                }
            } else if (parseFieldMatcher.match(currentFieldName, HistogramAggregator.MIN_DOC_COUNT_FIELD)) {
                otherOptions.put(HistogramAggregator.MIN_DOC_COUNT_FIELD, parser.longValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, HistogramAggregator.KEYED_FIELD)) {
                otherOptions.put(HistogramAggregator.KEYED_FIELD, parser.booleanValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, Rounding.OffsetRounding.OFFSET_FIELD)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    otherOptions.put(Rounding.OffsetRounding.OFFSET_FIELD, parseStringOffset(parser.text()));
                    return true;
                } else {
                    otherOptions.put(Rounding.OffsetRounding.OFFSET_FIELD, parser.longValue());
                    return true;
                }
            } else {
                return false;
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            if (parseFieldMatcher.match(currentFieldName, HistogramAggregator.ORDER_FIELD)) {
                InternalOrder order = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        String dir = parser.text();
                        boolean asc = "asc".equals(dir);
                        if (!asc && !"desc".equals(dir)) {
                            throw new ParsingException(parser.getTokenLocation(), "Unknown order direction in aggregation ["
                                    + aggregationName + "]: [" + dir
                                    + "]. Should be either [asc] or [desc]");
                        }
                        order = resolveOrder(currentFieldName, asc);
                    }
                }
                otherOptions.put(HistogramAggregator.ORDER_FIELD, order);
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, ExtendedBounds.EXTENDED_BOUNDS_FIELD)) {
                ExtendedBounds extendedBounds = ExtendedBounds.fromXContent(parser, parseFieldMatcher, aggregationName);
                otherOptions.put(ExtendedBounds.EXTENDED_BOUNDS_FIELD, extendedBounds);
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    protected Object parseStringInterval(String interval) {
        return Long.valueOf(interval);
    }

    protected long parseStringOffset(String offset) throws IOException {
        return Long.valueOf(offset);
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
