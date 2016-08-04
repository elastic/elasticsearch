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
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.NumericValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

/**
 * A parser for date histograms. This translates json into an
 * {@link HistogramAggregationBuilder} instance.
 */
public class HistogramParser extends NumericValuesSourceParser {

    private static final ObjectParser<double[], ParseFieldMatcherSupplier> EXTENDED_BOUNDS_PARSER = new ObjectParser<>(
            Histogram.EXTENDED_BOUNDS_FIELD.getPreferredName(),
            () -> new double[]{ Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY });
    static {
        EXTENDED_BOUNDS_PARSER.declareDouble((bounds, d) -> bounds[0] = d, new ParseField("min"));
        EXTENDED_BOUNDS_PARSER.declareDouble((bounds, d) -> bounds[1] = d, new ParseField("max"));
    }

    public HistogramParser() {
        super(true, true, false);
    }

    @Override
    protected HistogramAggregationBuilder createFactory(String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        HistogramAggregationBuilder factory = new HistogramAggregationBuilder(aggregationName);
        Double interval = (Double) otherOptions.get(Histogram.INTERVAL_FIELD);
        if (interval == null) {
            throw new ParsingException(null, "Missing required field [interval] for histogram aggregation [" + aggregationName + "]");
        } else {
            factory.interval(interval);
        }
        Double offset = (Double) otherOptions.get(Histogram.OFFSET_FIELD);
        if (offset != null) {
            factory.offset(offset);
        }

        double[] extendedBounds = (double[]) otherOptions.get(Histogram.EXTENDED_BOUNDS_FIELD);
        if (extendedBounds != null) {
            factory.extendedBounds(extendedBounds[0], extendedBounds[1]);
        }
        Boolean keyed = (Boolean) otherOptions.get(Histogram.KEYED_FIELD);
        if (keyed != null) {
            factory.keyed(keyed);
        }
        Long minDocCount = (Long) otherOptions.get(Histogram.MIN_DOC_COUNT_FIELD);
        if (minDocCount != null) {
            factory.minDocCount(minDocCount);
        }
        InternalOrder order = (InternalOrder) otherOptions.get(Histogram.ORDER_FIELD);
        if (order != null) {
            factory.order(order);
        }
        return factory;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (token.isValue()) {
            if (parseFieldMatcher.match(currentFieldName, Histogram.INTERVAL_FIELD)) {
                otherOptions.put(Histogram.INTERVAL_FIELD, parser.doubleValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, Histogram.MIN_DOC_COUNT_FIELD)) {
                otherOptions.put(Histogram.MIN_DOC_COUNT_FIELD, parser.longValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, Histogram.KEYED_FIELD)) {
                otherOptions.put(Histogram.KEYED_FIELD, parser.booleanValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, Histogram.OFFSET_FIELD)) {
                otherOptions.put(Histogram.OFFSET_FIELD, parser.doubleValue());
                return true;
            } else {
                return false;
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            if (parseFieldMatcher.match(currentFieldName, Histogram.ORDER_FIELD)) {
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
                otherOptions.put(Histogram.ORDER_FIELD, order);
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, Histogram.EXTENDED_BOUNDS_FIELD)) {
                double[] bounds = EXTENDED_BOUNDS_PARSER.apply(parser, () -> parseFieldMatcher);
                otherOptions.put(Histogram.EXTENDED_BOUNDS_FIELD, bounds);
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
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
