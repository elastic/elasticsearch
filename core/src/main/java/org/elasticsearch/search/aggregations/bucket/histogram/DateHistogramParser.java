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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DateHistogramParser extends HistogramParser {

    public DateHistogramParser() {
        super(true);
    }

    @Override
    protected Object parseStringInterval(String text) {
        return new DateHistogramInterval(text);
    }

    @Override
    protected DateHistogramAggregationBuilder createFactory(String aggregationName, ValuesSourceType valuesSourceType,
                                                            ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        DateHistogramAggregationBuilder factory = new DateHistogramAggregationBuilder(aggregationName);
        Object interval = otherOptions.get(Rounding.Interval.INTERVAL_FIELD);
        if (interval == null) {
            throw new ParsingException(null, "Missing required field [interval] for histogram aggregation [" + aggregationName + "]");
        } else if (interval instanceof Long) {
            factory.interval((Long) interval);
        } else if (interval instanceof DateHistogramInterval) {
            factory.dateHistogramInterval((DateHistogramInterval) interval);
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

    static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key) || "_time".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        return new InternalOrder.Aggregation(key, asc);
    }

    @Override
    protected long parseStringOffset(String offset) throws IOException {
        return DateHistogramAggregationBuilder.parseStringOffset(offset);
    }
}
