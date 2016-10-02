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
package org.elasticsearch.search.aggregations.bucket.range.date;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class DateRangeParser extends RangeParser {

    public DateRangeParser() {
        super(true, true, true);
    }

    @Override
    protected DateRangeAggregationBuilder createFactory(String aggregationName, ValuesSourceType valuesSourceType,
                                                        ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        DateRangeAggregationBuilder factory = new DateRangeAggregationBuilder(aggregationName);
        @SuppressWarnings("unchecked")
        List<Range> ranges = (List<Range>) otherOptions.get(RangeAggregator.RANGES_FIELD);
        for (Range range : ranges) {
            factory.addRange(range);
        }
        Boolean keyed = (Boolean) otherOptions.get(RangeAggregator.KEYED_FIELD);
        if (keyed != null) {
            factory.keyed(keyed);
        }
        return factory;
    }
}
