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
package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.NumericValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RangeParser extends NumericValuesSourceParser {

    public RangeParser() {
        this(true, true, false);
    }

    /**
     * Used by subclasses that parse slightly different kinds of ranges.
     */
    protected RangeParser(boolean scriptable, boolean formattable, boolean timezoneAware) {
        super(scriptable, formattable, timezoneAware);
    }

    @Override
    protected AbstractRangeBuilder<?, ?> createFactory(String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        RangeAggregationBuilder factory = new RangeAggregationBuilder(aggregationName);
        @SuppressWarnings("unchecked")
        List<? extends Range> ranges = (List<? extends Range>) otherOptions.get(RangeAggregator.RANGES_FIELD);
        for (Range range : ranges) {
            factory.addRange(range);
        }
        Boolean keyed = (Boolean) otherOptions.get(RangeAggregator.KEYED_FIELD);
        if (keyed != null) {
            factory.keyed(keyed);
        }
        return factory;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (token == XContentParser.Token.START_ARRAY) {
            if (parseFieldMatcher.match(currentFieldName, RangeAggregator.RANGES_FIELD)) {
                List<Range> ranges = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    Range range = parseRange(parser, parseFieldMatcher);
                    ranges.add(range);
                }
                otherOptions.put(RangeAggregator.RANGES_FIELD, ranges);
                return true;
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            if (parseFieldMatcher.match(currentFieldName, RangeAggregator.KEYED_FIELD)) {
                boolean keyed = parser.booleanValue();
                otherOptions.put(RangeAggregator.KEYED_FIELD, keyed);
                return true;
            }
        }
        return false;
    }

    protected Range parseRange(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        return Range.fromXContent(parser, parseFieldMatcher);
    }
}
