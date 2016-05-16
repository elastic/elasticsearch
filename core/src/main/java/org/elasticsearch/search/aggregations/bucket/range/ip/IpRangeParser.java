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
package org.elasticsearch.search.aggregations.bucket.range.ip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.BytesValuesSourceParser;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.ip.IpRangeAggregationBuilder.Range;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

/**
 * A parser for ip range aggregations.
 */
public class IpRangeParser extends BytesValuesSourceParser {

    private static final ParseField MASK_FIELD = new ParseField("mask");

    public IpRangeParser() {
        super(false, false);
    }

    @Override
    protected ValuesSourceAggregationBuilder<ValuesSource.Bytes, ?> createFactory(
            String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        IpRangeAggregationBuilder range = new IpRangeAggregationBuilder(aggregationName);
        @SuppressWarnings("unchecked")
        Iterable<Range> ranges = (Iterable<Range>) otherOptions.get(RangeAggregator.RANGES_FIELD);
        if (otherOptions.containsKey(RangeAggregator.RANGES_FIELD)) {
            for (Range r : ranges) {
                range.addRange(r);
            }
        }
        if (otherOptions.containsKey(RangeAggregator.KEYED_FIELD)) {
            range.keyed((Boolean) otherOptions.get(RangeAggregator.KEYED_FIELD));
        }
        return range;
    }

    private Range parseRange(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        String key = null;
        String from = null;
        String to = null;
        String mask = null;

        if (parser.currentToken() != Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[ranges] must contain objects, but hit a " + parser.currentToken());
        }
        while (parser.nextToken() != Token.END_OBJECT) {
            if (parser.currentToken() == Token.FIELD_NAME) {
                continue;
            }
            if (parseFieldMatcher.match(parser.currentName(), RangeAggregator.Range.KEY_FIELD)) {
                key = parser.text();
            } else if (parseFieldMatcher.match(parser.currentName(), RangeAggregator.Range.FROM_FIELD)) {
                from = parser.text();
            } else if (parseFieldMatcher.match(parser.currentName(), RangeAggregator.Range.TO_FIELD)) {
                to = parser.text();
            } else if (parseFieldMatcher.match(parser.currentName(), MASK_FIELD)) {
                mask = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected ip range parameter: [" + parser.currentName() + "]");
            }
        }
        if (mask != null) {
            if (key == null) {
                key = mask;
            }
            return new Range(key, mask);
        } else {
            return new Range(key, from, to);
        }
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName,
            Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher,
            Map<ParseField, Object> otherOptions) throws IOException {
        if (parseFieldMatcher.match(currentFieldName, RangeAggregator.RANGES_FIELD)) {
            if (parser.currentToken() != Token.START_ARRAY) {
                throw new ParsingException(parser.getTokenLocation(), "[ranges] must be passed as an array, but got a " + token);
            }
            List<Range> ranges = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                Range range = parseRange(parser, parseFieldMatcher);
                ranges.add(range);
            }
            otherOptions.put(RangeAggregator.RANGES_FIELD, ranges);
            return true;
        } else if (parseFieldMatcher.match(parser.currentName(), RangeAggregator.KEYED_FIELD)) {
            otherOptions.put(RangeAggregator.KEYED_FIELD, parser.booleanValue());
            return true;
        }
        return false;
    }

}
