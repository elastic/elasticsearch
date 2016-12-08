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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.ip.IpRangeAggregationBuilder.Range;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.BytesValuesSourceParser;

import java.io.IOException;

/**
 * A parser for ip range aggregations.
 */
public class IpRangeParser extends BytesValuesSourceParser {

    private static final ParseField MASK_FIELD = new ParseField("mask");

    private final ObjectParser<IpRangeAggregationBuilder, QueryParseContext> parser;

    public IpRangeParser() {
        parser = new ObjectParser<>(IpRangeAggregationBuilder.NAME);
        addFields(parser, false, false);

        parser.declareBoolean(IpRangeAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);

        parser.declareObjectArray((agg, ranges) -> {
            for (Range range : ranges) agg.addRange(range);
        }, IpRangeParser::parseRange, RangeAggregator.RANGES_FIELD);
    }

    @Override
    public AggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        return parser.parse(context.parser(), new IpRangeAggregationBuilder(aggregationName), context);
    }

    private static Range parseRange(XContentParser parser, QueryParseContext context) throws IOException {
        final ParseFieldMatcher parseFieldMatcher = context.getParseFieldMatcher();
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
                from = parser.textOrNull();
            } else if (parseFieldMatcher.match(parser.currentName(), RangeAggregator.Range.TO_FIELD)) {
                to = parser.textOrNull();
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

}
