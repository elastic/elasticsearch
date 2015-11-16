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
package org.elasticsearch.search.aggregations.bucket.range.ipv4;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class IpRangeParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalIPv4Range.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser<ValuesSource.Numeric> vsParser = ValuesSourceParser.numeric(aggregationName, InternalIPv4Range.TYPE, context)
                .targetValueType(ValueType.IP)
                .formattable(false)
                .build();

        List<RangeAggregator.Range> ranges = null;
        boolean keyed = false;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("ranges".equals(currentFieldName)) {
                    ranges = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double from = Double.NEGATIVE_INFINITY;
                        String fromAsStr = null;
                        double to = Double.POSITIVE_INFINITY;
                        String toAsStr = null;
                        String key = null;
                        String mask = null;
                        String toOrFromOrMaskOrKey = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                toOrFromOrMaskOrKey = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                if ("from".equals(toOrFromOrMaskOrKey)) {
                                    from = parser.doubleValue();
                                } else if ("to".equals(toOrFromOrMaskOrKey)) {
                                    to = parser.doubleValue();
                                }
                            } else if (token == XContentParser.Token.VALUE_STRING) {
                                if ("from".equals(toOrFromOrMaskOrKey)) {
                                    fromAsStr = parser.text();
                                } else if ("to".equals(toOrFromOrMaskOrKey)) {
                                    toAsStr = parser.text();
                                } else if ("key".equals(toOrFromOrMaskOrKey)) {
                                    key = parser.text();
                                } else if ("mask".equals(toOrFromOrMaskOrKey)) {
                                    mask = parser.text();
                                }
                            }
                        }
                        RangeAggregator.Range range = new RangeAggregator.Range(key, from, fromAsStr, to, toAsStr);
                        if (mask != null) {
                            parseMaskRange(mask, range, aggregationName, context);
                        }
                        ranges.add(range);
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].",
                        parser.getTokenLocation());
            }
        }

        if (ranges == null) {
            throw new SearchParseException(context, "Missing [ranges] in ranges aggregator [" + aggregationName + "]",
                    parser.getTokenLocation());
        }

        return new RangeAggregator.Factory(aggregationName, vsParser.config(), InternalIPv4Range.FACTORY, ranges, keyed);
    }

    private static void parseMaskRange(String cidr, RangeAggregator.Range range, String aggregationName, SearchContext ctx) {
        long[] fromTo = IpFieldMapper.cidrMaskToMinMax(cidr);
        if (fromTo == null) {
            throw new SearchParseException(ctx, "invalid CIDR mask [" + cidr + "] in aggregation [" + aggregationName + "]",
                    null);
        }
        range.from = fromTo[0] < 0 ? Double.NEGATIVE_INFINITY : fromTo[0];
        range.to = fromTo[1] < 0 ? Double.POSITIVE_INFINITY : fromTo[1];
        if (range.key == null) {
            range.key = cidr;
        }
    }

}
