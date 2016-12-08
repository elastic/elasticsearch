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

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.NumericValuesSourceParser;

import java.io.IOException;

public class RangeParser extends NumericValuesSourceParser {

    private final ObjectParser<RangeAggregationBuilder, QueryParseContext> parser;

    public RangeParser() {
        parser = new ObjectParser<>(RangeAggregationBuilder.NAME);
        addFields(parser, true, true, false);
        parser.declareBoolean(RangeAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);

        parser.declareObjectArray((agg, ranges) -> {
            for (Range range : ranges) agg.addRange(range);
        }, RangeParser::parseRange, RangeAggregator.RANGES_FIELD);
    }

    @Override
    public AggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        return parser.parse(context.parser(), new RangeAggregationBuilder(aggregationName), context);
    }

    private static Range parseRange(XContentParser parser, QueryParseContext context) throws IOException {
        return Range.fromXContent(parser, context.getParseFieldMatcher());
    }
}
