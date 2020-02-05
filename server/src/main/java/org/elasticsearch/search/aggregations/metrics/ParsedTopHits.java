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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;

public class ParsedTopHits extends ParsedAggregation implements TopHits {

    private SearchHits searchHits;

    @Override
    public String getType() {
        return TopHitsAggregationBuilder.NAME;
    }

    @Override
    public SearchHits getHits() {
        return searchHits;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return searchHits.toXContent(builder, params);
    }

    private static final ObjectParser<ParsedTopHits, Void> PARSER =
            new ObjectParser<>(ParsedTopHits.class.getSimpleName(), true, ParsedTopHits::new);
    static {
        declareAggregationFields(PARSER);
        PARSER.declareObject((topHit, searchHits) -> topHit.searchHits = searchHits, (parser, context) -> SearchHits.fromXContent(parser),
                new ParseField(SearchHits.Fields.HITS));
    }

    public static ParsedTopHits fromXContent(XContentParser parser, String name) throws IOException {
        ParsedTopHits aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}
