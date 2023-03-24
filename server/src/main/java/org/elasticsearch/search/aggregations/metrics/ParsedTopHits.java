/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

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
        return ChunkedToXContent.wrapAsToXContent(searchHits).toXContent(builder, params);
    }

    private static final ObjectParser<ParsedTopHits, Void> PARSER = new ObjectParser<>(
        ParsedTopHits.class.getSimpleName(),
        true,
        ParsedTopHits::new
    );
    static {
        declareAggregationFields(PARSER);
        PARSER.declareObject(
            (topHit, searchHits) -> topHit.searchHits = searchHits,
            (parser, context) -> SearchHits.fromXContent(parser),
            new ParseField(SearchHits.Fields.HITS)
        );
    }

    public static ParsedTopHits fromXContent(XContentParser parser, String name) throws IOException {
        ParsedTopHits aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}
