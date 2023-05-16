/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.builder;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * {@code SearchQueryBuilder} is a wrapper class for containing all
 * the information required to perform a single search query
 * as part of a series of multiple queries for something like ranking.
 * It's expected to typically be used as part of a {@link java.util.List}.
 */
public class SearchQueryBuilder implements ToXContent, Writeable, Rewriteable<SearchQueryBuilder> {

    public static SearchQueryBuilder parseXContent(XContentParser parser, SearchUsage searchUsage) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;

        QueryBuilder queryBuilder = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SearchSourceBuilder.QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryBuilder = AbstractQueryBuilder.parseTopLevelQuery(parser, searchUsage::trackQueryUsage);
                }
            }
        }

        return new SearchQueryBuilder(queryBuilder);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        queryBuilder.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    private final QueryBuilder queryBuilder;

    public SearchQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public SearchQueryBuilder(StreamInput in) throws IOException {
        this.queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalNamedWriteable(queryBuilder);
    }

    @Override
    public SearchQueryBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        QueryBuilder rewrittenQueryBuilder = queryBuilder.rewrite(ctx);
        return rewrittenQueryBuilder == queryBuilder ? this : new SearchQueryBuilder(rewrittenQueryBuilder);
    }
}
