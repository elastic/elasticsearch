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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.query.SearchQueryWrapper;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * {@code SearchQueryBuilder} is a wrapper class for containing all
 * the information required to perform a single search query
 * as part of a series of multiple queries for features like ranking.
 * It's expected to typically be used as part of a {@link java.util.List}.
 */
public class SearchQueryWrapperBuilder implements ToXContent, Writeable, Rewriteable<SearchQueryWrapperBuilder> {

    public static SearchQueryWrapperBuilder parseXContent(XContentParser parser, SearchUsage searchUsage) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;

        QueryBuilder queryBuilder = null;

        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SearchSourceBuilder.QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryBuilder = AbstractQueryBuilder.parseTopLevelQuery(parser, searchUsage::trackQueryUsage);
                }
            }
        }

        return new SearchQueryWrapperBuilder(queryBuilder);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SearchSourceBuilder.QUERY_FIELD.getPreferredName());
        queryBuilder.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    private final QueryBuilder queryBuilder;

    public SearchQueryWrapperBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public SearchQueryWrapperBuilder(StreamInput in) throws IOException {
        this.queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalNamedWriteable(queryBuilder);
    }

    @Override
    public SearchQueryWrapperBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        QueryBuilder rewrittenQueryBuilder = queryBuilder.rewrite(ctx);
        return rewrittenQueryBuilder == queryBuilder ? this : new SearchQueryWrapperBuilder(rewrittenQueryBuilder);
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public SearchQueryWrapper toSearchQuery(SearchExecutionContext context) throws IOException {
        return new SearchQueryWrapper(queryBuilder.toQuery(context));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchQueryWrapperBuilder that = (SearchQueryWrapperBuilder) o;
        return Objects.equals(queryBuilder, that.queryBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryBuilder);
    }
}
