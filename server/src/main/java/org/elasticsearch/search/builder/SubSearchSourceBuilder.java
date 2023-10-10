/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.builder;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * {@code SearchQueryBuilder} is a wrapper class for containing all
 * the information required to perform a single search query
 * as part of a series of multiple queries for features like ranking.
 * It's expected to typically be used as part of a {@link java.util.List}.
 */
public class SubSearchSourceBuilder implements ToXContent, Writeable, Rewriteable<SubSearchSourceBuilder> {

    private static final ConstructingObjectParser<SubSearchSourceBuilder, SearchUsage> PARSER = new ConstructingObjectParser<>(
        "sub_search_source_builder",
        args -> new SubSearchSourceBuilder((QueryBuilder) args[0])
    );

    static {
        PARSER.declareObject(
            constructorArg(),
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p, c::trackQueryUsage),
            SearchSourceBuilder.QUERY_FIELD
        );
    }

    public static SubSearchSourceBuilder fromXContent(XContentParser parser, SearchUsage searchUsage) throws IOException {
        return PARSER.parse(parser, searchUsage);
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

    public SubSearchSourceBuilder(QueryBuilder queryBuilder) {
        Objects.requireNonNull(queryBuilder);
        this.queryBuilder = queryBuilder;
    }

    public SubSearchSourceBuilder(StreamInput in) throws IOException {
        this.queryBuilder = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(queryBuilder);
    }

    @Override
    public SubSearchSourceBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        QueryBuilder rewrittenQueryBuilder = queryBuilder.rewrite(ctx);
        return rewrittenQueryBuilder == queryBuilder ? this : new SubSearchSourceBuilder(rewrittenQueryBuilder);
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public Query toSearchQuery(SearchExecutionContext context) throws IOException {
        return queryBuilder.toQuery(context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubSearchSourceBuilder that = (SubSearchSourceBuilder) o;
        return Objects.equals(queryBuilder, that.queryBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryBuilder);
    }
}
