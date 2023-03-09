/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Holds internal information about the queries
 * used for ranking required for shard phases.
 */
public class RankContextInternal implements Writeable, Rewriteable<RankContextInternal> {

    protected final List<QueryBuilder> queryBuilders;

    public RankContextInternal(List<QueryBuilder> queryBuilders) {
        this.queryBuilders = Collections.unmodifiableList(Objects.requireNonNull(queryBuilders));
    }

    public RankContextInternal(StreamInput in) throws IOException {
        queryBuilders = in.readNamedWriteableList(QueryBuilder.class);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(queryBuilders);
    }

    @Override
    public RankContextInternal rewrite(QueryRewriteContext ctx) throws IOException {
        List<QueryBuilder> rewrittenQueryBuilders = new ArrayList<>();
        boolean rewritten = false;
        for (QueryBuilder queryBuilder : queryBuilders) {
            QueryBuilder rewrittenQueryBuilder = queryBuilder.rewrite(ctx);
            rewrittenQueryBuilders.add(rewrittenQueryBuilder);
            rewritten |= rewrittenQueryBuilder != queryBuilder;
        }
        return rewritten ? new RankContextInternal(rewrittenQueryBuilders) : this;
    }

    public List<QueryBuilder> queryBuilders() {
        return queryBuilders;
    }

    public List<Query> queries(SearchExecutionContext ctx) throws IOException {
        List<Query> queries = new ArrayList<>();
        for (QueryBuilder queryBuilder : queryBuilders) {
            queries.add(queryBuilder.toQuery(ctx));
        }
        return queries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankContextInternal that = (RankContextInternal) o;
        return Objects.equals(queryBuilders, that.queryBuilders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryBuilders);
    }

    @Override
    public String toString() {
        return "RankContextInternal{" + "queryBuilders=" + queryBuilders + '}';
    }
}
