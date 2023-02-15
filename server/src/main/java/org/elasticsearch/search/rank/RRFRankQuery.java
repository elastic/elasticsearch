/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RRFRankQuery extends Query {

    private final List<Query> queries;
    private final Query compoundQuery;

    public RRFRankQuery(List<Query> queries) {
        this.queries = Collections.unmodifiableList(Objects.requireNonNull(queries));
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Query query : queries) {
            builder.add(query, BooleanClause.Occur.SHOULD);
        }
        this.compoundQuery = builder.build();
    }

    private RRFRankQuery(List<Query> queries, Query compoundQuery) {
        this.queries = Collections.unmodifiableList(Objects.requireNonNull(queries));
        this.compoundQuery = compoundQuery;
    }

    List<Query> getQueries() {
        return queries;
    }

    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return compoundQuery.createWeight(searcher, scoreMode, boost);
    }

    public Query rewrite(IndexReader reader) throws IOException {
        boolean rewritten = false;

        List<Query> rewrittenQueries = new ArrayList<>();
        for (Query query : queries) {
            Query rewrittenQuery = query.rewrite(reader);
            rewritten |= rewrittenQuery != query;
            rewrittenQueries.add(rewrittenQuery);
        }
        Query rewrittenCompoundQuery = compoundQuery.rewrite(reader);
        rewritten |= rewrittenCompoundQuery != compoundQuery;

        return rewritten ? new RRFRankQuery(rewrittenQueries, rewrittenCompoundQuery) : this;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        for (Query query : queries) {
            query.visit(visitor);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RRFRankQuery that = (RRFRankQuery) o;
        return Objects.equals(queries, that.queries) && Objects.equals(compoundQuery, that.compoundQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queries, compoundQuery);
    }

    @Override
    public String toString(String string) {
        return "RRFRankQuery{" +
            "queries=" + queries +
            ", compoundQuery=" + compoundQuery +
            '}';
    }
}
