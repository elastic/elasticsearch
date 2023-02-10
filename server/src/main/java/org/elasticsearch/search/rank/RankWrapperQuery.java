/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

public class RankWrapperQuery extends Query {

    public static class RankWrapperWeight extends Weight {

        public final Consumer<Scorer> consumer;
        private final Weight weight;

        public RankWrapperWeight(Consumer<Scorer> consumer, Query wrappedQuery, Weight weight) {
            super(wrappedQuery);
            this.consumer = consumer;
            this.weight = weight;
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            return weight.matches(context, doc);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return weight.explain(context, doc);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer scorer = weight.scorer(context);
            consumer.accept(scorer);
            return scorer;
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            return weight.scorerSupplier(context);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
            return weight.bulkScorer(context);
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            return weight.count(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return weight.isCacheable(ctx);
        }
    }

    public final Consumer<Scorer> consumer;
    public Query wrappedQuery;

    public RankWrapperQuery(Consumer<Scorer> consumer, Query wrappedQuery) {
        this.consumer = Objects.requireNonNull(consumer);
        this.wrappedQuery = Objects.requireNonNull(wrappedQuery);
    }

    public Consumer<Scorer> getConsumer() {
        return consumer;
    }

    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new RankWrapperWeight(consumer, wrappedQuery, wrappedQuery.createWeight(searcher, scoreMode, boost));
    }

    public Query rewrite(IndexReader reader) throws IOException {
        Query rewrittenQuery = wrappedQuery.rewrite(reader);

        if (rewrittenQuery != wrappedQuery) {
            return new RankWrapperQuery(consumer, wrappedQuery);
        }

        return this;
    }

    @Override
    public String toString(String field) {
        return wrappedQuery.toString();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        wrappedQuery.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
        return wrappedQuery.getClass() == obj.getClass() && wrappedQuery.equals(obj);
    }

    @Override
    public int hashCode() {
        return wrappedQuery.hashCode();
    }
}
