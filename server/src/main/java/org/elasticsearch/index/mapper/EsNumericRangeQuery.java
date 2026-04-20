/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link Query} wrapper that delegates all Lucene scoring to an inner query and exposes
 * the field name and numeric range bounds. {@code LuceneSourceOperator} detects this type
 * at runtime and routes to a bulk-evaluation path that bypasses per-doc collection entirely.
 */
public final class EsNumericRangeQuery extends Query {
    private final Query delegate;
    private final String field;
    private final long lowerValue;
    private final long upperValue;

    public EsNumericRangeQuery(Query delegate, String field, long lowerValue, long upperValue) {
        this.delegate = Objects.requireNonNull(delegate);
        this.field = Objects.requireNonNull(field);
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
    }

    public String field() {
        return field;
    }

    public long lowerValue() {
        return lowerValue;
    }

    public long upperValue() {
        return upperValue;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new EsNumericRangeWeight(delegate.createWeight(searcher, scoreMode, boost));
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewritten = delegate.rewrite(searcher);
        if (rewritten == delegate) {
            return super.rewrite(searcher);
        }
        return new EsNumericRangeQuery(rewritten, field, lowerValue, upperValue);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        delegate.visit(visitor);
    }

    @Override
    public String toString(String fieldName) {
        return "EsNumericRangeQuery(" + delegate.toString(fieldName) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (sameClassAs(o) == false) return false;
        EsNumericRangeQuery that = (EsNumericRangeQuery) o;
        return lowerValue == that.lowerValue
            && upperValue == that.upperValue
            && Objects.equals(field, that.field)
            && Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return classHash() ^ Objects.hash(delegate, field, lowerValue, upperValue);
    }

    /**
     * Wraps an inner {@link Weight} so that {@link #getQuery()} returns the enclosing
     * {@link EsNumericRangeQuery} rather than the delegate query.
     */
    private class EsNumericRangeWeight extends Weight {
        private final Weight inner;

        EsNumericRangeWeight(Weight inner) {
            super(EsNumericRangeQuery.this);
            this.inner = inner;
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            return inner.scorerSupplier(context);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return inner.explain(context, doc);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return inner.isCacheable(ctx);
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            return inner.count(context);
        }
    }
}
