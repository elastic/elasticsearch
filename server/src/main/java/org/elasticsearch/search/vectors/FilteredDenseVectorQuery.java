/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * Wraps a {@link DenseVectorQuery} with a filter, restricting scoring to documents that also match the
 * filter. Pulling the filter out of {@link DenseVectorQuery} keeps {@code Floats} and {@code Bytes} focused
 * purely on vector scoring; this class owns all filter-specific behavior (rewriting, weight construction).
 *
 * <p>The filter is not applied as a plain outer conjunction: {@link DenseVectorQuery.DenseVectorWeight}
 * pushes the filter's iterator into the per-leaf {@code VectorScorer.bulk(...)} call, which lets
 * similarity computation and filtering run together in SIMD-friendly batches.
 */
public final class FilteredDenseVectorQuery extends Query {

    private final DenseVectorQuery innerQuery;
    private final Query filter;

    public FilteredDenseVectorQuery(DenseVectorQuery innerQuery, Query filter) {
        this.innerQuery = Objects.requireNonNull(innerQuery);
        this.filter = Objects.requireNonNull(filter);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query rewrittenFilter = indexSearcher.rewrite(filter);
        if (rewrittenFilter.getClass() == MatchNoDocsQuery.class) {
            return rewrittenFilter;
        }
        if (rewrittenFilter.getClass() == MatchAllDocsQuery.class) {
            return innerQuery;
        }
        if (rewrittenFilter == filter) {
            return this;
        }
        return new FilteredDenseVectorQuery(innerQuery, rewrittenFilter);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        BooleanQuery booleanFilter = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER).build();
        Query rewrittenFilter = searcher.rewrite(booleanFilter);
        Weight filterWeight = rewrittenFilter.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
        return innerQuery.denseVectorWeight(boost, filterWeight);
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        // Delegate straight to the inner query so visitors see the same leaf (and the same
        // DenseVectorQuery.Floats/Bytes instanceof checks succeed) whether or not a filter is
        // present, matching pre-extraction behavior where the filter was never itself visited.
        innerQuery.visit(queryVisitor);
    }

    @Override
    public String toString(String field) {
        return "FilteredDenseVectorQuery(" + innerQuery.toString(field) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilteredDenseVectorQuery other = (FilteredDenseVectorQuery) o;
        return Objects.equals(innerQuery, other.innerQuery) && Objects.equals(filter, other.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(innerQuery, filter);
    }
}
