/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * A query wrapper that ensures the inner query is always eligible for query caching.
 * <p>
 * Lucene uses heuristics to determine whether a query should be cached ({@link UsageTrackingQueryCachingPolicy}),
 * and some queries may be skipped if they are considered too cheap or otherwise uninteresting for caching.
 * Wrapping a query in {@link CachingEnableFilterQuery} guarantees that it will be treated as
 * cacheable by the query cache.
 * </p>
 *
 * <p>
 * This wrapper does not alter the scoring or filtering semantics of the inner query.
 * It only changes how the query cache perceives it, by making it always considered
 * interesting enough to cache.
 * </p>
 *
 * <p>
 * This is particularly useful in cases where the filter is always entirely consumed,
 * such as filtered vector search, where the filter is transformed into a bitset eagerly.
 * In these scenarios, caching the filter query can significantly improve performance and avoid recomputation.
 * </p>
 *
 * <h2>Example usage:</h2>
 * <pre>{@code
 * Query inner = new TermQuery(new Term("field", "value"));
 * Query cacheable = new CacheWrapperQuery(inner);
 * TopDocs results = searcher.search(cacheable, 10);
 * }</pre>
 */
public class CachingEnableFilterQuery extends Query {
    private final Query in;

    public CachingEnableFilterQuery(Query in) {
        this.in = in;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        var inWeight = in.createWeight(searcher, scoreMode, boost);
        return new FilterWeight(this, inWeight) {
        };
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        var rewrite = in.rewrite(indexSearcher);
        if (rewrite instanceof MatchNoDocsQuery || rewrite instanceof MatchAllDocsQuery || rewrite instanceof BooleanQuery) {
            // If the query matches all documents, no documents, or rewrites into a compound query
            // that is already eligible for caching, we can safely remove this wrapper.
            return rewrite;
        }
        return rewrite != in ? new CachingEnableFilterQuery(rewrite) : this;
    }

    @Override
    public String toString(String field) {
        return in.toString(field);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        in.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return in.equals(((CachingEnableFilterQuery) obj).in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), in.hashCode());
    }
}
