/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Matches the docs whose {@code _tsid} is in a {@link TsidPrefixRange}. Time-series segments are sorted by {@code _tsid},
 * so a segment's matches are one doc range, resolved from the segment's prefix partitions when its scorer is requested.
 * <p>
 * Time-series partitioning finds the range with {@link #find} and slices only its prefix partitions; the query still
 * filters, so any other partitioning reads the right docs too.
 */
public final class TsidPrefixRangeQuery extends Query {
    private final TsidPrefixRange range;

    public TsidPrefixRangeQuery(TsidPrefixRange range) {
        this.range = range;
    }

    public TsidPrefixRange range() {
        return range;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        final Map<Object, PrimarySortRange.DocRange> cachedRanges = ConcurrentCollections.newConcurrentMap();
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final Object key = context.id();
                var docs = cachedRanges.get(key);
                if (docs == null) {
                    docs = range.docRange(context.reader());
                    cachedRanges.put(key, docs);
                }
                if (docs.length() == 0) {
                    return null;
                }
                final DocIdSetIterator iterator = DocIdSetIterator.range(docs.minDoc(), docs.maxDoc());
                return new DefaultScorerSupplier(new ConstantScoreScorer(score(), scoreMode, iterator));
            }

            @Override
            public boolean isCacheable(LeafReaderContext context) {
                return false;
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(TimeSeriesIdFieldMapper.NAME)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String field) {
        return TimeSeriesIdFieldMapper.NAME + ":prefix[" + range + "]";
    }

    @Override
    public boolean equals(Object obj) {
        return sameClassAs(obj) && obj instanceof TsidPrefixRangeQuery that && range.equals(that.range);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + range.hashCode();
    }

    /**
     * Find the range of a {@link TsidPrefixRangeQuery} in the required clauses of {@code query}.
     */
    @Nullable
    static TsidPrefixRange find(Query query) {
        if (query instanceof TsidPrefixRangeQuery tsidRange) {
            return tsidRange.range;
        }
        if (query instanceof ConstantScoreQuery constantScore) {
            return find(constantScore.getQuery());
        }
        if (query instanceof BoostQuery boost) {
            return find(boost.getQuery());
        }
        if (query instanceof BooleanQuery bool) {
            for (BooleanClause clause : bool) {
                if (clause.occur() == BooleanClause.Occur.MUST || clause.occur() == BooleanClause.Occur.FILTER) {
                    TsidPrefixRange found = find(clause.query());
                    if (found != null) {
                        return found;
                    }
                }
            }
        }
        return null;
    }
}
