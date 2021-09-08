/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

import static java.util.Arrays.compareUnsigned;

/**
 * Query merging two point in range queries.
 */
public class MergedPointRangeQuery extends Query {
    /**
     * Merge two {@linkplain PointRangeQuery}s into a {@linkplain MergedPointRangeQuery}
     * that matches points that match both filters.
     */
    public static Query merge(PointRangeQuery lhs, PointRangeQuery rhs) {
        if (lhs.equals(rhs)) {
            // Lucky case! The queries were the same so their UNION is just the query itself.
            return lhs;
        }
        if (lhs.getField() != rhs.getField() || lhs.getNumDims() != rhs.getNumDims() || lhs.getBytesPerDim() != rhs.getBytesPerDim()) {
            return null;
        }
        return new MergedPointRangeQuery(lhs, rhs);
    }

    private final String field;
    private final Query delegateForMultiValuedSegments;
    private final Query delegateForSingleValuedSegments;

    private MergedPointRangeQuery(PointRangeQuery lhs, PointRangeQuery rhs) {
        field = lhs.getField();
        delegateForMultiValuedSegments = new BooleanQuery.Builder().add(lhs, Occur.MUST).add(rhs, Occur.MUST).build();
        int numDims = lhs.getNumDims();
        int bytesPerDim = lhs.getBytesPerDim();
        this.delegateForSingleValuedSegments = pickDelegateForSingleValuedSegments(
            mergeBound(lhs.getLowerPoint(), rhs.getLowerPoint(), numDims, bytesPerDim, true),
            mergeBound(lhs.getUpperPoint(), rhs.getUpperPoint(), numDims, bytesPerDim, false),
            numDims,
            bytesPerDim
        );
    }

    private Query pickDelegateForSingleValuedSegments(byte[] lower, byte[] upper, int numDims, int bytesPerDim) {
        // If we ended up with disjoint ranges in any dimension then on single valued segments we can't match any docs.
        for (int dim = 0; dim < numDims; dim++) {
            int offset = dim * bytesPerDim;
            if (compareUnsigned(lower, offset, offset + bytesPerDim, upper, offset, offset + bytesPerDim) > 0) {
                return new MatchNoDocsQuery("disjoint ranges");
            }
        }
        // Otherwise on single valued segments we can only match docs the match the UNION of the two ranges.
        return new PointRangeQuery(field, lower, upper, numDims) {
            @Override
            protected String toString(int dimension, byte[] value) {
                // Stolen from Lucene's Binary range query. It'd be best to delegate, but the method isn't visible.
                StringBuilder sb = new StringBuilder();
                sb.append("(");
                for (int i = 0; i < value.length; i++) {
                    if (i > 0) {
                        sb.append(' ');
                    }
                    sb.append(Integer.toHexString(value[i] & 0xFF));
                }
                sb.append(')');
                return sb.toString();
            }
        };
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            Weight multiValuedSegmentWeight;
            Weight singleValuedSegmentWeight;

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                ScorerSupplier scorerSupplier = scorerSupplier(context);
                if (scorerSupplier == null) {
                    return null;
                }
                return scorerSupplier.get(Long.MAX_VALUE);
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                /*
                 * If we're sure docs only have a single value for the field
                 * we can pick the most compact bounds. If there are multiple values
                 * for the field we have to run the boolean query.
                 */
                PointValues points = context.reader().getPointValues(field);
                if (points == null) {
                    return null;
                }
                if (points.size() == points.getDocCount()) {
                    // Each doc that has points has exactly one point.
                    return singleValuedSegmentWeight().scorerSupplier(context);
                }
                return multiValuedSegmentWeight().scorerSupplier(context);
            }

            @Override
            public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
                PointValues points = context.reader().getPointValues(field);
                if (points == null) {
                    return null;
                }
                if (points.size() == points.getDocCount()) {
                    // Each doc that has points has exactly one point.
                    return singleValuedSegmentWeight().bulkScorer(context);
                }
                return multiValuedSegmentWeight().bulkScorer(context);
            }

            private Weight singleValuedSegmentWeight() throws IOException {
                if (singleValuedSegmentWeight == null) {
                    singleValuedSegmentWeight = delegateForSingleValuedSegments.createWeight(searcher, scoreMode, boost);
                }
                return singleValuedSegmentWeight;
            }

            private Weight multiValuedSegmentWeight() throws IOException {
                if (multiValuedSegmentWeight == null) {
                    multiValuedSegmentWeight = delegateForMultiValuedSegments.createWeight(searcher, scoreMode, boost);
                }
                return multiValuedSegmentWeight;
            }
        };
    }

    /**
     * The query used when we have single valued segments.
     */
    Query delegateForSingleValuedSegments() {
        return delegateForSingleValuedSegments;
    }

    @Override
    public String toString(String field) {
        return "MergedPointRange[" + delegateForMultiValuedSegments.toString(field) + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        MergedPointRangeQuery other = (MergedPointRangeQuery) obj;
        return delegateForMultiValuedSegments.equals(other.delegateForMultiValuedSegments)
            && delegateForSingleValuedSegments.equals(other.delegateForSingleValuedSegments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), delegateForMultiValuedSegments, delegateForSingleValuedSegments);
    }

    private static byte[] mergeBound(byte[] lhs, byte[] rhs, int numDims, int bytesPerDim, boolean lower) {
        byte[] merged = new byte[lhs.length];
        for (int dim = 0; dim < numDims; dim++) {
            int offset = dim * bytesPerDim;
            boolean cmp = compareUnsigned(lhs, offset, offset + bytesPerDim, rhs, offset, offset + bytesPerDim) <= 0;
            byte[] from = (cmp ^ lower) ? lhs : rhs;
            System.arraycopy(from, offset, merged, offset, bytesPerDim);
        }
        return merged;
    }
}
