/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Set;

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
        Weight delegateForMultiValuedSegmentsWeight = delegateForMultiValuedSegments.createWeight(searcher, scoreMode, boost);
        return new Weight(this) {
            Weight mostCompactWeight;

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return delegateForMultiValuedSegmentsWeight.isCacheable(ctx);
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
                    if (mostCompactWeight == null) {
                        mostCompactWeight = delegateForSingleValuedSegments.createWeight(searcher, scoreMode, boost);
                    }
                    return mostCompactWeight.scorerSupplier(context);
                }
                return delegateForMultiValuedSegmentsWeight.scorerSupplier(context);
            }

            @Override
            public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
                return super.bulkScorer(context);
            }

            @Override
            @Deprecated
            public void extractTerms(Set<Term> terms) {
                delegateForMultiValuedSegmentsWeight.extractTerms(terms);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return delegateForMultiValuedSegmentsWeight.explain(context, doc);
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
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MergedPointRangeQuery other = (MergedPointRangeQuery) obj;
        return delegateForMultiValuedSegments.equals(other.delegateForMultiValuedSegments);
    }

    @Override
    public int hashCode() {
        return classHash() * 31 + delegateForMultiValuedSegments.hashCode();
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
