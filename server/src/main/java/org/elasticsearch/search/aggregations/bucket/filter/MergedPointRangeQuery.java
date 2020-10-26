package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
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
        if (lhs.getField() != rhs.getField() || lhs.getNumDims() != rhs.getNumDims() || lhs.getBytesPerDim() != rhs.getBytesPerDim()) {
            return null;
        }
        Integer lowerCmp = compareAllDims(lhs.getLowerPoint(), rhs.getLowerPoint(), lhs.getNumDims(), lhs.getBytesPerDim());
        if (lowerCmp == null) {
            // Not all dimensions compared the same way.
            return null;
        }
        Integer upperCmp = compareAllDims(lhs.getUpperPoint(), rhs.getUpperPoint(), lhs.getNumDims(), lhs.getBytesPerDim());
        if (upperCmp == null) {
            // Not all dimensions compared the same way.
            return null;   // NOCOMMIT it shouldn't matter - we can just merge them anyway
        }
        if (lowerCmp == 1 && upperCmp == 1) {
            // The points are the same.
            return lhs;
        }
        byte[] lower = lowerCmp < 0 ? rhs.getLowerPoint() : lhs.getLowerPoint();
        byte[] upper = upperCmp < 0 ? lhs.getUpperPoint() : rhs.getUpperPoint();
        PointRangeQuery mostCompact = new PointRangeQuery(lhs.getField(), lower, upper, lhs.getNumDims()) {
            @Override
            protected String toString(int dimension, byte[] value) {
                // Stolen from Lucene's Binary range query. It'd be best to delegate, but the method isn't visible.
                StringBuilder sb = new StringBuilder();
                sb.append("binary(");
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
        return new MergedPointRangeQuery(lhs, rhs, mostCompact);
    }

    private final String field;
    private final BooleanQuery delegateForMultiValuedSegments;
    private final PointRangeQuery mostCompactQuery;

    private MergedPointRangeQuery(PointRangeQuery lhs, PointRangeQuery rhs, PointRangeQuery mostCompactQuery) {
        field = lhs.getField();
        delegateForMultiValuedSegments = new BooleanQuery.Builder().add(lhs, Occur.MUST).add(rhs, Occur.MUST).build();
        this.mostCompactQuery = mostCompactQuery;
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
                        mostCompactWeight = mostCompactQuery.createWeight(searcher, scoreMode, boost);
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

    @Override
    public String toString(String field) {
        return delegateForMultiValuedSegments.toString(field);
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

    /**
     * Figure out if lhs point is closer to the origin in all dimensions than
     * the rhs point or if it is further. Return null if it is closer
     * in some dimensions and further in others.
     */
    private static Integer compareAllDims(byte[] lhs, byte[] rhs, int numDims, int bytesPerDim) {
        int runningCmp = 0;
        for (int dim = 0; dim < numDims; dim++) {
            int cmp = cmpDim(lhs, rhs, dim, bytesPerDim);
            if (runningCmp == 0) {
                // Previous dimensions were all equal
                runningCmp = cmp;
                continue;
            }
            if (cmp == 0) {
                // This dimension has the same value.
                continue;
            }
            // TODO can't we merge these here instead of give up?
            if ((runningCmp ^ cmp) < 0) {
                // Signs differ so this dimension doesn't compare the same way as the previous ones so we can't merge.
                return null;
            }
        }
        return runningCmp;
    }

    private static int cmpDim(byte[] lhs, byte[] rhs, int dim, int bytesPerDim) {
        int offset = dim * bytesPerDim;
        return compareUnsigned(lhs, offset, offset + bytesPerDim, rhs, offset, offset + bytesPerDim);
    }
}
