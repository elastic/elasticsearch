/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that matches documents whose numeric field value is present in a bitmap, for fields
 * indexed with BKD point values.
 * <p>
 * The bitmap values are streamed in sorted order and merged with the BKD tree leaves, giving
 * O(N_index_leaves + N_bitmap_values) total work across the entire tree.
 * Based on {@link org.apache.lucene.search.PointInSetQuery}.
 * <p>
 * The field's width lives entirely in the {@link BitmapValues}, so this handles {@code integer} and
 * {@code long} fields with one implementation: every comparison here is over the sortable-bytes
 * encoding that {@link org.apache.lucene.document.IntPoint} and
 * {@link org.apache.lucene.document.LongPoint} both use, read through
 * {@link ArrayUtil#getUnsignedComparator}.
 * <p>
 * Only non-negative values are supported; the caller must validate this before constructing the
 * query. The reason is an ordering mismatch: the point encoding flips the sign bit so that unsigned
 * byte-wise comparison reproduces signed numeric order, while the bitmap iterates in unsigned order.
 * The two agree only while every bitmap value is non-negative.
 */
public class BitmapBKDQuery extends Query implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BitmapBKDQuery.class);

    private final String field;
    private final BitmapValues values;
    private final byte[] bitmapLowerPoint;
    private final byte[] bitmapUpperPoint;

    public BitmapBKDQuery(String field, BitmapValues values) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
        if (values.isEmpty()) {
            this.bitmapLowerPoint = null;
            this.bitmapUpperPoint = null;
        } else {
            this.bitmapLowerPoint = new byte[values.bytesPerValue()];
            this.bitmapUpperPoint = new byte[values.bytesPerValue()];
            values.encodeFirst(bitmapLowerPoint);
            values.encodeLast(bitmapUpperPoint);
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                LeafReader reader = context.reader();
                PointValues pointValues = reader.getPointValues(field);
                if (pointValues == null) {
                    return null;
                }
                if (values.isEmpty()) {
                    return null;
                }

                // Skip segment if bitmap range doesn't overlap with segment's value range
                ArrayUtil.ByteArrayComparator cmp = ArrayUtil.getUnsignedComparator(values.bytesPerValue());
                byte[] segmentMin = pointValues.getMinPackedValue();
                byte[] segmentMax = pointValues.getMaxPackedValue();
                if (cmp.compare(bitmapLowerPoint, 0, segmentMax, 0) > 0 || cmp.compare(bitmapUpperPoint, 0, segmentMin, 0) < 0) {
                    return null;
                }

                return new ScorerSupplier() {
                    long cost = -1;

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), pointValues);
                        pointValues.intersect(new CollectingMergePointVisitor(result));
                        return new ConstantScoreScorer(score(), scoreMode, result.build().iterator());
                    }

                    @Override
                    public long cost() {
                        if (cost == -1) {
                            // estimateDocCount only ever calls compare(), so the plain merge visitor is
                            // enough here and no DocIdSetBuilder need be allocated.
                            cost = pointValues.estimateDocCount(new MergePointVisitor());
                        }
                        return cost;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }

    /**
     * The merge-sort between the BKD tree's sorted leaves and the bitmap's sorted encoded values,
     * derived from {@link org.apache.lucene.search.PointInSetQuery}'s {@code MergePointVisitor}. Both
     * sides are scanned at most once, giving O(N_index_leaves + N_bitmap_values) total work across the
     * entire tree.
     * <p>
     * This half decides <em>which cells match</em> and collects nothing, which is all
     * {@link PointValues#estimateDocCount} needs — it calls only {@link #compare}. Collecting the
     * matched documents costs a {@link DocIdSetBuilder}, so that lives in
     * {@link CollectingMergePointVisitor} and the cost estimate does not pay for it.
     */
    private class MergePointVisitor implements IntersectVisitor {
        private final BitmapValues.PeekableIterator iterator;
        private final ArrayUtil.ByteArrayComparator comparator;
        /** The bitmap value being merged, encoded. Owned here, so nothing else can recycle it. */
        private final byte[] queryPoint;
        private boolean hasQueryPoint;

        MergePointVisitor() {
            this.comparator = ArrayUtil.getUnsignedComparator(values.bytesPerValue());
            this.iterator = values.iterator();
            this.queryPoint = new byte[values.bytesPerValue()];
            takeQueryPoint();
        }

        /** Consumes the iterator's pending value into {@link #queryPoint}, or marks the bitmap exhausted. */
        private void takeQueryPoint() {
            hasQueryPoint = iterator.hasNext();
            if (hasQueryPoint) {
                iterator.encodePeek(queryPoint);
                iterator.next();
            }
        }

        /**
         * Skips bitmap values below {@code packedValue}. Only reached when {@link #queryPoint} is
         * already below it, and bitmap values are non-negative, so the decoded target is non-negative
         * too and the iterator's signed comparison matches the bitmap's unsigned iteration order.
         */
        private void skipTo(byte[] packedValue) {
            iterator.advanceTo(values.decode(packedValue, 0));
            takeQueryPoint();
        }

        @Override
        public void visit(int docID) {
            throw new UnsupportedOperationException("this visitor does not collect; use " + CollectingMergePointVisitor.class);
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
            throw new UnsupportedOperationException("this visitor does not collect; use " + CollectingMergePointVisitor.class);
        }

        protected boolean matches(byte[] packedValue) {
            while (hasQueryPoint) {
                int cmp = comparator.compare(queryPoint, 0, packedValue, 0);
                if (cmp == 0) {
                    return true;
                } else if (cmp < 0) {
                    // Query point is before index point, skip ahead in the bitmap
                    skipTo(packedValue);
                } else {
                    // Query point is after index point, no match for this doc
                    break;
                }
            }
            return false;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            while (hasQueryPoint) {
                int cmpMin = comparator.compare(queryPoint, 0, minPackedValue, 0);
                if (cmpMin < 0) {
                    // query point is before the start of this cell, advance
                    skipTo(minPackedValue);
                    continue;
                }
                int cmpMax = comparator.compare(queryPoint, 0, maxPackedValue, 0);
                if (cmpMax > 0) {
                    // query point is after the end of this cell
                    return Relation.CELL_OUTSIDE_QUERY;
                }

                if (cmpMin == 0 && cmpMax == 0) {
                    // cell min and max are exactly equal to our point,
                    // which can easily happen if many (> 512) docs share this one value
                    return Relation.CELL_INSIDE_QUERY;
                } else {
                    return Relation.CELL_CROSSES_QUERY;
                }
            }

            // We exhausted all points in the bitmap
            return Relation.CELL_OUTSIDE_QUERY;
        }
    }

    /**
     * Adds document collection to {@link MergePointVisitor}, for the pass that actually builds the
     * matching doc set. Kept separate so that {@link PointValues#estimateDocCount}, which only calls
     * {@link MergePointVisitor#compare}, does not have to allocate a {@link DocIdSetBuilder} it would
     * never write to.
     */
    private class CollectingMergePointVisitor extends MergePointVisitor {
        private final DocIdSetBuilder result;
        private DocIdSetBuilder.BulkAdder adder;

        CollectingMergePointVisitor(DocIdSetBuilder result) {
            this.result = result;
        }

        @Override
        public void grow(int count) {
            adder = result.grow(count);
        }

        @Override
        public void visit(int docID) {
            adder.add(docID);
        }

        @Override
        public void visit(DocIdSetIterator docIdSetIterator) throws IOException {
            adder.add(docIdSetIterator);
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
            if (matches(packedValue)) {
                visit(docID);
            }
        }

        @Override
        public void visit(DocIdSetIterator docIdSetIterator, byte[] packedValue) throws IOException {
            if (matches(packedValue)) {
                adder.add(docIdSetIterator);
            }
        }
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String defaultField) {
        return "BitmapBKDQuery(field=" + field + ", " + values + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        BitmapBKDQuery that = (BitmapBKDQuery) other;
        return field.equals(that.field) && values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, values);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (values.isEmpty()) {
            return new MatchNoDocsQuery("empty bitmap");
        }
        return super.rewrite(indexSearcher);
    }

    /**
     * The bitmap dominates this query's footprint and can reach tens of megabytes. Reporting it lets the
     * query cache account for a cached entry's true size instead of treating it as negligible.
     */
    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(field) + values.ramBytesUsed();
    }
}
