/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that matches documents whose integer field value is present in a {@link RoaringBitmap},
 * for fields that use the BKD (point values) index path.
 * <p>
 * The bitmap values are streamed in sorted order and merged with the BKD tree leaves, giving
 * O(N_index_leaves + N_bitmap_values) total work across the entire tree.
 * Based on {@link org.apache.lucene.search.PointInSetQuery}.
 * <p>
 * Only non-negative integer values are supported; the caller must validate this before
 * constructing the query.
 */
public class IntBitmapIndexBKDQuery extends Query {
    private final String field;
    private final RoaringBitmap bitmap;
    private final byte[] bitmapLowerPoint;
    private final byte[] bitmapUpperPoint;

    public IntBitmapIndexBKDQuery(String field, RoaringBitmap bitmap) {
        this.field = Objects.requireNonNull(field);
        this.bitmap = Objects.requireNonNull(bitmap);
        if (bitmap.isEmpty()) {
            this.bitmapLowerPoint = null;
            this.bitmapUpperPoint = null;
        } else {
            this.bitmapLowerPoint = new byte[Integer.BYTES];
            this.bitmapUpperPoint = new byte[Integer.BYTES];
            IntPoint.encodeDimension(bitmap.first(), bitmapLowerPoint, 0);
            IntPoint.encodeDimension(bitmap.last(), bitmapUpperPoint, 0);
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
                if (bitmap.isEmpty()) {
                    return null;
                }

                // Skip segment if bitmap range doesn't overlap with segment's value range
                ArrayUtil.ByteArrayComparator cmp = ArrayUtil.getUnsignedComparator(Integer.BYTES);
                byte[] segmentMin = pointValues.getMinPackedValue();
                byte[] segmentMax = pointValues.getMaxPackedValue();
                if (cmp.compare(bitmapLowerPoint, 0, segmentMax, 0) > 0 || cmp.compare(bitmapUpperPoint, 0, segmentMin, 0) < 0) {
                    return null;
                }

                return new ScorerSupplier() {
                    long cost = -1;

                    @Override
                    public org.apache.lucene.search.Scorer get(long leadCost) throws IOException {
                        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), pointValues);
                        pointValues.intersect(new MergePointVisitor(result));
                        return new ConstantScoreScorer(score(), scoreMode, result.build().iterator());
                    }

                    @Override
                    public long cost() {
                        if (cost == -1) {
                            try {
                                DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), pointValues);
                                cost = pointValues.estimateDocCount(new MergePointVisitor(result));
                            } catch (IOException e) {
                                throw new java.io.UncheckedIOException(e);
                            }
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
     * Streams sorted integer values from a {@link RoaringBitmap} as IntPoint-encoded byte arrays.
     */
    private static class EncodedPointStream {
        private final PeekableIntIterator iterator;
        private final BytesRef encoded = new BytesRef(new byte[Integer.BYTES]);

        EncodedPointStream(RoaringBitmap bitmap) {
            this.iterator = bitmap.getIntIterator();
        }

        BytesRef next() {
            if (iterator.hasNext()) {
                IntPoint.encodeDimension(iterator.next(), encoded.bytes, 0);
                return encoded;
            }
            return null;
        }

        /** Advances the iterator so that the next value is &ge; the decoded target. */
        void advance(byte[] target) {
            iterator.advanceIfNeeded(IntPoint.decodeDimension(target, 0));
        }
    }

    /**
     * A copy of {@link org.apache.lucene.search.PointInSetQuery}'s {@code MergePointVisitor}.
     * <p>
     * Does a merge-sort between the BKD tree's sorted leaves and the bitmap's
     * sorted encoded values. Both sides are scanned at most once, giving
     * O(N_index_leaves + N_bitmap_values) total work across the entire tree.
     */
    private class MergePointVisitor implements IntersectVisitor {
        private final DocIdSetBuilder result;
        private final EncodedPointStream iterator;
        private BytesRef nextQueryPoint;
        private final ArrayUtil.ByteArrayComparator comparator;
        private DocIdSetBuilder.BulkAdder adder;

        MergePointVisitor(DocIdSetBuilder result) {
            this.result = result;
            this.comparator = ArrayUtil.getUnsignedComparator(Integer.BYTES);
            this.iterator = new EncodedPointStream(bitmap);
            this.nextQueryPoint = iterator.next();
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

        private boolean matches(byte[] packedValue) {
            while (nextQueryPoint != null) {
                int cmp = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, packedValue, 0);
                if (cmp == 0) {
                    return true;
                } else if (cmp < 0) {
                    // Query point is before index point, skip ahead in the bitmap
                    iterator.advance(packedValue);
                    nextQueryPoint = iterator.next();
                } else {
                    // Query point is after index point, no match for this doc
                    break;
                }
            }
            return false;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            while (nextQueryPoint != null) {
                int cmpMin = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, minPackedValue, 0);
                if (cmpMin < 0) {
                    // query point is before the start of this cell, advance
                    iterator.advance(minPackedValue);
                    nextQueryPoint = iterator.next();
                    continue;
                }
                int cmpMax = comparator.compare(nextQueryPoint.bytes, nextQueryPoint.offset, maxPackedValue, 0);
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

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String defaultField) {
        if (bitmap.isEmpty()) {
            return "IntBitmapIndexBKDQuery(field=" + field + ", cardinality=0)";
        }
        return "IntBitmapIndexBKDQuery(field="
            + field
            + ", cardinality="
            + bitmap.getCardinality()
            + ", first="
            + bitmap.first()
            + ", last="
            + bitmap.last()
            + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        IntBitmapIndexBKDQuery that = (IntBitmapIndexBKDQuery) other;
        return field.equals(that.field) && bitmap.equals(that.bitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, bitmap);
    }
}
