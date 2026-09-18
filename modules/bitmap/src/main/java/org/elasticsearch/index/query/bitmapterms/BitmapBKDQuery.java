/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.PointTree;
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
import org.apache.lucene.util.IntsRef;
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
 * On a segment sorted by this field the merge's matches come out already in doc order, so they are
 * streamed rather than collected; see {@link StreamingDocIdIterator}. Otherwise they are collected into a
 * {@link DocIdSetBuilder} first, which is what puts them in order &mdash; and what stops a top-N consumer
 * from finishing early, since it cannot reach its limit until the whole match set has been built.
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

                // Read once and shared: every member that reports a cost returns this, and the streaming
                // scan also carries it as its own DocIdSetIterator cost.
                final long estimatedCost = estimateCost(pointValues.getDocCount(), segmentMin, segmentMax);

                if (streams(reader, pointValues)) {
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            // Opened here rather than per supplier, so a supplier asked only for its cost
                            // reads neither structure and each scorer gets its own cursor over both.
                            NumericDocValues docValues = DocValues.unwrapSingleton(DocValues.getSortedNumeric(reader, field));
                            StreamingDocIdIterator scan = new StreamingDocIdIterator(
                                pointValues.getPointTree(),
                                docValues,
                                reader.maxDoc(),
                                estimatedCost
                            );
                            return new ConstantScoreScorer(score(), scoreMode, scan);
                        }

                        @Override
                        public long cost() {
                            return estimatedCost;
                        }
                    };
                }

                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), pointValues);
                        pointValues.intersect(new CollectingMergePointVisitor(result));
                        return new ConstantScoreScorer(score(), scoreMode, result.build().iterator());
                    }

                    @Override
                    public long cost() {
                        return estimatedCost;
                    }
                };
            }

            /**
             * Only the streaming path reads doc values, to skip with, and those are updatable; the collecting
             * path reads points alone, which cannot change under a cached entry. Testing the same predicate
             * the scorer chooses with keeps a segment that collects from being withheld from the cache on the
             * strength of a dependency it does not have.
             */
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                PointValues pointValues;
                try {
                    pointValues = ctx.reader().getPointValues(field);
                } catch (IOException e) {
                    // This method cannot throw, and a segment whose points will not read is in no state to
                    // have its results cached either.
                    return false;
                }
                return pointValues == null || streams(ctx.reader(), pointValues) == false || DocValues.isCacheable(ctx, field);
            }
        };
    }

    /**
     * Whether this segment's matches come out of the tree walk in doc order, so they can be streamed rather
     * than collected. Under an ascending index sort they do; single-valued only, which is what
     * {@code size() == getDocCount()} says, since index sort places a multi-valued document by one of its
     * values and another of them can sit in a far later cell while the document sits early.
     */
    private boolean streams(LeafReader reader, PointValues pointValues) {
        return SegmentSort.ascendingBy(reader, field) && pointValues.size() == pointValues.getDocCount();
    }

    /**
     * Rough per-segment cost, from segment metadata alone: the bitmap's cardinality scaled by the average
     * documents per distinct value, clamped to the segment's doc count. That factor is not 1 just because
     * the field is single-valued &mdash; that bounds values per document, not documents per value, and many
     * documents may share one. Spreading the documents evenly across the segment's value span is the only
     * way to get it without reading the tree, since BKD records no distinct-value count.
     * <p>
     * {@link PointValues#estimateDocCount} is more accurate, but it walks the tree, and a bitmap of
     * scattered values crosses most cells so that walk reaches nearly every leaf. A {@code bool} query asks
     * for a cost in order to decide which clause to lead with, so paying it there means traversing once to
     * plan and again to execute &mdash; which is the work {@code cost()} is asked in order to avoid. The
     * streaming path never traverses eagerly at all, and the {@code DocIdSet} that would go on to report a
     * true cost never exists there.
     * <p>
     * Even spread is a crude assumption. A field holding dense ids plus one distant outlier has a span far
     * wider than its populated range, which flattens the average to one document per value and makes the
     * bitmap look more selective than it is.
     */
    private long estimateCost(int docCount, byte[] segmentMin, byte[] segmentMax) {
        long lowest = values.decode(segmentMin, 0);
        long highest = values.decode(segmentMax, 0);
        // Inclusive bounds, hence the +1, and in floating point because a long field's span can exceed
        // Long.MAX_VALUE -- Long.MIN_VALUE to Long.MAX_VALUE wraps to -1 in long arithmetic.
        double span = (double) highest - (double) lowest + 1.0;
        // Floored at one document per value, since a value the segment holds is carried by at least one.
        // A sparser field than that divides below 1, and a zero would report the query as matching
        // nothing -- which is the reading that makes a conjunction lead with this clause.
        double avgDocsPerValue = Math.max(1.0, docCount / span);
        // Clamped before multiplying, so neither factor exceeds the doc count and the product cannot
        // overflow whatever cardinality the bitmap reports.
        long cardinality = Math.min(values.cardinality(), docCount);
        return Math.min(docCount, (long) (cardinality * avgDocsPerValue));
    }

    /**
     * The merge-sort between the BKD tree's sorted leaves and the bitmap's sorted encoded values,
     * derived from {@link org.apache.lucene.search.PointInSetQuery}'s {@code MergePointVisitor}. Both
     * sides are scanned at most once, giving O(N_index_leaves + N_bitmap_values) total work across the
     * entire tree.
     * <p>
     * This half decides <em>which cells match</em> and collects nothing. Collecting the matched documents
     * costs somewhere to put them, and the two paths want different somewheres, so that lives in the
     * subclasses: {@link CollectingMergePointVisitor} for the whole match set, and
     * {@link BufferingMergePointVisitor} for one cell of it at a time.
     */
    private abstract class MergePointVisitor implements IntersectVisitor {
        private final BitmapValues.PeekableIterator iterator;
        private final ArrayUtil.ByteArrayComparator comparator;
        /** The bitmap value being merged, encoded. Owned here, so nothing else can recycle it. */
        private final byte[] queryPoint;
        /** {@link #queryPoint} decoded, so that a skip target can be compared without decoding it. */
        private long queryValue;
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
                queryValue = iterator.peek();
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

        /**
         * Jumps the bitmap cursor to {@code value}, so that every cell below it is then rejected by a
         * single {@link #compare} rather than descended into. Unlike {@link #skipTo(byte[])} this is
         * reached from outside the merge, where the cursor may already be at or past the target, so it
         * checks before consuming — a blind {@code takeQueryPoint} would drop a value that still matches.
         */
        void skipTo(long value) {
            if (hasQueryPoint == false || value <= queryValue) {
                return;
            }
            iterator.advanceTo(value);
            takeQueryPoint();
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

                if (cmpMin == 0) {
                    if (cmpMax == 0) {
                        // cell min and max are exactly equal to our point,
                        // which can easily happen if many (> 512) docs share this one value
                        return Relation.CELL_INSIDE_QUERY;
                    }
                    // If the bitmap covers all values in the cell, match it whole without decoding any points.
                    long cellMax = values.decode(maxPackedValue, 0);
                    if (cellMax - queryValue < values.cardinality() && values.coversRange(queryValue, cellMax)) {
                        return Relation.CELL_INSIDE_QUERY;
                    }
                }
                return Relation.CELL_CROSSES_QUERY;
            }

            // We exhausted all points in the bitmap
            return Relation.CELL_OUTSIDE_QUERY;
        }
    }

    /**
     * Adds document collection to {@link MergePointVisitor}, accumulating the whole match set into a
     * {@link DocIdSetBuilder}, which is what puts it in doc order. Used wherever the walk's own order
     * cannot be relied on, which is every segment the index sort does not cover.
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

    /**
     * Holds one cell's matching doc ids for {@link StreamingDocIdIterator} to hand out. Separate from
     * {@link CollectingMergePointVisitor} because that one accumulates the whole match set before any of
     * it is returned, which is precisely what the streaming path must not do; this one is emptied and
     * refilled per cell, so its footprint stays a leaf's worth of points however large the match set
     * grows.
     */
    private final class BufferingMergePointVisitor extends MergePointVisitor {
        private int[] docs = new int[64];
        private int size;
        private int position;

        /** Empties the buffer, so it only ever holds the cell currently being visited. */
        void reset() {
            size = 0;
            position = 0;
        }

        boolean isEmpty() {
            return position == size;
        }

        int next() {
            return docs[position++];
        }

        /** The first buffered doc at or after {@code target}, or -1 once the buffer is drained. */
        int advanceTo(int target) {
            while (position < size) {
                int doc = docs[position++];
                if (doc >= target) {
                    return doc;
                }
            }
            return -1;
        }

        @Override
        public void grow(int count) {
            docs = ArrayUtil.grow(docs, size + count);
        }

        @Override
        public void visit(int docID) {
            if (size == docs.length) {
                docs = ArrayUtil.grow(docs, size + 1);
            }
            docs[size++] = docID;
        }

        @Override
        public void visit(IntsRef ref) {
            grow(ref.length);
            System.arraycopy(ref.ints, ref.offset, docs, size, ref.length);
            size += ref.length;
        }

        @Override
        public void visit(DocIdSetIterator docIdSetIterator) throws IOException {
            for (int docID = docIdSetIterator.nextDoc(); docID != DocIdSetIterator.NO_MORE_DOCS; docID = docIdSetIterator.nextDoc()) {
                visit(docID);
            }
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
                visit(docIdSetIterator);
            }
        }
    }

    /**
     * Streams the merge's matches, for a segment whose values increase with its doc ids &mdash; the
     * ascending index sort the caller checked for, which is what makes the walk emit doc ids in order.
     * <p>
     * The tree is walked through a {@link PointTree} cursor rather than
     * {@link PointValues#intersect(IntersectVisitor)}, because that recurses and a recursion cannot be
     * suspended between two documents; here the cursor's position in the tree <em>is</em> the resume point.
     * Each step compares the current cell against the bitmap and does one of three things: steps over a
     * cell the bitmap misses entirely, descends into an inner cell, or reads a leaf's matching doc ids into
     * a buffer that {@link #nextDoc()} then hands out one at a time. Descending as far as the leaves rather
     * than bulk-reading an inner cell that matches in full is what keeps that buffer to one leaf's points,
     * however many documents an inner cell covers.
     */
    private final class StreamingDocIdIterator extends DocIdSetIterator {
        private final PointTree tree;
        private final BufferingMergePointVisitor visitor = new BufferingMergePointVisitor();
        private final NumericDocValues docValues;
        private final int maxDoc;
        private final long cost;
        /** Whether the walk has climbed back out of the root, leaving no cell left to visit. */
        private boolean finished;
        private int doc = -1;

        StreamingDocIdIterator(PointTree tree, NumericDocValues docValues, int maxDoc, long cost) {
            this.tree = tree;
            this.docValues = docValues;
            this.maxDoc = maxDoc;
            this.cost = cost;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            if (visitor.isEmpty() && nextCellWithMatches() == false) {
                return doc = NO_MORE_DOCS;
            }
            return emit(visitor.next());
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= maxDoc) {
                // No document can satisfy this, and the doc values read below would be out of bounds.
                return doc = NO_MORE_DOCS;
            }
            int buffered = visitor.advanceTo(target);
            if (buffered != -1) {
                return emit(buffered);
            }
            // The tree positions on a value quickly, but advance() is given a doc id and the tree holds no
            // doc-id index to translate it with. Doc values do: the value this document carries, which under
            // the index sort is a lower bound on every match from here on. Jumping the bitmap cursor there
            // lets a single compare() reject each cell in between. Read forward only, since advance targets
            // increase monotonically; a document with no value declines the jump and the walk proceeds.
            if (docValues != null && docValues.advanceExact(target)) {
                visitor.skipTo(docValues.longValue());
            }
            while (nextCellWithMatches()) {
                buffered = visitor.advanceTo(target);
                if (buffered != -1) {
                    return emit(buffered);
                }
            }
            return doc = NO_MORE_DOCS;
        }

        /**
         * The one place a doc id leaves this iterator, so the ascending order the whole strategy rests on
         * is asserted once rather than argued about per call site.
         */
        private int emit(int next) {
            assert next > doc : "doc ids out of order: [" + next + "] after [" + doc + "]";
            return doc = next;
        }

        /**
         * Advances the walk to the next cell holding matches and buffers them. False once the tree is
         * exhausted.
         */
        private boolean nextCellWithMatches() throws IOException {
            while (finished == false) {
                Relation relation = visitor.compare(tree.getMinPackedValue(), tree.getMaxPackedValue());
                if (relation == Relation.CELL_OUTSIDE_QUERY) {
                    moveToNextCell();
                    continue;
                }
                if (tree.moveToChild()) {
                    // Descend even when the whole cell matches, so that what is buffered is bounded by a
                    // leaf's points rather than by however many documents an inner cell holds. The child
                    // reaches the same verdict without moving the bitmap cursor, since it spans a subrange
                    // of a range already found covered; what that costs is one more coversRange probe per
                    // level, against a whole subtree's worth of documents held in memory at once.
                    continue;
                }
                visitor.reset();
                if (relation == Relation.CELL_INSIDE_QUERY) {
                    // Every point in the leaf matches, so its values need not be read back at all.
                    tree.visitDocIDs(visitor);
                } else {
                    tree.visitDocValues(visitor);
                }
                // Stepped past now rather than on the next call, so the cursor always rests on work still
                // to do and the buffer is never tied to where the cursor sits.
                moveToNextCell();
                if (visitor.isEmpty() == false) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Steps the pre-order walk past the current cell without descending into it, climbing to a parent
         * whenever a cell has no further sibling. Skipping a whole subtree therefore costs no more than
         * the one {@link MergePointVisitor#compare} that rejected it.
         */
        private void moveToNextCell() throws IOException {
            while (tree.moveToSibling() == false) {
                if (tree.moveToParent() == false) {
                    finished = true;
                    return;
                }
            }
        }

        @Override
        public long cost() {
            return cost;
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
