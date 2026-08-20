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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that matches documents whose numeric field value is present in a bitmap, for fields that
 * use the {@code index_terms} inverted-index path (see {@code NumberFieldMapper}).
 * <p>
 * Terms are sortable bytes, so the terms dictionary and the bitmap ascend in the same direction for
 * non-negative values. A merge-scan advances both iterators without re-seeking, giving
 * O(N_terms_in_range + M_bitmap_values) work per segment. {@link TermMerge} is that scan, shared by
 * everything below.
 * <p>
 * On a segment sorted by this field the matches come out of that scan already in doc order, so they
 * are streamed rather than collected; see {@link StreamingDocIdIterator}. Otherwise they are collected into a
 * {@link DocIdSetBuilder} first, which is what puts them in order.
 * <p>
 * The field's width lives entirely in the {@link BitmapValues}, so this handles {@code integer} and
 * {@code long} fields with one implementation. Terms are decoded to {@code long} rather than compared
 * as bytes, which keeps the merge working on values that cannot be invalidated by moving either
 * iterator &mdash; the {@link BytesRef} from {@link TermsEnum#term()} is only valid until the enum next
 * moves.
 * <p>
 * Only non-negative values are supported; the caller must validate this before constructing the
 * query.
 */
public class BitmapTermsQuery extends Query implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BitmapTermsQuery.class);

    private final String field;
    private final BitmapValues values;

    public BitmapTermsQuery(String field, BitmapValues values) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (values.isEmpty()) {
                    return null;
                }
                LeafReader reader = context.reader();
                Terms terms = reader.terms(field);
                if (terms == null) {
                    return null;
                }

                if (canStream(reader, terms)) {
                    final long estimatedCost = estimateCost(reader, terms);
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            // Opened here rather than per supplier, so a supplier asked only for its cost
                            // does not read doc values and each scorer gets its own cursor over them.
                            NumericDocValues docValues = DocValues.unwrapSingleton(DocValues.getSortedNumeric(reader, field));
                            TermMerge merge = new TermMerge(terms.iterator());
                            StreamingDocIdIterator scan = new StreamingDocIdIterator(merge, docValues, reader.maxDoc(), estimatedCost);
                            return new ConstantScoreScorer(score(), scoreMode, scan);
                        }

                        @Override
                        public long cost() {
                            return estimatedCost;
                        }
                    };
                }

                // Cheap proxy: an upper bound on the matching terms, since not every bitmap value need exist in the
                // dictionary. Not a bound on the matching docs, as one term can hold many; the exact count needs the
                // merge scan, and the collected DocIdSet reports its true cost once get() has run.
                final long cost = values.cardinality();
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), terms);
                        collectDocs(result, new TermMerge(terms.iterator()));
                        return new ConstantScoreScorer(score(), scoreMode, result.build().iterator());
                    }

                    @Override
                    public long cost() {
                        return cost;
                    }
                };
            }

            /**
             * Sums the matching terms' {@code docFreq}, which the terms dictionary already carries, so
             * counting decodes no postings and visits no document. This needs no index sort.
             * <p>
             * Both statistics ignore deletions, and a multi-valued field could file one document under
             * several matching terms, so either would make the sum an overcount; those fall back to
             * counting by iteration.
             */
            @Override
            public int count(LeafReaderContext context) throws IOException {
                if (values.isEmpty()) {
                    return 0;
                }
                LeafReader reader = context.reader();
                if (reader.hasDeletions()) {
                    return super.count(context);
                }
                Terms terms = reader.terms(field);
                if (terms == null) {
                    return 0;
                }
                if (singleValued(terms) == false) {
                    return super.count(context);
                }
                TermMerge merge = new TermMerge(terms.iterator());
                int count = 0;
                while (merge.nextMatch()) {
                    count += merge.docFreq();
                }
                return count;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // Only the streaming path reads doc values, to skip with, and those are updatable.
                return SegmentSort.ascendingBy(ctx.reader(), field) == false || DocValues.isCacheable(ctx, field);
            }
        };
    }

    /**
     * Whether this segment's matches come out of the merge scan already in doc order, so they can be
     * streamed instead of collected.
     * <p>
     * Doc order must equal value order, and the field must be single-valued: index sort positions a
     * multi-valued document by one of its values, so another of its values could belong to a much later
     * term while the document itself sits early.
     * <p>
     * Documents missing a value are fine, and where the sort's missing value places them does not matter.
     * They carry no term, so they can only leave gaps in the doc ids a term's postings cover; two
     * documents with values V1 &lt; V2 still sort in that order, so {@code term(V1)}'s postings still
     * precede {@code term(V2)}'s. Nor is the missing value visible to {@link StreamingDocIdIterator#skipTo}, which
     * reads doc values rather than the sort key: {@link NumericDocValues#advanceExact} simply reports
     * false for such a document and the skip is declined.
     */
    private boolean canStream(LeafReader reader, Terms terms) throws IOException {
        return SegmentSort.ascendingBy(reader, field) && singleValued(terms);
    }

    /** Every document with this field carries exactly one value, so no document repeats across terms. */
    private static boolean singleValued(Terms terms) throws IOException {
        return terms.getSumDocFreq() == terms.getDocCount();
    }

    /**
     * Rough per-segment cost for the streaming path: the bitmap's cardinality scaled by the average documents per
     * term, clamped to {@code maxDoc}. Both inputs are terms dictionary metadata, since the true count would take
     * the merge scan. The collecting path can afford the plain cardinality because the built set reports its real
     * size soon after; a streamed one never does, and Lucene keeps using this number to order conjunctions.
     */
    private long estimateCost(LeafReader reader, Terms terms) throws IOException {
        long termCount = terms.size();
        long avgDocsPerTerm = termCount > 0 ? Math.max(1, terms.getDocCount() / termCount) : 1;
        return Math.min(reader.maxDoc(), values.cardinality() * avgDocsPerTerm);
    }

    /** Collects every matching term's postings, which the builder then puts in doc order. */
    private static void collectDocs(DocIdSetBuilder result, TermMerge merge) throws IOException {
        PostingsEnum postings = null;
        while (merge.nextMatch()) {
            postings = merge.postings(postings);
            result.grow(merge.docFreq()).add(postings);
        }
    }

    /**
     * Walks the terms dictionary and the bitmap together, stopping on each term present in both. Both
     * sides ascend in the same order for non-negative values, so neither cursor ever rewinds and the
     * whole scan costs O(N_terms_in_range + M_bitmap_values).
     */
    private final class TermMerge {
        private final TermsEnum termsEnum;
        private final BitmapValues.PeekableIterator bitmap = values.iterator();
        /** Always holds the encoding of {@link #bitmapValue}; {@link #seekTarget} is a view onto it. */
        private final byte[] encoded = new byte[values.bytesPerValue()];
        private final BytesRef seekTarget = new BytesRef(encoded);

        private long bitmapValue;
        private long termValue;
        private boolean exhausted;
        /** Whether the cursors rest on a match that the next call must step past. */
        private boolean onMatch;

        TermMerge(TermsEnum termsEnum) throws IOException {
            this.termsEnum = termsEnum;
            if (nextBitmapValue() == false || termsEnum.seekCeil(seekTarget) == TermsEnum.SeekStatus.END) {
                exhausted = true;
                return;
            }
            termValue = decodeTerm(termsEnum.term());
        }

        /** Positions the terms enum on the next term present in both. False once either side runs out. */
        boolean nextMatch() throws IOException {
            if (exhausted) {
                return false;
            }
            if (onMatch) {
                onMatch = false;
                if (stepPastMatch() == false) {
                    return false;
                }
            }
            while (bitmapValue != termValue) {
                if (bitmapValue > termValue) {
                    // Terms enum is behind: seek it forward to the current bitmap value.
                    if (termsEnum.seekCeil(seekTarget) == TermsEnum.SeekStatus.END) {
                        exhausted = true;
                        return false;
                    }
                    termValue = decodeTerm(termsEnum.term());
                } else {
                    // Bitmap is behind: advance it to at least termValue.
                    bitmap.advanceTo(termValue);
                    if (nextBitmapValue() == false) {
                        exhausted = true;
                        return false;
                    }
                }
            }
            onMatch = true;
            return true;
        }

        /** Documents carrying the matched term, deletions included, as the terms dictionary records it. */
        int docFreq() throws IOException {
            return termsEnum.docFreq();
        }

        PostingsEnum postings(PostingsEnum reuse) throws IOException {
            return termsEnum.postings(reuse, PostingsEnum.NONE);
        }

        /**
         * Jumps the bitmap cursor to {@code value}, so a far skip costs one seek rather than one per term
         * passed over. The terms enum is left behind deliberately: the next {@link #nextMatch()} sees
         * {@code bitmapValue > termValue} and seeks it in one step.
         */
        void skipTo(long value) throws IOException {
            if (exhausted || value <= bitmapValue) {
                return;
            }
            // The cursor is being repositioned past the current match, so there is nothing left to step
            // past; doing both would skip a value.
            onMatch = false;
            bitmap.advanceTo(value);
            if (nextBitmapValue() == false) {
                exhausted = true;
            }
        }

        /** Moves both cursors past the term just returned. */
        private boolean stepPastMatch() throws IOException {
            if (nextBitmapValue() == false) {
                exhausted = true;
                return false;
            }
            BytesRef next = termsEnum.next();
            if (next == null) {
                exhausted = true;
                return false;
            }
            termValue = decodeTerm(next);
            return true;
        }

        /** Consumes the pending bitmap value into {@link #bitmapValue} and {@link #encoded}. */
        private boolean nextBitmapValue() {
            if (bitmap.hasNext() == false) {
                return false;
            }
            bitmap.encodePeek(encoded);
            bitmapValue = bitmap.next();
            return true;
        }
    }

    /**
     * Streams the merge scan's matches without materialising them.
     * <p>
     * Under an ascending index sort on this field, every document of a term precedes every document of
     * a larger term, so walking the terms in order already yields doc ids in order. The
     * {@link DocIdSetBuilder} the unsorted path needs in order to sort them is pure overhead here, and
     * paying it up front is what stops a consumer from finishing early: a top-N collector cannot reach
     * its limit until the whole match set has been built.
     */
    private static final class StreamingDocIdIterator extends DocIdSetIterator {
        private final TermMerge merge;
        private final NumericDocValues docValues;
        private final int maxDoc;
        private final long cost;
        /** Kept across terms so each one recycles the last one's enum rather than allocating. */
        private PostingsEnum postings;
        /** Whether {@link #postings} belongs to a term still being emitted. */
        private boolean draining;
        private int doc = -1;

        StreamingDocIdIterator(TermMerge merge, NumericDocValues docValues, int maxDoc, long cost) {
            this.merge = merge;
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
            while (true) {
                if (draining) {
                    int next = postings.nextDoc();
                    if (next != NO_MORE_DOCS) {
                        return doc = next;
                    }
                    draining = false;
                }
                if (merge.nextMatch() == false) {
                    return doc = NO_MORE_DOCS;
                }
                postings = merge.postings(postings);
                draining = true;
            }
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= maxDoc) {
                // No document can satisfy this, and skipTo below would read doc values out of bounds.
                return doc = NO_MORE_DOCS;
            }
            if (draining) {
                int next = postings.advance(target);
                if (next != NO_MORE_DOCS) {
                    return doc = next;
                }
                draining = false;
            }
            skipTo(target);
            while (merge.nextMatch()) {
                postings = merge.postings(postings);
                int next = postings.advance(target);
                if (next != NO_MORE_DOCS) {
                    draining = true;
                    return doc = next;
                }
            }
            return doc = NO_MORE_DOCS;
        }

        /**
         * Reads the value carried by {@code targetDoc} and jumps the merge there, turning a far skip into
         * one seek instead of one per term in between. Doc values are read forward only, which holds
         * because advance targets increase monotonically.
         */
        private void skipTo(int targetDoc) throws IOException {
            if (docValues == null || docValues.advanceExact(targetDoc) == false) {
                return;
            }
            merge.skipTo(docValues.longValue());
        }

        @Override
        public long cost() {
            return cost;
        }
    }

    /** Decodes immediately, so nothing holds a {@link BytesRef} across a move of the terms enum. */
    private long decodeTerm(BytesRef term) {
        return values.decode(term.bytes, term.offset);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String defaultField) {
        return "BitmapTermsQuery(field=" + field + ", " + values + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (sameClassAs(other) == false) {
            return false;
        }
        BitmapTermsQuery that = (BitmapTermsQuery) other;
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
