/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The runs of adjacent documents a segment's slices occupy.
 * <p>
 * A slice is a routing value and a slice index is sorted on it, so each slice holds one run of doc ids per segment.
 * Finding the runs only needs the boundaries between them, which the skip index of the sort field reports per
 * interval along with a document count, so no values are read. The field must hold one sorted doc value per
 * document, whose ordinals name the slices.
 */
public final class SliceRanges {

    private SliceRanges() {}

    /**
     * A run of adjacent documents. A {@link #isSingleSlice() single slice} run holds that slice's documents in the
     * segment; any other run straddles a slice boundary and its documents are not attributed. Slice ids are local to
     * the segment and increase with doc id.
     */
    public record Range(int minSliceId, int maxSliceId, int minDoc, int maxDoc, int docCount) {

        public boolean isSingleSlice() {
            return minSliceId == maxSliceId;
        }
    }

    @FunctionalInterface
    public interface Visitor {
        void visit(Range range) throws IOException;
    }

    /**
     * Visits {@code field}'s ranges in doc id order.
     *
     * @throws IllegalArgumentException if the field does not hold sorted doc values with a skip index
     */
    public static void scan(LeafReader leaf, String field, Visitor visitor) throws IOException {
        final DocValuesSkipper skipper = requireSkipper(leaf, field);
        // A slice can hold several intervals, so one is held back until the next proves it complete.
        Range pending = null;
        int doc = 0;
        while (doc < leaf.maxDoc()) {
            skipper.advance(doc);
            final int minDoc = skipper.minDocID(0);
            if (minDoc == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
            final int maxDoc = skipper.maxDocID(0);
            // The skip index of a sorted field records the ordinals of its values, which are ints.
            final int minSliceId = (int) skipper.minValue(0);
            final int maxSliceId = (int) skipper.maxValue(0);
            final int docCount = skipper.docCount(0);
            if (pending != null && pending.isSingleSlice() && minSliceId == maxSliceId && pending.minSliceId() == minSliceId) {
                pending = new Range(minSliceId, minSliceId, pending.minDoc(), maxDoc, pending.docCount() + docCount);
            } else {
                if (pending != null) {
                    visitor.visit(pending);
                }
                pending = new Range(minSliceId, maxSliceId, minDoc, maxDoc, docCount);
            }
            // The skipper may only be advanced past the interval it is on.
            doc = maxDoc + 1;
        }
        if (pending != null) {
            visitor.visit(pending);
        }
    }

    /**
     * All of {@code leaf}'s ranges. Holds one range per slice, so callers needing only the largest should filter in
     * {@link #scan} instead.
     */
    public static List<Range> collect(LeafReader leaf, String field) throws IOException {
        final List<Range> ranges = new ArrayList<>();
        scan(leaf, field, ranges::add);
        return ranges;
    }

    private static DocValuesSkipper requireSkipper(LeafReader leaf, String field) throws IOException {
        final FieldInfo info = leaf.getFieldInfos().fieldInfo(field);
        if (info != null && info.getDocValuesType() != DocValuesType.SORTED) {
            throw new IllegalArgumentException(
                "field [" + field + "] holds [" + info.getDocValuesType() + "] doc values, expected one sorted value a document"
            );
        }
        final DocValuesSkipper skipper = leaf.getDocValuesSkipper(field);
        if (skipper == null) {
            throw new IllegalArgumentException("field [" + field + "] has no doc values skip index to read slice ranges from");
        }
        return skipper;
    }

    /** Translates between a segment's slice keys and the ids {@link Range} reports. */
    public static final class Slices {

        private final SortedDocValues values;

        private Slices(SortedDocValues values) {
            this.values = values;
        }

        /** The slices of {@code field} in {@code leaf}, or {@code null} when the segment holds none of its values. */
        @Nullable
        public static Slices of(LeafReader leaf, String field) throws IOException {
            final SortedDocValues values = leaf.getSortedDocValues(field);
            return values == null ? null : new Slices(values);
        }

        public BytesRef key(int sliceId) throws IOException {
            return values.lookupOrd(sliceId);
        }

        /** The id of {@code key} in this segment, or a negative number when the segment has none of its documents. */
        public int id(BytesRef key) throws IOException {
            return values.lookupTerm(key);
        }
    }
}
