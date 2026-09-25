/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.util.Accountable;

/**
 * The width-specific half of the bitmap queries: a sorted set of non-negative values backed by a
 * roaring bitmap, which {@link BitmapBKDQuery} and {@link BitmapTermsQuery} merge against a segment
 * without knowing whether the field is an {@code integer} or a {@code long}.
 * <p>
 * Values are exchanged as {@code long} regardless of the field's width, following
 * {@link org.apache.lucene.search.comparators.NumericComparator}, which shares its skipping logic
 * across widths the same way — a {@code bytesCount} field plus a {@code sortableBytesToLong} hook.
 * Widening {@code int} to {@code long} is lossless and order-preserving, and it is what Lucene
 * already does on the doc-values path, where {@link org.apache.lucene.index.NumericDocValues#longValue()}
 * returns a {@code long} for both widths.
 * <p>
 * Nothing narrows: the encode methods are implemented by the width that owns the values, so an
 * {@code integer} bitmap encodes an {@code int} it already holds rather than casting one back down.
 * <p>
 * Implementations are immutable and safe to share across concurrent per-segment search threads,
 * which a cached {@link org.apache.lucene.search.Query} requires.
 */
public interface BitmapValues extends Accountable {

    boolean isEmpty();

    long cardinality();

    /** The smallest value. Undefined when empty. */
    long first();

    /** The largest value. Undefined when empty. */
    long last();

    /** Width of one value's sortable-bytes encoding: {@code Integer.BYTES} or {@code Long.BYTES}. */
    int bytesPerValue();

    /**
     * Decodes {@link #bytesPerValue()} sortable bytes, as written by this width's encoder and as the
     * field's BKD points and {@code index_terms} terms both store them.
     */
    long decode(byte[] src, int offset);

    /** Writes {@link #first()} as sortable bytes. Undefined when empty. */
    void encodeFirst(byte[] dest);

    /** Writes {@link #last()} as sortable bytes. Undefined when empty. */
    void encodeLast(byte[] dest);

    /**
     * Whether the bitmap covers the inclusive range {@code [min, max]}, meaning it holds every value in
     * that range. Both bounds must be non-negative, which the queries' precondition on the values gives.
     * <p>
     * A {@code false} may mean "cannot be decided cheaply" rather than "not covered": callers use this
     * only to take a faster path, so declining costs an optimization, never correctness.
     */
    boolean coversRange(long min, long max);

    PeekableIterator iterator();

    /**
     * A forward-only iterator over the values in ascending order. Independent iterators may be taken
     * concurrently from one {@link BitmapValues}.
     * <p>
     * The value at the head of the iterator is <em>pending</em> until {@link #next()} consumes it, so a
     * caller can inspect it repeatedly without advancing — which the merge against a sorted index
     * needs, since several documents may share one value.
     */
    interface PeekableIterator {

        /** Whether a pending value remains. */
        boolean hasNext();

        /** The pending value, without consuming it. Only valid while {@link #hasNext()} holds. */
        long peek();

        /** Writes the pending value as sortable bytes, without consuming it. */
        void encodePeek(byte[] dest);

        /** Consumes and returns the pending value. Only valid while {@link #hasNext()} holds. */
        long next();

        /**
         * Skips pending values below {@code target}, so that {@link #peek()} is at or after it.
         * <p>
         * Since every value is non-negative, a negative {@code target} is below all of them and
         * advances nothing.
         */
        void advanceTo(long target);
    }
}
