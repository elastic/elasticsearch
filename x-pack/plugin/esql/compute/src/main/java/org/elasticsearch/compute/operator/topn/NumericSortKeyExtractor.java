/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

/**
 * Per-page reader of the sort-key channel feeding {@link NumericTopNOperator}'s heap. The
 * operator builds one extractor at the top of each {@code addInput} call and then drives the
 * tight per-row loop through {@link #encodedAt(int)} + {@link #isNullAt(int)} — both monomorphic
 * once the JIT specialises on the concrete class produced by the per-type
 * {@code extractorFor(...)} factory.
 *
 * <p>This is the {@code NumericTopNOperator} analogue of {@link KeyExtractor} (which the generic
 * {@code TopNOperator} uses): instead of writing a sortable byte run into a
 * {@code BreakingBytesRefBuilder}, it returns an already-encoded {@code long} that the heap can
 * rank directly. The encoding is type-specific (e.g. {@code NumericUtils.doubleToSortableLong}
 * for {@code DOUBLE}, then bitwise NOT for ASC); see each per-type implementation.
 *
 * <p>Multi-valued sort keys are handled here, not in the operator. Mirroring the generic
 * {@link KeyExtractor} family ({@code KeyExtractorForLong.MinFromAscendingBlock} etc.) the per-page
 * factory picks an MV-MIN extractor for ASC and an MV-MAX extractor for DESC: a row that holds
 * multiple values in its sort-key slot competes as the most-favourable one under the sort
 * direction. An empty MV slot is treated as a null. This is exactly the semantic the generic
 * operator implements; the same query keeps producing the same answer when it switches to
 * {@link NumericTopNOperator}.
 */
interface NumericSortKeyExtractor {

    /**
     * The encoded long for {@code position} — ready to push into {@link PrimitiveTernaryHeap}.
     * Caller is responsible for first checking {@link #isNullAt(int)}: when the slot is null the
     * return value is undefined (an arbitrary placeholder), and the heap's composite ordering
     * routes the null via the null bit, not via this value.
     */
    long encodedAt(int position);

    /**
     * Whether the row at {@code position} is null on the sort key. A row whose multi-valued slot
     * holds zero values is treated as null, matching the generic
     * {@code KeyExtractorForLong.MinFromUnorderedBlock} behaviour ({@code size == 0 → nul(key)}).
     */
    boolean isNullAt(int position);

    /**
     * Shared encoding contract for every per-type extractor: under the configured direction, map
     * a raw signed-long sort key to the form {@link PrimitiveTernaryHeap}'s min-heap ranks
     * directly.
     *
     * <ul>
     *     <li>DESC: identity ({@code encoded = raw}). The heap root is the smallest raw
     *         survivor, i.e. the rejection threshold for the next incoming row.</li>
     *     <li>ASC: bitwise complement ({@code encoded = ~raw}). NOT is the monotonically
     *         decreasing involution that flips the order without the {@link Long#MIN_VALUE}
     *         overflow trap of unary negation ({@code -Long.MIN_VALUE == Long.MIN_VALUE}). The
     *         heap root is the largest raw survivor in encoded space.</li>
     * </ul>
     *
     * <p>Per-type extractors widen their primitive (INT signed extension, DOUBLE via
     * {@link org.apache.lucene.util.NumericUtils#doubleToSortableLong}, BOOLEAN to 0/1) into a
     * long before calling this. Decoding back to the raw type is the operator's job — see
     * {@code NumericTopNOperator#decodeLong}.
     */
    static long encode(long raw, boolean ascending) {
        return ascending ? ~raw : raw;
    }
}
