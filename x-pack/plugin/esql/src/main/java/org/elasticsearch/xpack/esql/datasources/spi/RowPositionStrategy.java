/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

/**
 * Per-reader strategy for populating the {@code _rowPosition} slot in emitted pages.
 * <p>
 * Every {@link FormatReader} declares its strategy via {@link FormatReader#rowPositionStrategy()};
 * the dispatcher applies the strategy polymorphically — there is no switch over reader types and
 * no {@code instanceof} check on the reader. The strategy is a small, focused object that owns
 * <em>both</em> the question "does the reader natively populate {@code _rowPosition}?" and the
 * follow-up "if not, how should the slot be filled?".
 *
 * <p>Two concrete shapes today: {@link PassThroughRowPositionStrategy} for readers that fill the
 * slot themselves in their native iterator (parquet-mr, ORC, CSV, NDJSON), and
 * {@link NullSpliceRowPositionStrategy} for readers that have no row-position channel and must
 * surface NULL (parquet-rs). A future {@code ByteOffsetRowPositionStrategy} can lift the per-batch
 * byte-offset injection out of the CSV / NDJSON hot path without touching the dispatcher.
 *
 * <p>Adding a new strategy is the only change required to support a new reader family — every
 * dispatch site already calls {@code strategy.apply(inner, ctx)} polymorphically.
 */
public interface RowPositionStrategy {

    /**
     * Wrap (or pass through) the reader's inner page iterator so each page has the
     * {@code _rowPosition} slot populated. Validates any preconditions this strategy requires;
     * throws {@link IllegalStateException} when a precondition is unmet (e.g. byte-offset strategy
     * called with no decompressed anchor).
     *
     * @param rowPositionSlot the index in the projected output where {@code _rowPosition} sits, or
     *                        {@code -1} when {@code _rowPosition} is not in the projection. The
     *                        dispatcher pre-computes this once per {@code reader.read()} so
     *                        strategies inspect a primitive instead of walking a list per call.
     */
    CloseableIterator<Page> apply(CloseableIterator<Page> inner, int rowPositionSlot);

    /**
     * Human-readable reason why this strategy emits the shape it does, available to wrapping
     * iterators for their {@code describe()} output (the null-splice iterator embeds it) so
     * {@code _id} composition against a null-splice reader is attributable rather than silently
     * null. The default returns the simple class name; the null-splice strategy overrides to
     * expose its constructor reason.
     */
    default String reason() {
        return getClass().getSimpleName();
    }
}
