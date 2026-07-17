/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Implemented by reader iterators that emit the synthetic
 * {@link ColumnExtractor#ROW_POSITION_COLUMN} alongside their pages and can produce a matching
 * {@link ColumnExtractor} whose addressing space is identical to the one the iterator is using
 * for its row-position counter.
 * <p>
 * Implementing this interface is mandatory for any reader iterator on the deferred-extraction
 * path: opting in commits to the full handshake — the iterator both produces a matching
 * extractor and emits {@code _rowPosition} values <em>already encoded</em> with the registry-
 * assigned id, so downstream operators can decode them without an intermediate re-encoding
 * pass. Iterators that don't support deferred extraction simply omit
 * {@link ColumnExtractorAware} on the format reader and never reach this code path.
 *
 * <h2>Encoding handshake</h2>
 * <ol>
 *   <li>The factory wraps the iterator's pages and calls
 *       {@link #createColumnExtractor(Consumer)} to build a matching {@link ColumnExtractor}.</li>
 *   <li>The factory registers the extractor with {@code SourceExtractors}, receiving an id.</li>
 *   <li>The factory calls {@link #setExtractorId(int)} <em>before</em> draining the first page,
 *       handing the iterator the id it must OR into every {@code _rowPosition} value it emits.</li>
 *   <li>From then on, every page the iterator returns carries pre-encoded {@code _rowPosition}
 *       values of the form {@code (id << 48) | physicalRowOffset} — see {@code SourceExtractors}
 *       for the bit layout.</li>
 * </ol>
 * Encoding inside the iterator is significantly cheaper than wrapping the page stream with a
 * separate encoder: it avoids re-allocating the {@code _rowPosition} block per page (the iterator
 * already has the values in a primitive {@code long[]} buffer) and removes a per-page page-rebuild
 * step from the producer thread.
 * <p>
 * Implementations should construct the extractor lazily — typically on the first call to
 * {@link #createColumnExtractor(Consumer)} and may return the same instance on subsequent calls.
 * Lifetime is owned by the caller via {@code SourceExtractors} (the registry calls
 * {@link ColumnExtractor#close()} when the driver finishes).
 */
public interface ColumnExtractorProducer {

    /**
     * Creates the {@link ColumnExtractor} matching this iterator's addressing space.
     * <p>
     * The iterator must already be positioned to emit pages with {@link ColumnExtractor#ROW_POSITION_COLUMN}
     * before this is called; implementations may capture iterator-internal state (such as a
     * range-restricted footer) at construction time.
     *
     * @param driverThreadWarningSink where the extractor relays per-value declared-coercion warnings
     *                                (see {@link SkipWarnings}). The extractor runs on the driver
     *                                thread, so this sink emits directly to the response headers; it
     *                                is budget-gated so the whole source stays within one cap. May be
     *                                {@code null}, in which case the extractor falls back to emitting
     *                                warnings directly (per-instance cap only): the on-driver-thread
     *                                default used by tests and benchmarks.
     */
    ColumnExtractor createColumnExtractor(@Nullable Consumer<String> driverThreadWarningSink) throws IOException;

    /**
     * Hands the iterator the {@code SourceExtractors}-assigned id under which its matching
     * {@link ColumnExtractor} is registered. Must be called once, after
     * {@link #createColumnExtractor(Consumer)} and before the first page is drained. Every subsequent
     * page the iterator emits must carry {@code _rowPosition} values already encoded with this
     * id (typically by OR-ing {@code ((long) id << 48)} into each value as it is materialised).
     * <p>
     * Calling this method twice on the same iterator, or skipping it on the deferred path, is a
     * programmer error: the iterator's pages either carry mismatched ids or unencoded raw row
     * offsets that the lookup registry cannot route.
     *
     * @param id  registry-assigned extractor id; must be in {@code [0, MAX_EXTRACTOR_ID]}
     *            (see {@code SourceExtractors})
     */
    void setExtractorId(int id);
}
