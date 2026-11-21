/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;

/**
 * Where the chunks of one read hand their pieces of a stripe, so that the pieces of a stripe become one
 * contribution instead of one per chunk.
 * <p>
 * A read is cut into chunks on buffer fill at a record boundary, never on the stripe grid, so several chunks each
 * observe part of one stripe and each part carries that stripe's whole column summary. A chunk that has no sink
 * publishes its own part directly, which is what a reader driven outside a parallel read does; a chunk that has
 * one hands it over and the implementation decides when the stripe is whole. One instance serves one read and is
 * called from every chunk's parser thread, so an implementation must be thread-safe.
 */
public interface StripeStatsSink {

    /**
     * What a stripe's statistics were measured under, as one value because every part of it is a reason two
     * pieces of a stripe describe different rows: a different file version, a different way of interpreting its
     * bytes, a different grid to address it on, or an error policy that may have dropped rows from one and not
     * the other.
     *
     * @param readConfig                    fingerprint of the resolved read configuration, or {@code null} when
     *                                      the producing path had none to describe — which never compares equal
     *                                      to a known one
     * @param rowCountReadConfigIndependent whether the producing policy makes the row count the file's physical
     *                                      record count, and so shareable across read configurations
     */
    record ReadIdentity(
        long mtimeMillis,
        String configFingerprint,
        @Nullable String readConfig,
        boolean rowCountReadConfigIndependent,
        long stripeSize
    ) {}

    /**
     * Takes one chunk's piece of stripe {@code ordinal}, covering the half-open byte range {@code [start, end)}
     * of that stripe's grid cell in the read's own byte coordinates.
     *
     * @param atStripeStart whether this piece's chunk covers the stripe's left grid line, so the piece anchors
     *                      the stripe's true start — a byte-cover predicate, not "holds the stripe's first record"
     * @param atStripeEnd   whether its chunk covers the right grid line, or is the file-final chunk
     * @param eof           whether its chunk observed the end of the input and this is its last stripe
     * @param statistics    the piece's own statistics over the records attributed to this stripe, filtered to
     *                      the file schema and capped to the publishable extremum width, so a sink retains only
     *                      what a published contribution carries and never a value a record would withhold
     */
    void contribute(
        String sourceLocation,
        ReadIdentity identity,
        long ordinal,
        long start,
        long end,
        boolean atStripeStart,
        boolean atStripeEnd,
        boolean eof,
        SourceStatistics statistics
    );
}
