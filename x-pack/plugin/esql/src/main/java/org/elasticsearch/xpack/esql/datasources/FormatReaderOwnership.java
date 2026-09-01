/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.io.Closeable;

/**
 * Drives the {@link FormatReader} ownership contract at the framework's minting sites.
 * <p>
 * A {@code with*} method returns either {@code this} — nothing was minted, so nothing changed hands — or a new
 * instance the caller owns and must close. Every framework caller therefore has the same question to answer:
 * "did this chain actually mint something, or am I holding the reader I was given?" Closing without asking would
 * close a reader the caller does not own, up to and including the registry's node-level singleton, whose
 * {@code close()} would then run once per query.
 */
final class FormatReaderOwnership {

    private static final Logger logger = LogManager.getLogger(FormatReaderOwnership.class);

    private FormatReaderOwnership() {}

    /**
     * Closes {@code derived} only if a {@code with*} chain actually minted it — that is, only if it is a
     * different instance from the {@code source} reader the caller started from. Never throws: a reader that
     * cannot release its own resources is a leak the caller cannot repair, and failing here would turn that leak
     * into a query failure over an otherwise-complete result.
     */
    static void closeIfDerived(@Nullable FormatReader derived, @Nullable FormatReader source) {
        if (derived == null || derived == source) {
            return;
        }
        try {
            derived.close();
        } catch (Exception e) {
            logger.warn(() -> "failed to close format reader [" + derived.getClass().getName() + "]", e);
        }
    }

    /**
     * The {@link Closeable} form of {@link #closeIfDerived}, for callers that hand the reader's release to a
     * lifecycle chain (e.g. the operator factory's {@code onClose}) instead of closing it inline. Returns
     * {@code null} when the chain minted nothing, so the caller can skip extending its chain at all.
     */
    @Nullable
    static Closeable ownedBy(@Nullable FormatReader derived, @Nullable FormatReader source) {
        if (derived == null || derived == source) {
            return null;
        }
        return () -> closeIfDerived(derived, source);
    }
}
