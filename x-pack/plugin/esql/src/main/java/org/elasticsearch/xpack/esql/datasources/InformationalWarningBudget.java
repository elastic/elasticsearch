/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Central, thread-safe cap and de-duplicator for the informational-warning channel of a single
 * external source on one node. A read fans out into many units (row-group macro-splits, one
 * iterator per globbed file) spread across parallel drivers, each unit building its own
 * {@link SkipWarnings} capped only per instance; without a shared budget the total grows with the
 * unit count until the response {@code Warning} headers are undeliverable. One budget owned by
 * {@code AsyncExternalSourceOperatorFactory} (shared across all its drivers) bounds the whole source
 * per node.
 * <p>
 * {@link #accept(String)} is the gate every informational sink calls before delivering a warning. It
 * dedups by value (matching how {@code ThreadContext} dedups response headers) and admits at most
 * {@code cap} distinct payload messages plus one overflow marker, whichever layer the warning came
 * from (an eager reader relaying through the source buffer, or the driver-thread deferred extractor).
 * Lock-free: concurrent producer threads across drivers share one budget.
 */
final class InformationalWarningBudget {

    private final int cap;
    private final Set<String> seen = ConcurrentHashMap.newKeySet();
    private final AtomicInteger count = new AtomicInteger();
    private final AtomicBoolean overflow = new AtomicBoolean();

    InformationalWarningBudget(int cap) {
        if (cap < 1) {
            // cap 0 would deliver only the overflow marker (the first warning already crosses the cap),
            // suppressing every summary and detail; a negative cap is nonsensical.
            throw new IllegalArgumentException("cap must be at least 1, got [" + cap + "]");
        }
        this.cap = cap;
    }

    /**
     * Decides what to deliver for one informational warning: the input itself when it fits within
     * the cap and is not a duplicate, the one-time overflow marker for the caller that first crosses
     * the cap, or {@code null} to drop (a duplicate, or any warning once the budget is full).
     * <p>
     * A warning equal to {@link SkipWarnings#overflowMessage()} is a per-instance meta-notice (that
     * one chunk's own {@code MAX_ADDED_WARNINGS} detail cap was exceeded), not real summary/detail
     * content, so it is relayed at most once via the {@link #seen} dedup check without spending one
     * of the {@code cap} slots reserved for content; several chunks hitting their own cap independently
     * therefore cost this budget one dedup entry, not one slot each. It reuses the identical text this
     * budget's own overflow marker uses, so the two are indistinguishable to the client either way.
     * <p>
     * Once overflow is set, later calls short-circuit before touching {@link #seen}. The dedup set is
     * therefore bounded by {@code cap} plus the distinct warnings (including at most one overflow-marker
     * entry) offered concurrently in the window before overflow becomes visible; it stops growing once
     * overflow flips. This is a small, bounded overshoot on the informational (warning-time) path, not
     * a per-row cost.
     */
    @Nullable
    String accept(String warning) {
        // Warnings are always non-null messages built by SkipWarnings; fail fast on a broken caller
        // rather than letting the null propagate into the dedup set or a response header.
        Objects.requireNonNull(warning, "warning");
        if (overflow.get()) {
            return null;
        }
        if (seen.add(warning) == false) {
            return null;
        }
        if (warning.equals(SkipWarnings.overflowMessage())) {
            return warning;
        }
        if (count.incrementAndGet() <= cap) {
            return warning;
        }
        // Exactly one caller crosses the cap first and emits the single overflow marker; the rest drop.
        return overflow.compareAndSet(false, true) ? SkipWarnings.overflowMessage() : null;
    }
}
