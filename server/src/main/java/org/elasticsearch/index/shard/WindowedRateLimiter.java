/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Grants a fixed number of permits in each fixed time window, so a message that can occur for every failed document
 * is logged at a high level only for the first few failures. Windows are aligned to the clock, not to the first
 * permit, so permits taken just before a window ends and just after it starts can be close together in time.
 * Share one instance between all the callers that must stay within a single limit.
 */
final class WindowedRateLimiter {

    private record WindowUsage(long window, int permitsUsed) {}

    private final int permitsPerWindow;
    private final long windowNanos;
    private final LongSupplier nanoTimeSupplier;
    private final AtomicReference<WindowUsage> usage = new AtomicReference<>(new WindowUsage(0, 0));

    WindowedRateLimiter(int permitsPerWindow, TimeValue window) {
        this(permitsPerWindow, window, System::nanoTime);
    }

    WindowedRateLimiter(int permitsPerWindow, TimeValue window, LongSupplier nanoTimeSupplier) {
        if (permitsPerWindow <= 0) {
            throw new IllegalArgumentException("permitsPerWindow must be positive but was [" + permitsPerWindow + "]");
        }
        Objects.requireNonNull(window, "window must not be null");
        if (window.nanos() <= 0) {
            throw new IllegalArgumentException("window must be at least one nanosecond but was [" + window + "]");
        }
        Objects.requireNonNull(nanoTimeSupplier, "nanoTimeSupplier must not be null");
        this.permitsPerWindow = permitsPerWindow;
        this.windowNanos = window.nanos();
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    /**
     * Cheap read-only check that lets callers skip their work once the permits are used. It must compare the window,
     * because the usage is only reset by {@link #tryAcquirePermit()}, which this check guards.
     */
    boolean mayHavePermit() {
        WindowUsage current = usage.get();
        return current.window() != currentWindow() || current.permitsUsed() < permitsPerWindow;
    }

    /**
     * Takes one permit from the current window.
     *
     * @return true if a permit was taken, false if the current window has none left
     */
    boolean tryAcquirePermit() {
        long window = currentWindow();
        while (true) {
            WindowUsage current = usage.get();
            int permitsUsed = current.window() == window ? current.permitsUsed() : 0;
            if (permitsUsed >= permitsPerWindow) {
                return false;
            }
            if (usage.compareAndSet(current, new WindowUsage(window, permitsUsed + 1))) {
                return true;
            }
        }
    }

    private long currentWindow() {
        return Math.floorDiv(nanoTimeSupplier.getAsLong(), windowNanos);
    }
}
