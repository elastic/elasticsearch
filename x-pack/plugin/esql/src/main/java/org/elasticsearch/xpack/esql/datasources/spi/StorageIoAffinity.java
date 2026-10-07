/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Thread-local row-group lease for the calling thread of a storage GET. The query-budget wrapper
 * reads {@link #current()} on {@code startReadBytesAsync} / {@code readBytes} before handing off
 * to the SDK. SDK completion callbacks must not call {@link #current()} — they run on another
 * thread and must use the lease captured in the permit token instead.
 * <p>
 * Nested scopes restore the outer scope on close. {@link ThreadLocal#remove()} runs only when
 * there was no outer scope.
 */
public final class StorageIoAffinity {

    private static final ThreadLocal<Scope> CURRENT = new ThreadLocal<>();

    private StorageIoAffinity() {}

    /**
     * One installed affinity. {@link #countGets} is true for async row-group GETs that should
     * move the lease's outstanding counter; false for sync fallback and for callers that only
     * need grant identity.
     */
    public static final class Scope implements AutoCloseable {
        private final RowGroupIo lease;
        public final boolean countGets;
        private final Scope previous;
        private boolean closed;

        private Scope(RowGroupIo lease, boolean countGets, Scope previous) {
            this.lease = lease;
            this.countGets = countGets;
            this.previous = previous;
        }

        public RowGroupIo lease() {
            return lease;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            restore(previous);
        }
    }

    /**
     * Installs {@code lease} as the current thread's affinity, saving any outer scope to restore
     * on {@link Scope#close()}.
     */
    public static Scope open(RowGroupIo lease, boolean countGets) {
        Scope previous = CURRENT.get();
        Scope scope = new Scope(lease, countGets, previous);
        CURRENT.set(scope);
        return scope;
    }

    /**
     * The innermost open scope on this thread, or {@code null}. Must not be read from SDK
     * completion callbacks.
     */
    public static Scope current() {
        return CURRENT.get();
    }

    private static void restore(Scope previous) {
        if (previous == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(previous);
        }
    }
}
