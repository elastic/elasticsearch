/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Retains {@link AcquiredSearchContexts} beyond the lifetime of the initial distributed query so a follow-up fetch
 * phase can revisit the original shard owners.
 * <p>
 * During the initial query, the data-node handler calls {@link #register} to place the session's search contexts into this registry,
 * receiving a {@link Handle} in return. Concurrent fetch requests can call {@link #acquire} to obtain another {@link Handle} that grants
 * access to the same search contexts. When an explicit release request arrives from the coordinating node, the registration is closed —
 * its reference is released — but any already-acquired handles remain valid until individually closed. The underlying search contexts are
 * released only when the last outstanding handle is closed.
 * <p>
 * <b>Concurrency design:</b> This registry uses a {@link ConcurrentHashMap} for the session map and {@link AbstractRefCounted} for
 * per-entry lifecycle. Once the registration is closed, new fetch leases are rejected while already-acquired leases remain valid until
 * individually closed. Idle registrations also expire after a short keep-alive as a backstop for abandoned coordinator sessions.
 * <p>
 * <b>Lifecycle model:</b>
 * <ul>
 *     <li><b>REGISTERED_ACTIVE</b>: after {@link #register}, producer is still active and expiry is disabled.</li>
 *     <li><b>REGISTERED_IDLE</b>: after {@link Handle#finishRegistration()}, producer is inactive and expiry is eligible when there are no
 *     outstanding leases.</li>
 *     <li><b>CLOSED_REGISTRATION</b>: after explicit close/release, new acquires are rejected but existing leases can continue.</li>
 *     <li><b>DESTROYED</b>: when the final reference is released, contexts are closed and the map entry is removed.</li>
 * </ul>
 */
final class RetainedSearchContextsRegistry {
    private static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueMinutes(5);

    private final ConcurrentHashMap<String, Entry> entriesBySessionId = new ConcurrentHashMap<>();
    private final LongSupplier relativeTimeInMillis;
    private final long keepAliveInMillis;

    RetainedSearchContextsRegistry() {
        this(System::currentTimeMillis, DEFAULT_KEEP_ALIVE);
    }

    RetainedSearchContextsRegistry(LongSupplier relativeTimeInMillis) {
        this(relativeTimeInMillis, DEFAULT_KEEP_ALIVE);
    }

    RetainedSearchContextsRegistry(LongSupplier relativeTimeInMillis, TimeValue keepAlive) {
        this.relativeTimeInMillis = relativeTimeInMillis;
        this.keepAliveInMillis = keepAlive.millis();
    }

    /**
     * Registers the given search contexts for retention under {@code sessionId}, transferring lifecycle ownership to this registry.
     * On success, the returned {@link Handle} holds one reference; closing it (or all outstanding handles) will release the contexts.
     *
     * @throws IllegalStateException if contexts are already retained for {@code sessionId}. In this case ownership is <b>not</b>
     *                               transferred — the caller remains responsible for closing {@code searchContexts}.
     */
    Handle register(String sessionId, AcquiredSearchContexts searchContexts) {
        Entry entry = new Entry(searchContexts, relativeTimeInMillis.getAsLong(), e -> entriesBySessionId.remove(sessionId, e));
        if (entriesBySessionId.putIfAbsent(sessionId, entry) != null) {
            throw new IllegalStateException("search contexts already retained for session [" + sessionId + "]");
        }
        return new Handle(
            sessionId,
            searchContexts.globalView(),
            entry::closeRegistration,
            () -> entry.finishRegistration(relativeTimeInMillis.getAsLong())
        );
    }

    Handle acquire(String sessionId) {
        Entry entry = entriesBySessionId.get(sessionId);
        long nowInMillis = relativeTimeInMillis.getAsLong();
        if (entry == null || entry.tryAcquire(nowInMillis) == false) {
            throw new IllegalStateException("no retained search contexts for session [" + sessionId + "]");
        }
        return new Handle(sessionId, entry.searchContexts.globalView(), () -> entry.closeLease(relativeTimeInMillis.getAsLong()), () -> {});
    }

    int retainedSessions() {
        return entriesBySessionId.size();
    }

    boolean isRetained(String sessionId) {
        Entry entry = entriesBySessionId.get(sessionId);
        return entry != null && entry.refs.hasReferences();
    }

    void closeRegistration(String sessionId) {
        Entry entry = entriesBySessionId.get(sessionId);
        if (entry != null) {
            entry.closeRegistration();
        }
    }

    void expire() {
        long nowInMillis = relativeTimeInMillis.getAsLong();
        entriesBySessionId.forEach((sessionId, entry) -> {
            if (entry.isExpired(nowInMillis, keepAliveInMillis)) {
                entry.closeRegistration();
            }
        });
    }

    private static final class Entry {
        private final AcquiredSearchContexts searchContexts;
        private final AbstractRefCounted refs;
        private final AtomicBoolean registrationClosed = new AtomicBoolean();
        private final AtomicBoolean producerActive = new AtomicBoolean(true);
        private final AtomicLong lastAccessTimeInMillis;

        private Entry(AcquiredSearchContexts searchContexts, long nowInMillis, Consumer<Entry> onMapRemoval) {
            this.searchContexts = searchContexts;
            this.lastAccessTimeInMillis = new AtomicLong(nowInMillis);
            this.refs = AbstractRefCounted.of(() -> {
                onMapRemoval.accept(this);
                searchContexts.close();
            });
        }

        boolean tryAcquire(long nowInMillis) {
            if (registrationClosed.get()) {
                return false;
            }
            if (refs.tryIncRef() == false) {
                return false;
            }
            if (registrationClosed.get()) {
                refs.decRef();
                return false;
            }
            lastAccessTimeInMillis.accumulateAndGet(nowInMillis, Math::max);
            return true;
        }

        void closeLease(long nowInMillis) {
            lastAccessTimeInMillis.accumulateAndGet(nowInMillis, Math::max);
            refs.decRef();
        }

        boolean isExpired(long nowInMillis, long keepAliveInMillis) {
            // Expiry is only a backstop for abandoned registrations:
            // never expire while producer is still active, while registration is already closed,
            // or while any lease remains outstanding.
            if (registrationClosed.get() || producerActive.get() || refs.refCount() > 1) {
                return false;
            }
            return nowInMillis - lastAccessTimeInMillis.get() > keepAliveInMillis;
        }

        void finishRegistration(long nowInMillis) {
            // Producer completion transitions this entry from active to idle, enabling reaper-based expiry.
            if (registrationClosed.get() == false) {
                lastAccessTimeInMillis.accumulateAndGet(nowInMillis, Math::max);
                producerActive.set(false);
            }
        }

        /**
         * Releases the registration's reference on the underlying ref count. Guarded by an {@link AtomicBoolean} so that concurrent
         * callers (task-completion listener and coordinator release request) safely converge to a single {@code decRef}.
         */
        void closeRegistration() {
            if (registrationClosed.compareAndSet(false, true)) {
                refs.decRef();
            }
        }
    }

    static final class Handle implements Releasable {
        private final String sessionId;
        private final IndexedByShardId<ComputeSearchContext> searchContexts;
        private final Runnable onClose;
        private final Runnable onFinishRegistration;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Handle(
            String sessionId,
            IndexedByShardId<ComputeSearchContext> searchContexts,
            Runnable onClose,
            Runnable onFinishRegistration
        ) {
            this.sessionId = sessionId;
            this.searchContexts = searchContexts;
            this.onClose = onClose;
            this.onFinishRegistration = onFinishRegistration;
        }

        String sessionId() {
            return sessionId;
        }

        IndexedByShardId<ComputeSearchContext> searchContexts() {
            return searchContexts;
        }

        void finishRegistration() {
            if (closed.get() == false) {
                onFinishRegistration.run();
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                onClose.run();
            }
        }
    }
}
