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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Retains {@link AcquiredSearchContexts} beyond the lifetime of the initial distributed query so a follow-up fetch
 * phase can revisit the original shard owners.
 * <p>
 * During the initial query, the data-node handler calls {@link #register} to place the session's search contexts into this registry,
 * receiving a {@link Handle} in return. Concurrent fetch requests can call {@link #acquire} to obtain another {@link Handle} that grants
 * access to the same search contexts. When the query task completes (or an explicit release request arrives from the coordinating node),
 * the registration is closed — its reference is released — but any already-acquired handles remain valid until individually closed.
 * The underlying search contexts are released only when the last outstanding handle is closed.
 * <p>
 * <b>Concurrency design:</b> This registry uses a {@link ConcurrentHashMap} for the session map and {@link AbstractRefCounted} for
 * per-entry lifecycle. There is no coarse-grained synchronization. A consequence is that {@link #acquire} may succeed during the brief
 * window between a coordinator sending its release signal and that signal being processed (i.e., a "straggler" acquire). This is
 * acceptable: a straggler operates on still-valid contexts (guaranteed by {@code tryIncRef}), completes its fetch, and its eventual
 * close triggers final cleanup. The alternative — a strict gate preventing all post-release acquires — would require synchronized
 * compound operations but provides no correctness benefit, only avoiding a small amount of wasted work in an unlikely race.
 */
final class RetainedSearchContextsRegistry {
    private final ConcurrentHashMap<String, Entry> entriesBySessionId = new ConcurrentHashMap<>();

    /**
     * Registers the given search contexts for retention under {@code sessionId}, transferring lifecycle ownership to this registry.
     * On success, the returned {@link Handle} holds one reference; closing it (or all outstanding handles) will release the contexts.
     *
     * @throws IllegalStateException if contexts are already retained for {@code sessionId}. In this case ownership is <b>not</b>
     *                               transferred — the caller remains responsible for closing {@code searchContexts}.
     */
    Handle register(String sessionId, AcquiredSearchContexts searchContexts) {
        Entry entry = new Entry(searchContexts, e -> entriesBySessionId.remove(sessionId, e));
        if (entriesBySessionId.putIfAbsent(sessionId, entry) != null) {
            throw new IllegalStateException("search contexts already retained for session [" + sessionId + "]");
        }
        return new Handle(sessionId, searchContexts.globalView(), entry::closeRegistration);
    }

    Handle acquire(String sessionId) {
        Entry entry = entriesBySessionId.get(sessionId);
        if (entry == null || entry.refs.tryIncRef() == false) {
            throw new IllegalStateException("no retained search contexts for session [" + sessionId + "]");
        }
        return new Handle(sessionId, entry.searchContexts.globalView(), entry.refs::decRef);
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

    private static final class Entry {
        private final AcquiredSearchContexts searchContexts;
        private final AbstractRefCounted refs;
        private final AtomicBoolean registrationClosed = new AtomicBoolean();

        private Entry(AcquiredSearchContexts searchContexts, Consumer<Entry> onMapRemoval) {
            this.searchContexts = searchContexts;
            this.refs = AbstractRefCounted.of(() -> {
                onMapRemoval.accept(this);
                searchContexts.close();
            });
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
        private final AtomicBoolean closed = new AtomicBoolean();

        private Handle(String sessionId, IndexedByShardId<ComputeSearchContext> searchContexts, Runnable onClose) {
            this.sessionId = sessionId;
            this.searchContexts = searchContexts;
            this.onClose = onClose;
        }

        String sessionId() {
            return sessionId;
        }

        IndexedByShardId<ComputeSearchContext> searchContexts() {
            return searchContexts;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                onClose.run();
            }
        }
    }
}
