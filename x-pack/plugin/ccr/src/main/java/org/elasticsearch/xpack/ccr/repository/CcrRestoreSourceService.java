/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.CombinedRateLimiter;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongConsumer;

public class CcrRestoreSourceService extends AbstractLifecycleComponent implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(CcrRestoreSourceService.class);

    private final Map<String, RestoreSession> onGoingRestores = ConcurrentCollections.newConcurrentMap();
    private final Map<IndexShard, HashSet<String>> sessionsForShard = new HashMap<>();
    private final ThreadPool threadPool;
    private final CcrSettings ccrSettings;
    private final CounterMetric throttleTime = new CounterMetric();

    public CcrRestoreSourceService(ThreadPool threadPool, CcrSettings ccrSettings) {
        this.threadPool = threadPool;
        this.ccrSettings = ccrSettings;
    }

    @Override
    public synchronized void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            HashSet<String> sessions = sessionsForShard.remove(indexShard);
            if (sessions != null) {
                for (String sessionUUID : sessions) {
                    RestoreSession restore = onGoingRestores.remove(sessionUUID);
                    assert restore != null : "Session UUID [" + sessionUUID + "] registered for shard but not found in ongoing restores";
                    restore.decRef();
                }
            }
        }
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected synchronized void doClose() throws IOException {
        sessionsForShard.clear();
        onGoingRestores.values().forEach(AbstractRefCounted::decRef);
        onGoingRestores.clear();
    }

    public synchronized Store.MetadataSnapshot openSession(String sessionUUID, IndexShard indexShard) throws IOException {
        boolean success = false;
        RestoreSession restore = null;
        try {
            if (onGoingRestores.containsKey(sessionUUID)) {
                logger.debug("not opening new session [{}] as it already exists", sessionUUID);
                restore = onGoingRestores.get(sessionUUID);
            } else {
                logger.debug("opening session [{}] for shard [{}]", sessionUUID, indexShard.shardId());
                if (indexShard.state() == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(indexShard.shardId(), "cannot open ccr restore session if shard closed");
                }
                restore = new RestoreSession(sessionUUID, indexShard, indexShard.acquireSafeIndexCommit(), scheduleTimeout(sessionUUID));
                onGoingRestores.put(sessionUUID, restore);
                HashSet<String> sessions = sessionsForShard.computeIfAbsent(indexShard, (s) -> new HashSet<>());
                sessions.add(sessionUUID);
            }
            Store.MetadataSnapshot metadata = restore.getMetadata();
            success = true;
            return metadata;
        } finally {
            if (success == false) {
                onGoingRestores.remove(sessionUUID);
                if (restore != null) {
                    restore.decRef();
                }
            }
        }
    }

    public void closeSession(String sessionUUID) {
        internalCloseSession(sessionUUID, true);
    }

    public synchronized SessionReader getSessionReader(String sessionUUID) {
        RestoreSession restore = onGoingRestores.get(sessionUUID);
        if (restore == null) {
            logger.debug("could not get session [{}] because session not found", sessionUUID);
            throw new IllegalArgumentException("session [" + sessionUUID + "] not found");
        }
        restore.idle = false;
        return new SessionReader(restore, ccrSettings, throttleTime::inc);
    }

    private void internalCloseSession(String sessionUUID, boolean throwIfSessionMissing) {
        final RestoreSession restore;
        synchronized (this) {
            restore = onGoingRestores.remove(sessionUUID);
            if (restore == null) {
                if (throwIfSessionMissing) {
                    logger.debug("could not close session [{}] because session not found", sessionUUID);
                    throw new IllegalArgumentException("session [" + sessionUUID + "] not found");
                } else {
                    return;
                }
            }
            HashSet<String> sessions = sessionsForShard.get(restore.indexShard);
            assert sessions != null : "No session UUIDs for shard even though one [" + sessionUUID + "] is active in ongoing restores";
            if (sessions != null) {
                boolean removed = sessions.remove(sessionUUID);
                assert removed : "No session found for UUID [" + sessionUUID + "]";
                if (sessions.isEmpty()) {
                    sessionsForShard.remove(restore.indexShard);
                }
            }
        }
        restore.decRef();
    }

    private Scheduler.Cancellable scheduleTimeout(String sessionUUID) {
        TimeValue idleTimeout = ccrSettings.getRecoveryActivityTimeout();
        return threadPool.scheduleWithFixedDelay(() -> maybeTimeout(sessionUUID), idleTimeout, ThreadPool.Names.GENERIC);
    }

    private void maybeTimeout(String sessionUUID) {
        RestoreSession restoreSession = onGoingRestores.get(sessionUUID);
        if (restoreSession != null) {
            if (restoreSession.idle) {
                internalCloseSession(sessionUUID, false);
            } else {
                restoreSession.idle = true;
            }
        }
    }

    public long getThrottleTime() {
        return this.throttleTime.count();
    }

    private static class RestoreSession extends AbstractRefCounted {

        private final String sessionUUID;
        private final IndexShard indexShard;
        private final Engine.IndexCommitRef commitRef;
        private final Scheduler.Cancellable timeoutTask;
        private final KeyedLock<String> keyedLock = new KeyedLock<>();
        private final Map<String, IndexInput> cachedInputs = new ConcurrentHashMap<>();
        private volatile boolean idle = false;

        private RestoreSession(String sessionUUID, IndexShard indexShard, Engine.IndexCommitRef commitRef,
                               Scheduler.Cancellable timeoutTask) {
            super("restore-session");
            this.sessionUUID = sessionUUID;
            this.indexShard = indexShard;
            this.commitRef = commitRef;
            this.timeoutTask = timeoutTask;
        }

        private Store.MetadataSnapshot getMetadata() throws IOException {
            indexShard.store().incRef();
            try {
                return indexShard.store().getMetadata(commitRef.getIndexCommit());
            } finally {
                indexShard.store().decRef();
            }
        }

        private long readFileBytes(String fileName, BytesReference reference) throws IOException {
            try (Releasable ignored = keyedLock.acquire(fileName)) {
                final IndexInput indexInput = cachedInputs.computeIfAbsent(fileName, f -> {
                    try {
                        return commitRef.getIndexCommit().getDirectory().openInput(fileName, IOContext.READONCE);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

                BytesRefIterator refIterator = reference.iterator();
                BytesRef ref;
                while ((ref = refIterator.next()) != null) {
                    indexInput.readBytes(ref.bytes, ref.offset, ref.length);
                }

                long offsetAfterRead = indexInput.getFilePointer();

                if (offsetAfterRead == indexInput.length()) {
                    cachedInputs.remove(fileName);
                    IOUtils.close(indexInput);
                }

                return offsetAfterRead;
            }
        }

        @Override
        protected void closeInternal() {
            logger.debug("closing session [{}] for shard [{}]", sessionUUID, indexShard.shardId());
            assert keyedLock.hasLockedKeys() == false : "Should not hold any file locks when closing";
            timeoutTask.cancel();
            IOUtils.closeWhileHandlingException(cachedInputs.values());
            IOUtils.closeWhileHandlingException(commitRef);
        }
    }

    public static class SessionReader implements Closeable {

        private final RestoreSession restoreSession;
        private final CcrSettings ccrSettings;
        private final LongConsumer throttleListener;

        private SessionReader(RestoreSession restoreSession, CcrSettings ccrSettings, LongConsumer throttleListener) {
            this.restoreSession = restoreSession;
            this.ccrSettings = ccrSettings;
            this.throttleListener = throttleListener;
            restoreSession.incRef();
        }

        /**
         * Read bytes into the reference from the file. This method will return the offset in the file where
         * the read completed.
         *
         * @param fileName to read
         * @param reference to read bytes into
         * @return the offset of the file after the read is complete
         * @throws IOException if the read fails
         */
        public long readFileBytes(String fileName, BytesReference reference) throws IOException {
            CombinedRateLimiter rateLimiter = ccrSettings.getRateLimiter();
            long throttleTime = rateLimiter.maybePause(reference.length());
            throttleListener.accept(throttleTime);
            return restoreSession.readFileBytes(fileName, reference);
        }

        @Override
        public void close() {
            restoreSession.decRef();
        }
    }
}
