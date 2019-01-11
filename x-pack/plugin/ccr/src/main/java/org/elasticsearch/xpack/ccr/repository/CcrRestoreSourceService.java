/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class CcrRestoreSourceService extends AbstractLifecycleComponent implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(CcrRestoreSourceService.class);

    private final Map<String, RestoreSession> onGoingRestores = ConcurrentCollections.newConcurrentMap();
    private final Map<IndexShard, HashSet<String>> sessionsForShard = new HashMap<>();
    private final CopyOnWriteArrayList<Consumer<String>> openSessionListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<String>> closeSessionListeners = new CopyOnWriteArrayList<>();

    public CcrRestoreSourceService(Settings settings) {
        super(settings);
    }

    @Override
    public synchronized void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            HashSet<String> sessions = sessionsForShard.remove(indexShard);
            if (sessions != null) {
                for (String sessionUUID : sessions) {
                    RestoreSession restore = onGoingRestores.remove(sessionUUID);
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

    // TODO: The listeners are for testing. Once end-to-end file restore is implemented and can be tested,
    //  these should be removed.
    public void addOpenSessionListener(Consumer<String> listener) {
        openSessionListeners.add(listener);
    }

    public void addCloseSessionListener(Consumer<String> listener) {
        closeSessionListeners.add(listener);
    }

    // default visibility for testing
    synchronized HashSet<String> getSessionsForShard(IndexShard indexShard) {
        return sessionsForShard.get(indexShard);
    }

    // default visibility for testing
    synchronized RestoreSession getOngoingRestore(String sessionUUID) {
        return onGoingRestores.get(sessionUUID);
    }

    // TODO: Add a local timeout for the session. This timeout might might be for the entire session to be
    //  complete. Or it could be for session to have been touched.
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
                restore = new RestoreSession(sessionUUID, indexShard, indexShard.acquireSafeIndexCommit());
                onGoingRestores.put(sessionUUID, restore);
                openSessionListeners.forEach(c -> c.accept(sessionUUID));
                HashSet<String> sessions = sessionsForShard.computeIfAbsent(indexShard, (s) -> new HashSet<>());
                sessions.add(sessionUUID);
            }
            Store.MetadataSnapshot metaData = restore.getMetaData();
            success = true;
            return metaData;
        } finally {
            if (success == false) {
                onGoingRestores.remove(sessionUUID);
                if (restore != null) {
                    restore.decRef();
                }
            }
        }
    }

    public synchronized void closeSession(String sessionUUID) {
        closeSessionListeners.forEach(c -> c.accept(sessionUUID));
        RestoreSession restore = onGoingRestores.remove(sessionUUID);
        if (restore == null) {
            logger.info("could not close session [{}] because session not found", sessionUUID);
            throw new IllegalArgumentException("session [" + sessionUUID + "] not found");
        }
        restore.decRef();
    }

    public synchronized RestoreSession getRestoreSession(String sessionUUID) {
        RestoreSession restore = onGoingRestores.get(sessionUUID);
        if (restore == null) {
            logger.debug("could not get session [{}] because session not found", sessionUUID);
            throw new IllegalArgumentException("session [" + sessionUUID + "] not found");
        }
        restore.incRef();
        return restore;
    }

    public class RestoreSession extends AbstractRefCounted {

        private final String sessionUUID;
        private final IndexShard indexShard;
        private final Engine.IndexCommitRef commitRef;
        private volatile Tuple<String, IndexInput> cachedInput;

        private RestoreSession(String sessionUUID, IndexShard indexShard, Engine.IndexCommitRef commitRef) {
            super("restore-session");
            this.sessionUUID = sessionUUID;
            this.indexShard = indexShard;
            this.commitRef = commitRef;
        }

        private Store.MetadataSnapshot getMetaData() throws IOException {
            indexShard.store().incRef();
            try {
                return indexShard.store().getMetadata(commitRef.getIndexCommit());
            } finally {
                indexShard.store().decRef();
            }
        }

        public synchronized void readFileBytes(String fileName, BytesReference reference) throws IOException {
            // Should not access this method while holding global lock as that might block the cluster state
            // update thread on IO if it calls afterIndexShardClosed
            assert Thread.holdsLock(CcrRestoreSourceService.this) == false : "Should not hold CcrRestoreSourceService lock";
            if (cachedInput != null) {
                if (fileName.equals(cachedInput.v2()) == false) {
                    cachedInput.v2().close();
                    openNewIndexInput(fileName);
                }
            } else {
                openNewIndexInput(fileName);
            }
            BytesRefIterator refIterator = reference.iterator();
            BytesRef ref;
            IndexInput in = cachedInput.v2();
            while ((ref = refIterator.next()) != null) {
                byte[] refBytes = ref.bytes;
                in.readBytes(refBytes, 0, refBytes.length);
            }
        }

        private void openNewIndexInput(String fileName) throws IOException {
            IndexInput in = commitRef.getIndexCommit().getDirectory().openInput(fileName, IOContext.READONCE);
            cachedInput = new Tuple<>(fileName, in);
        }

        @Override
        protected void closeInternal() {
            try {
                removeSessionForShard(sessionUUID, indexShard);
            } finally {
                if (cachedInput != null) {
                    IOUtils.closeWhileHandlingException(cachedInput.v2());
                }
            }
        }

        private void removeSessionForShard(String sessionUUID, IndexShard indexShard) {
            synchronized (CcrRestoreSourceService.this) {
                logger.debug("closing session [{}] for shard [{}]", sessionUUID, indexShard.shardId());
                HashSet<String> sessions = sessionsForShard.get(indexShard);
                if (sessions != null) {
                    sessions.remove(sessionUUID);
                    if (sessions.isEmpty()) {
                        sessionsForShard.remove(indexShard);
                    }
                }
            }
        }
    }
}
