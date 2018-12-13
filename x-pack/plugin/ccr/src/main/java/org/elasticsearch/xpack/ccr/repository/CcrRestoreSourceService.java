/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
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

    private final Map<String, Engine.IndexCommitRef> onGoingRestores = ConcurrentCollections.newConcurrentMap();
    private final Map<IndexShard, HashSet<String>> sessionsForShard = new HashMap<>();
    private final CopyOnWriteArrayList<Consumer<String>> openSessionListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<String>> closeSessionListeners = new CopyOnWriteArrayList<>();

    public CcrRestoreSourceService(Settings settings) {
        super(settings);
    }

    // TODO: Need to register with IndicesService
    @Override
    public synchronized void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            logger.debug("shard [{}] closing, closing sessions", indexShard);
            HashSet<String> sessions = sessionsForShard.remove(indexShard);
            if (sessions != null) {
                for (String sessionUUID : sessions) {
                    logger.debug("closing session [{}] for shard [{}]", sessionUUID, indexShard);
                    Engine.IndexCommitRef commit = onGoingRestores.remove(sessionUUID);
                    IOUtils.closeWhileHandlingException(commit);
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
        IOUtils.closeWhileHandlingException(onGoingRestores.values());
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
    synchronized Engine.IndexCommitRef getIndexCommit(String sessionUUID) {
        return onGoingRestores.get(sessionUUID);
    }

    // TODO: Add a local timeout for the session. This timeout might might be for the entire session to be
    //  complete. Or it could be for session to have been touched.
    public synchronized Store.MetadataSnapshot openSession(String sessionUUID, IndexShard indexShard) throws IOException {
        logger.debug("opening session [{}] for shard [{}]", sessionUUID, indexShard);
        boolean success = false;
        Engine.IndexCommitRef commit = null;
        try {
            if (onGoingRestores.containsKey(sessionUUID)) {
                logger.debug("session [{}] already exists", sessionUUID);
                commit = onGoingRestores.get(sessionUUID);
            } else {
                // TODO: Add test
                if (indexShard.state() == IndexShardState.CLOSED) {
                    throw new IllegalIndexShardStateException(indexShard.shardId(), IndexShardState.CLOSED,
                        "cannot open ccr restore session if shard closed");
                }
                commit = indexShard.acquireSafeIndexCommit();
                onGoingRestores.put(sessionUUID, commit);
                openSessionListeners.forEach(c -> c.accept(sessionUUID));
                HashSet<String> sessions = sessionsForShard.computeIfAbsent(indexShard, (s) ->  new HashSet<>());
                sessions.add(sessionUUID);
            }
            indexShard.store().incRef();
            try {
                Store.MetadataSnapshot metadata = indexShard.store().getMetadata(commit.getIndexCommit());
                success = true;
                return metadata;
            } finally {
                indexShard.store().decRef();
            }
        } finally {
            if (success ==  false) {
                onGoingRestores.remove(sessionUUID);
                removeSessionForShard(sessionUUID, indexShard);
                IOUtils.closeWhileHandlingException(commit);
            }
        }
    }

    public synchronized void closeSession(String sessionUUID, IndexShard indexShard) {
        logger.debug("closing session [{}] for shard [{}]", sessionUUID, indexShard);
        closeSessionListeners.forEach(c -> c.accept(sessionUUID));
        Engine.IndexCommitRef commit = onGoingRestores.remove(sessionUUID);
        if (commit == null) {
            logger.info("could not close session [{}] for shard [{}] because session not found", sessionUUID, indexShard);
            throw new ElasticsearchException("session [" + sessionUUID + "] not found");
        }
        removeSessionForShard(sessionUUID, indexShard);
        IOUtils.closeWhileHandlingException(commit);
    }

    private void removeSessionForShard(String sessionUUID, IndexShard indexShard) {
        HashSet<String> sessions = sessionsForShard.get(indexShard);
        sessions.remove(sessionUUID);
        if (sessions.isEmpty()) {
            sessionsForShard.remove(indexShard);
        }
    }
}
