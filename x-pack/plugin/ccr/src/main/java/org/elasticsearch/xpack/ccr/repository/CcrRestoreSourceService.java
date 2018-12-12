/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CcrRestoreSourceService extends AbstractLifecycleComponent implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(CcrRestoreSourceService.class);

    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final Map<String, Engine.IndexCommitRef> onGoingRestores = ConcurrentCollections.newConcurrentMap();
    private final Map<IndexShard, HashSet<String>> sessionsForShard = new HashMap<>();

    public CcrRestoreSourceService(Settings settings) {
        super(settings);
    }

    @Override
    public synchronized void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            HashSet<String> sessions = sessionsForShard.remove(indexShard);
            if (sessions != null) {
                for (String sessionUUID : sessions) {
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
    protected void doClose() throws IOException {
        if (isOpen.compareAndSet(true, false)) {
            IOUtils.closeWhileHandlingException(onGoingRestores.values());
        }
    }

    public synchronized Store.MetadataSnapshot openSession(String sessionUUID, IndexShard indexShard) throws IOException {
        Engine.IndexCommitRef commit = indexShard.acquireSafeIndexCommit();
        onGoingRestores.put(sessionUUID, commit);
        indexShard.store().incRef();
        try {
            return indexShard.store().getMetadata(commit.getIndexCommit());
        } finally {
            indexShard.store().decRef();
        }
    }

    public synchronized void closeSession(String sessionUUID, IndexShard indexShard) {
        Engine.IndexCommitRef commit = onGoingRestores.get(sessionUUID);
        if (commit == null) {
            throw new ElasticsearchException("commit for [" + sessionUUID + "] not found");
        }
        IOUtils.closeWhileHandlingException(commit);
        HashSet<String> sessions = sessionsForShard.get(indexShard);
        sessions.remove(sessionUUID);
        if (sessions.isEmpty()) {
            sessionsForShard.remove(indexShard);
        }
    }
}
