/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 *
 */
public class StoreRecoveryService extends AbstractIndexShardComponent implements Closeable {

    private final MappingUpdatedAction mappingUpdatedAction;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final TimeValue waitForMappingUpdatePostRecovery;

    private final CancellableThreads cancellableThreads = new CancellableThreads();

    private static final String SETTING_MAPPING_UPDATE_WAIT_LEGACY = "index.gateway.wait_for_mapping_update_post_recovery";
    private static final String SETTING_MAPPING_UPDATE_WAIT = "index.shard.wait_for_mapping_update_post_recovery";
    private final RestoreService restoreService;
    private final RepositoriesService repositoriesService;

    @Inject
    public StoreRecoveryService(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool,
                                MappingUpdatedAction mappingUpdatedAction, ClusterService clusterService, RepositoriesService repositoriesService, RestoreService restoreService) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.restoreService = restoreService;
        this.repositoriesService = repositoriesService;
        this.clusterService = clusterService;
        this.waitForMappingUpdatePostRecovery = indexSettings.getAsTime(SETTING_MAPPING_UPDATE_WAIT, indexSettings.getAsTime(SETTING_MAPPING_UPDATE_WAIT_LEGACY, TimeValue.timeValueSeconds(15)));
    }

    public  interface RecoveryListener {
        void onRecoveryDone();

        void onIgnoreRecovery(String reason);

        void onRecoveryFailed(IndexShardRecoveryException e);
    }

    /**
     * Recovers the state of the shard from the gateway.
     */
    public void recover(final IndexShard indexShard, final boolean indexShouldExists, final RecoveryListener listener) throws IndexShardRecoveryException {
        if (indexShard.state() == IndexShardState.CLOSED) {
            // got closed on us, just ignore this recovery
            listener.onIgnoreRecovery("shard closed");
            return;
        }
        if (!indexShard.routingEntry().primary()) {
            listener.onRecoveryFailed(new IndexShardRecoveryException(shardId, "Trying to recover when the shard is in backup state", null));
            return;
        }
        try {
            if (indexShard.routingEntry().restoreSource() != null) {
                indexShard.recovering("from snapshot", RecoveryState.Type.SNAPSHOT, indexShard.routingEntry().restoreSource());
            } else {
                indexShard.recovering("from store", RecoveryState.Type.STORE, clusterService.localNode());
            }
        } catch (IllegalIndexShardStateException e) {
            // that's fine, since we might be called concurrently, just ignore this, we are already recovering
            listener.onIgnoreRecovery("already in recovering process, " + e.getMessage());
            return;
        }

        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {

                try {
                    final RecoveryState recoveryState = indexShard.recoveryState();
                    if (indexShard.routingEntry().restoreSource() != null) {
                        logger.debug("restoring from {} ...", indexShard.routingEntry().restoreSource());
                        restore(indexShard, recoveryState);
                    } else {
                        logger.debug("starting recovery from shard_store ...");
                        recoverFromStore(indexShard, indexShouldExists, recoveryState);
                    }

                    // Check that the gateway didn't leave the shard in init or recovering stage. it is up to the gateway
                    // to call post recovery.
                    IndexShardState shardState = indexShard.state();
                    assert shardState != IndexShardState.CREATED && shardState != IndexShardState.RECOVERING : "recovery process of " + shardId + " didn't get to post_recovery. shardState [" + shardState + "]";

                    if (logger.isTraceEnabled()) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("recovery completed from ").append("shard_store").append(", took [").append(timeValueMillis(recoveryState.getTimer().time())).append("]\n");
                        RecoveryState.Index index = recoveryState.getIndex();
                        sb.append("    index    : files           [").append(index.totalFileCount()).append("] with total_size [")
                                .append(new ByteSizeValue(index.totalBytes())).append("], took[")
                                .append(TimeValue.timeValueMillis(index.time())).append("]\n");
                        sb.append("             : recovered_files [").append(index.recoveredFileCount()).append("] with total_size [")
                                .append(new ByteSizeValue(index.recoveredBytes())).append("]\n");
                        sb.append("             : reusing_files   [").append(index.reusedFileCount()).append("] with total_size [")
                                .append(new ByteSizeValue(index.reusedBytes())).append("]\n");
                        sb.append("    verify_index    : took [").append(TimeValue.timeValueMillis(recoveryState.getVerifyIndex().time())).append("], check_index [")
                                .append(timeValueMillis(recoveryState.getVerifyIndex().checkIndexTime())).append("]\n");
                        sb.append("    translog : number_of_operations [").append(recoveryState.getTranslog().recoveredOperations())
                                .append("], took [").append(TimeValue.timeValueMillis(recoveryState.getTranslog().time())).append("]");
                        logger.trace(sb.toString());
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("recovery completed from [shard_store], took [{}]", timeValueMillis(recoveryState.getTimer().time()));
                    }
                    listener.onRecoveryDone();
                } catch (IndexShardRecoveryException e) {
                    if (indexShard.state() == IndexShardState.CLOSED) {
                        // got closed on us, just ignore this recovery
                        listener.onIgnoreRecovery("shard closed");
                        return;
                    }
                    if ((e.getCause() instanceof IndexShardClosedException) || (e.getCause() instanceof IndexShardNotStartedException)) {
                        // got closed on us, just ignore this recovery
                        listener.onIgnoreRecovery("shard closed");
                        return;
                    }
                    listener.onRecoveryFailed(e);
                } catch (IndexShardClosedException e) {
                    listener.onIgnoreRecovery("shard closed");
                } catch (IndexShardNotStartedException e) {
                    listener.onIgnoreRecovery("shard closed");
                } catch (Exception e) {
                    if (indexShard.state() == IndexShardState.CLOSED) {
                        // got closed on us, just ignore this recovery
                        listener.onIgnoreRecovery("shard closed");
                        return;
                    }
                    listener.onRecoveryFailed(new IndexShardRecoveryException(shardId, "failed recovery", e));
                }
            }
        });
    }

    /**
     * Recovers the state of the shard from the store.
     */
    private void recoverFromStore(IndexShard indexShard, boolean indexShouldExists, RecoveryState recoveryState) throws IndexShardRecoveryException {
        indexShard.prepareForIndexRecovery();
        long version = -1;
        final Map<String, Mapping> typesToUpdate;
        SegmentInfos si = null;
        final Store store = indexShard.store();
        store.incRef();
        try {
            try {
                store.failIfCorrupted();
                try {
                    si = store.readLastCommittedSegmentsInfo();
                } catch (Throwable e) {
                    String files = "_unknown_";
                    try {
                        files = Arrays.toString(store.directory().listAll());
                    } catch (Throwable e1) {
                        files += " (failure=" + ExceptionsHelper.detailedMessage(e1) + ")";
                    }
                    if (indexShouldExists) {
                        throw new IndexShardRecoveryException(shardId(), "shard allocated for local recovery (post api), should exist, but doesn't, current files: " + files, e);
                    }
                }
                if (si != null) {
                    if (indexShouldExists) {
                        version = si.getVersion();
                    } else {
                        // it exists on the directory, but shouldn't exist on the FS, its a leftover (possibly dangling)
                        // its a "new index create" API, we have to do something, so better to clean it than use same data
                        logger.trace("cleaning existing shard, shouldn't exists");
                        IndexWriter writer = new IndexWriter(store.directory(), new IndexWriterConfig(Lucene.STANDARD_ANALYZER).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
                        writer.close();
                        recoveryState.getTranslog().totalOperations(0);
                    }
                }
            } catch (Throwable e) {
                throw new IndexShardRecoveryException(shardId(), "failed to fetch index version after copying it over", e);
            }
            recoveryState.getIndex().updateVersion(version);

            // since we recover from local, just fill the files and size
            try {
                final RecoveryState.Index index = recoveryState.getIndex();
                if (si != null) {
                    final Directory directory = store.directory();
                    for (String name : Lucene.files(si)) {
                        long length = directory.fileLength(name);
                        index.addFileDetail(name, length, true);
                    }
                }
            } catch (IOException e) {
                logger.debug("failed to list file details", e);
            }
            if (indexShouldExists == false) {
                recoveryState.getTranslog().totalOperations(0);
                recoveryState.getTranslog().totalOperationsOnStart(0);
            }
            typesToUpdate = indexShard.performTranslogRecovery(indexShouldExists);

            indexShard.finalizeRecovery();
            String indexName = indexShard.shardId().index().name();
            for (Map.Entry<String, Mapping> entry : typesToUpdate.entrySet()) {
                validateMappingUpdate(indexName, entry.getKey(), entry.getValue());
            }
            indexShard.postRecovery("post recovery from shard_store");
        } catch (EngineException e) {
            throw new IndexShardRecoveryException(shardId, "failed to recovery from gateway", e);
        } finally {
            store.decRef();
        }
    }

    private void validateMappingUpdate(final String indexName, final String type, Mapping update) {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        mappingUpdatedAction.updateMappingOnMaster(indexName, type, update, waitForMappingUpdatePostRecovery, new MappingUpdatedAction.MappingUpdateListener() {
            @Override
            public void onMappingUpdate() {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
                error.set(t);
            }
        });
        cancellableThreads.execute(new CancellableThreads.Interruptable() {
            @Override
            public void run() throws InterruptedException {
                try {
                    if (latch.await(waitForMappingUpdatePostRecovery.millis(), TimeUnit.MILLISECONDS) == false) {
                        logger.debug("waited for mapping update on master for [{}], yet timed out", type);
                    } else {
                        if (error.get() != null) {
                            throw new IndexShardRecoveryException(shardId, "Failed to propagate mappings on master post recovery", error.get());
                        }
                    }
                } catch (InterruptedException e) {
                    logger.debug("interrupted while waiting for mapping update");
                    throw e;
                }
            }
        });
    }

    /**
     * Restores shard from {@link RestoreSource} associated with this shard in routing table
     *
     * @param recoveryState recovery state
     */
    private void restore(final IndexShard indexShard, final RecoveryState recoveryState) {
        RestoreSource restoreSource = indexShard.routingEntry().restoreSource();
        if (restoreSource == null) {
            throw new IndexShardRestoreFailedException(shardId, "empty restore source");
        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] restoring shard  [{}]", restoreSource.snapshotId(), shardId);
        }
        try {
            recoveryState.getTranslog().totalOperations(0);
            recoveryState.getTranslog().totalOperationsOnStart(0);
            indexShard.prepareForIndexRecovery();
            IndexShardRepository indexShardRepository = repositoriesService.indexShardRepository(restoreSource.snapshotId().getRepository());
            ShardId snapshotShardId = shardId;
            if (!shardId.getIndex().equals(restoreSource.index())) {
                snapshotShardId = new ShardId(restoreSource.index(), shardId.id());
            }
            indexShardRepository.restore(restoreSource.snapshotId(), restoreSource.version(), shardId, snapshotShardId, recoveryState);
            indexShard.skipTranslogRecovery();
            indexShard.finalizeRecovery();
            indexShard.postRecovery("restore done");
            restoreService.indexShardRestoreCompleted(restoreSource.snapshotId(), shardId);
        } catch (Throwable t) {
            if (Lucene.isCorruptionException(t)) {
                restoreService.failRestore(restoreSource.snapshotId(), shardId());
            }
            throw new IndexShardRestoreFailedException(shardId, "restore failed", t);
        }
    }

    @Override
    public void close() {
        cancellableThreads.cancel("closed");
    }
}
