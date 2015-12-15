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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * This package private utility class encapsulates the logic to recover an index shard from either an existing index on
 * disk or from a snapshot in a repository.
 */
final class StoreRecovery {

    private final ESLogger logger;
    private final ShardId shardId;

    StoreRecovery(ShardId shardId, ESLogger logger) {
        this.logger = logger;
        this.shardId = shardId;
    }

    /**
     * Recovers a shard from it's local file system store. This method required pre-knowledge about if the shard should
     * exist on disk ie. has been previously allocated or if the shard is a brand new allocation without pre-existing index
     * files / transaction logs. This
     * @param indexShard the index shard instance to recovery the shard into
     * @param indexShouldExists <code>true</code> iff the index should exist on disk ie. has the shard been allocated previously on the shards store.
     * @param localNode the reference to the local node
     * @return  <code>true</code> if the the shard has been recovered successfully, <code>false</code> if the recovery
     * has been ignored due to a concurrent modification of if the clusters state has changed due to async updates.
     * @see Store
     */
    boolean recoverFromStore(final IndexShard indexShard, final boolean indexShouldExists, DiscoveryNode localNode) {
        if (canRecover(indexShard)) {
            if (indexShard.routingEntry().restoreSource() != null) {
                throw new IllegalStateException("can't recover - restore source is not null");
            }
            return executeRecovery(indexShard, () -> {
                logger.debug("starting recovery from store ...");
                internalRecoverFromStore(indexShard, indexShouldExists);
            });
        }
        return false;
    }

    /**
     * Recovers an index from a given {@link IndexShardRepository}. This method restores a
     * previously created index snapshot into an existing initializing shard.
     * @param indexShard the index shard instance to recovery the snapshot from
     * @param repository the repository holding the physical files the shard should be recovered from
     * @return <code>true</code> if the the shard has been recovered successfully, <code>false</code> if the recovery
     * has been ignored due to a concurrent modification of if the clusters state has changed due to async updates.
     */
    boolean recoverFromRepository(final IndexShard indexShard, IndexShardRepository repository, DiscoveryNode localNode) {
        if (canRecover(indexShard)) {
            final ShardRouting shardRouting = indexShard.routingEntry();
            if (shardRouting.restoreSource() == null) {
                throw new IllegalStateException("can't restore - restore source is null");
            }
            return executeRecovery(indexShard, () -> {
                logger.debug("restoring from {} ...", shardRouting.restoreSource());
                restore(indexShard, repository);
            });
        }
        return false;

    }

    private boolean canRecover(IndexShard indexShard) {
        if (indexShard.state() == IndexShardState.CLOSED) {
            // got closed on us, just ignore this recovery
            return false;
        }
        if (!indexShard.routingEntry().primary()) {
            throw new IndexShardRecoveryException(shardId, "Trying to recover when the shard is in backup state", null);
        }
        return true;
    }

    /**
     * Recovers the state of the shard from the store.
     */
    private boolean executeRecovery(final IndexShard indexShard, Runnable recoveryRunnable) throws IndexShardRecoveryException {
        try {
            recoveryRunnable.run();
            // Check that the gateway didn't leave the shard in init or recovering stage. it is up to the gateway
            // to call post recovery.
            final IndexShardState shardState = indexShard.state();
            final RecoveryState recoveryState = indexShard.recoveryState();
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
            return true;
        } catch (IndexShardRecoveryException e) {
            if (indexShard.state() == IndexShardState.CLOSED) {
                // got closed on us, just ignore this recovery
                return false;
            }
            if ((e.getCause() instanceof IndexShardClosedException) || (e.getCause() instanceof IndexShardNotStartedException)) {
                // got closed on us, just ignore this recovery
                return false;
            }
            throw e;
        } catch (IndexShardClosedException | IndexShardNotStartedException e) {
        } catch (Exception e) {
            if (indexShard.state() == IndexShardState.CLOSED) {
                // got closed on us, just ignore this recovery
                return false;
            }
            throw new IndexShardRecoveryException(shardId, "failed recovery", e);
        }
        return false;
    }

    /**
     * Recovers the state of the shard from the store.
     */
    private void internalRecoverFromStore(IndexShard indexShard, boolean indexShouldExists) throws IndexShardRecoveryException {
        final RecoveryState recoveryState = indexShard.recoveryState();
        indexShard.prepareForIndexRecovery();
        long version = -1;
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
                        throw new IndexShardRecoveryException(shardId, "shard allocated for local recovery (post api), should exist, but doesn't, current files: " + files, e);
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
                throw new IndexShardRecoveryException(shardId, "failed to fetch index version after copying it over", e);
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
            indexShard.performTranslogRecovery(indexShouldExists);
            indexShard.finalizeRecovery();
            indexShard.postRecovery("post recovery from shard_store");
        } catch (EngineException e) {
            throw new IndexShardRecoveryException(shardId, "failed to recovery from gateway", e);
        } finally {
            store.decRef();
        }
    }

    /**
     * Restores shard from {@link RestoreSource} associated with this shard in routing table
     */
    private void restore(final IndexShard indexShard, final IndexShardRepository indexShardRepository) {
        RestoreSource restoreSource = indexShard.routingEntry().restoreSource();
        final RecoveryState.Translog translogState = indexShard.recoveryState().getTranslog();
        if (restoreSource == null) {
            throw new IndexShardRestoreFailedException(shardId, "empty restore source");
        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] restoring shard [{}]", restoreSource.snapshotId(), shardId);
        }
        try {
            translogState.totalOperations(0);
            translogState.totalOperationsOnStart(0);
            indexShard.prepareForIndexRecovery();
            ShardId snapshotShardId = shardId;
            if (!shardId.getIndex().equals(restoreSource.index())) {
                snapshotShardId = new ShardId(restoreSource.index(), shardId.id());
            }
            indexShardRepository.restore(restoreSource.snapshotId(), restoreSource.version(), shardId, snapshotShardId, indexShard.recoveryState());
            indexShard.skipTranslogRecovery();
            indexShard.finalizeRecovery();
            indexShard.postRecovery("restore done");
        } catch (Throwable t) {
            throw new IndexShardRestoreFailedException(shardId, "restore failed", t);
        }
    }
}
