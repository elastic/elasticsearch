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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

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
     * @return  <code>true</code> if the shard has been recovered successfully, <code>false</code> if the recovery
     * has been ignored due to a concurrent modification of if the clusters state has changed due to async updates.
     * @see Store
     */
    boolean recoverFromStore(final IndexShard indexShard, final boolean indexShouldExists) {
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

    boolean recoverFromLocalShards(BiConsumer<String, MappingMetaData> mappingUpdateConsumer, final IndexShard indexShard, final List<LocalShardSnapshot> shards) throws IOException {
        if (canRecover(indexShard)) {
            assert indexShard.recoveryState().getType() == RecoveryState.Type.LOCAL_SHARDS : "invalid recovery type: " + indexShard.recoveryState().getType();
            if (indexShard.routingEntry().restoreSource() != null) {
                throw new IllegalStateException("can't recover - restore source is not null");
            }
            if (shards.isEmpty()) {
                throw new IllegalArgumentException("shards must not be empty");
            }
            Set<Index> indices = shards.stream().map((s) -> s.getIndex()).collect(Collectors.toSet());
            if (indices.size() > 1) {
                throw new IllegalArgumentException("can't add shards from more than one index");
            }
            for (ObjectObjectCursor<String, MappingMetaData> mapping : shards.get(0).getMappings()) {
                mappingUpdateConsumer.accept(mapping.key, mapping.value);
            }
            for (ObjectObjectCursor<String, MappingMetaData> mapping : shards.get(0).getMappings()) {
                indexShard.mapperService().merge(mapping.key,mapping.value.source(), MapperService.MergeReason.MAPPING_RECOVERY, true);
            }
            return executeRecovery(indexShard, () -> {
                logger.debug("starting recovery from local shards {}", shards);
                try {
                    final Directory directory = indexShard.store().directory(); // don't close this directory!!
                    addIndices(indexShard.recoveryState().getIndex(), directory, shards.stream().map(s -> s.getSnapshotDirectory())
                        .collect(Collectors.toList()).toArray(new Directory[shards.size()]));
                    internalRecoverFromStore(indexShard, true);
                    // just trigger a merge to do housekeeping on the
                    // copied segments - we will also see them in stats etc.
                    indexShard.getEngine().forceMerge(false, -1, false, false, false);
                } catch (IOException ex) {
                    throw new IndexShardRecoveryException(indexShard.shardId(), "failed to recover from local shards", ex);
                }

            });
        }
        return false;
    }

    void addIndices(RecoveryState.Index indexRecoveryStats, Directory target, Directory... sources) throws IOException {
        target = new org.apache.lucene.store.HardlinkCopyDirectoryWrapper(target);
        try (IndexWriter writer = new IndexWriter(new StatsDirectoryWrapper(target, indexRecoveryStats),
            new IndexWriterConfig(null)
                .setCommitOnClose(false)
                // we don't want merges to happen here - we call maybe merge on the engine
                // later once we stared it up otherwise we would need to wait for it here
                // we also don't specify a codec here and merges should use the engines for this index
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE))) {
            writer.addIndexes(sources);
            writer.commit();
        }
    }

    /**
     * Directory wrapper that records copy process for recovery statistics
     */
    static final class StatsDirectoryWrapper extends FilterDirectory {
        private final RecoveryState.Index index;

        StatsDirectoryWrapper(Directory in, RecoveryState.Index indexRecoveryStats) {
            super(in);
            this.index = indexRecoveryStats;
        }

        @Override
        public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
            final long l = from.fileLength(src);
            final AtomicBoolean copies = new AtomicBoolean(false);
            // here we wrap the index input form the source directory to report progress of file copy for the recovery stats.
            // we increment the num bytes recovered in the readBytes method below, if users pull statistics they can see immediately
            // how much has been recovered.
            in.copyFrom(new FilterDirectory(from) {
                @Override
                public IndexInput openInput(String name, IOContext context) throws IOException {
                    index.addFileDetail(dest, l, false);
                    copies.set(true);
                    final IndexInput input = in.openInput(name, context);
                    return new IndexInput("StatsDirectoryWrapper(" + input.toString() + ")") {
                        @Override
                        public void close() throws IOException {
                            input.close();
                        }

                        @Override
                        public long getFilePointer() {
                            throw new UnsupportedOperationException("only straight copies are supported");
                        }

                        @Override
                        public void seek(long pos) throws IOException {
                            throw new UnsupportedOperationException("seeks are not supported");
                        }

                        @Override
                        public long length() {
                            return input.length();
                        }

                        @Override
                        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                            throw new UnsupportedOperationException("slices are not supported");
                        }

                        @Override
                        public byte readByte() throws IOException {
                           throw new UnsupportedOperationException("use a buffer if you wanna perform well");
                        }

                        @Override
                        public void readBytes(byte[] b, int offset, int len) throws IOException {
                            // we rely on the fact that copyFrom uses a buffer
                            input.readBytes(b, offset, len);
                            index.addRecoveredBytesToFile(dest, len);
                        }
                    };
                }
            }, src, dest, context);
            if (copies.get() == false) {
                index.addFileDetail(dest, l, true); // hardlinked - we treat it as reused since the file was already somewhat there
            } else {
                assert index.getFileDetails(dest) != null : "File [" + dest + "] has no file details";
                assert index.getFileDetails(dest).recovered() == l : index.getFileDetails(dest).toString();
            }
        }
    }

    /**
     * Recovers an index from a given {@link Repository}. This method restores a
     * previously created index snapshot into an existing initializing shard.
     * @param indexShard the index shard instance to recovery the snapshot from
     * @param repository the repository holding the physical files the shard should be recovered from
     * @return <code>true</code> if the shard has been recovered successfully, <code>false</code> if the recovery
     * has been ignored due to a concurrent modification of if the clusters state has changed due to async updates.
     */
    boolean recoverFromRepository(final IndexShard indexShard, Repository repository) {
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
                RecoveryState.Index index = recoveryState.getIndex();
                StringBuilder sb = new StringBuilder();
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
                logger.trace("recovery completed from [shard_store], took [{}]\n{}", timeValueMillis(recoveryState.getTimer().time()), sb);
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
                } catch (Exception e) {
                    String files = "_unknown_";
                    try {
                        files = Arrays.toString(store.directory().listAll());
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        files += " (failure=" + ExceptionsHelper.detailedMessage(inner) + ")";
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
                        Lucene.cleanLuceneIndex(store.directory());
                    }
                }
            } catch (Exception e) {
                throw new IndexShardRecoveryException(shardId, "failed to fetch index version after copying it over", e);
            }
            recoveryState.getIndex().updateVersion(version);
            // since we recover from local, just fill the files and size
            try {
                final RecoveryState.Index index = recoveryState.getIndex();
                if (si != null && recoveryState.getType() == RecoveryState.Type.STORE) {
                    addRecoveredFileDetails(si, store, index);
                }
            } catch (IOException e) {
                logger.debug("failed to list file details", e);
            }
            if (recoveryState.getType() == RecoveryState.Type.LOCAL_SHARDS) {
                assert indexShouldExists;
                indexShard.skipTranslogRecovery();
            } else {
                indexShard.performTranslogRecovery(indexShouldExists);
            }
            indexShard.finalizeRecovery();
            indexShard.postRecovery("post recovery from shard_store");
        } catch (EngineException | IOException e) {
            throw new IndexShardRecoveryException(shardId, "failed to recovery from gateway", e);
        } finally {
            store.decRef();
        }
    }

    private void addRecoveredFileDetails(SegmentInfos si, Store store, RecoveryState.Index index) throws IOException {
        final Directory directory = store.directory();
        for (String name : Lucene.files(si)) {
            long length = directory.fileLength(name);
            index.addFileDetail(name, length, true);
        }
    }

    /**
     * Restores shard from {@link RestoreSource} associated with this shard in routing table
     */
    private void restore(final IndexShard indexShard, final Repository repository) {
        RestoreSource restoreSource = indexShard.routingEntry().restoreSource();
        final RecoveryState.Translog translogState = indexShard.recoveryState().getTranslog();
        if (restoreSource == null) {
            throw new IndexShardRestoreFailedException(shardId, "empty restore source");
        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] restoring shard [{}]", restoreSource.snapshot(), shardId);
        }
        try {
            translogState.totalOperations(0);
            translogState.totalOperationsOnStart(0);
            indexShard.prepareForIndexRecovery();
            ShardId snapshotShardId = shardId;
            final String indexName = restoreSource.index();
            if (!shardId.getIndexName().equals(indexName)) {
                snapshotShardId = new ShardId(indexName, IndexMetaData.INDEX_UUID_NA_VALUE, shardId.id());
            }
            final IndexId indexId = repository.getRepositoryData().resolveIndexId(indexName);
            repository.restoreShard(indexShard, restoreSource.snapshot().getSnapshotId(), restoreSource.version(), indexId, snapshotShardId, indexShard.recoveryState());
            indexShard.skipTranslogRecovery();
            indexShard.finalizeRecovery();
            indexShard.postRecovery("restore done");
        } catch (Exception e) {
            throw new IndexShardRestoreFailedException(shardId, "restore failed", e);
        }
    }

}
