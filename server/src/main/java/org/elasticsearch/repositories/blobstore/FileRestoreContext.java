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
package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * This context will execute a file restore of the lucene files. It is primarily designed to be used to
 * restore from some form of a snapshot. It will setup a new store, identify files that need to be copied
 * for the source, and perform the copies. Implementers must implement the functionality of opening the
 * underlying file streams for snapshotted lucene file.
 */
public abstract class FileRestoreContext {

    protected static final Logger logger = LogManager.getLogger(FileRestoreContext.class);

    protected final String repositoryName;
    protected final RecoveryState recoveryState;
    protected final SnapshotId snapshotId;
    protected final ShardId shardId;
    protected final int bufferSize;

    /**
     * Constructs new restore context
     *
     * @param shardId       shard id to restore into
     * @param snapshotId    snapshot id
     * @param recoveryState recovery state to report progress
     * @param bufferSize    buffer size for restore
     */
    protected FileRestoreContext(String repositoryName, ShardId shardId, SnapshotId snapshotId, RecoveryState recoveryState,
                                 int bufferSize) {
        this.repositoryName = repositoryName;
        this.recoveryState = recoveryState;
        this.snapshotId = snapshotId;
        this.shardId = shardId;
        this.bufferSize = bufferSize;
    }

    /**
     * Performs restore operation
     */
    public void restore(SnapshotFiles snapshotFiles, Store store) throws IOException {
        store.incRef();
        try {
            logger.debug("[{}] [{}] restoring to [{}] ...", snapshotId, repositoryName, shardId);

            if (snapshotFiles.indexFiles().size() == 1
                && snapshotFiles.indexFiles().get(0).physicalName().startsWith("segments_")
                && snapshotFiles.indexFiles().get(0).hasUnknownChecksum()) {
                // If the shard has no documents, it will only contain a single segments_N file for the
                // shard's snapshot.  If we are restoring a snapshot created by a previous supported version,
                // it is still possible that in that version, an empty shard has a segments_N file with an unsupported
                // version (and no checksum), because we don't know the Lucene version to assign segments_N until we
                // have written some data.  Since the segments_N for an empty shard could have an incompatible Lucene
                // version number and no checksum, even though the index itself is perfectly fine to restore, this
                // empty shard would cause exceptions to be thrown.  Since there is no data to restore from an empty
                // shard anyway, we just create the empty shard here and then exit.
                store.createEmpty(store.indexSettings().getIndexVersionCreated().luceneVersion);
                return;
            }

            Store.MetadataSnapshot recoveryTargetMetadata;
            try {
                // this will throw an IOException if the store has no segments infos file. The
                // store can still have existing files but they will be deleted just before being
                // restored.
                recoveryTargetMetadata = store.getMetadata(null, true);
            } catch (org.apache.lucene.index.IndexNotFoundException e) {
                // happens when restore to an empty shard, not a big deal
                logger.trace("[{}] [{}] restoring from to an empty shard", shardId, snapshotId);
                recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
            } catch (IOException e) {
                logger.warn(new ParameterizedMessage("[{}] [{}] Can't read metadata from store, will not reuse local files during restore",
                    shardId, snapshotId), e);
                recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
            }
            final List<BlobStoreIndexShardSnapshot.FileInfo> filesToRecover = new ArrayList<>();
            final Map<String, StoreFileMetaData> snapshotMetaData = new HashMap<>();
            final Map<String, BlobStoreIndexShardSnapshot.FileInfo> fileInfos = new HashMap<>();
            for (final BlobStoreIndexShardSnapshot.FileInfo fileInfo : snapshotFiles.indexFiles()) {
                snapshotMetaData.put(fileInfo.metadata().name(), fileInfo.metadata());
                fileInfos.put(fileInfo.metadata().name(), fileInfo);
            }

            final Store.MetadataSnapshot sourceMetaData = new Store.MetadataSnapshot(unmodifiableMap(snapshotMetaData), emptyMap(), 0);

            final StoreFileMetaData restoredSegmentsFile = sourceMetaData.getSegmentsFile();
            if (restoredSegmentsFile == null) {
                throw new IndexShardRestoreFailedException(shardId, "Snapshot has no segments file");
            }

            final Store.RecoveryDiff diff = sourceMetaData.recoveryDiff(recoveryTargetMetadata);
            for (StoreFileMetaData md : diff.identical) {
                BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfos.get(md.name());
                recoveryState.getIndex().addFileDetail(fileInfo.physicalName(), fileInfo.length(), true);
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] [{}] not_recovering file [{}] from [{}], exists in local store and is same", shardId, snapshotId,
                        fileInfo.physicalName(), fileInfo.name());
                }
            }

            for (StoreFileMetaData md : concat(diff)) {
                BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfos.get(md.name());
                filesToRecover.add(fileInfo);
                recoveryState.getIndex().addFileDetail(fileInfo.physicalName(), fileInfo.length(), false);
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] [{}] recovering [{}] from [{}]", shardId, snapshotId,
                        fileInfo.physicalName(), fileInfo.name());
                }
            }

            if (filesToRecover.isEmpty()) {
                logger.trace("[{}] [{}] no files to recover, all exist within the local store", shardId, snapshotId);
            }

            try {
                // list of all existing store files
                final List<String> deleteIfExistFiles = Arrays.asList(store.directory().listAll());

                for (final BlobStoreIndexShardSnapshot.FileInfo fileToRecover : filesToRecover) {
                    // if a file with a same physical name already exist in the store we need to delete it
                    // before restoring it from the snapshot. We could be lenient and try to reuse the existing
                    // store files (and compare their names/length/checksum again with the snapshot files) but to
                    // avoid extra complexity we simply delete them and restore them again like StoreRecovery
                    // does with dangling indices. Any existing store file that is not restored from the snapshot
                    // will be clean up by RecoveryTarget.cleanFiles().
                    final String physicalName = fileToRecover.physicalName();
                    if (deleteIfExistFiles.contains(physicalName)) {
                        logger.trace("[{}] [{}] deleting pre-existing file [{}]", shardId, snapshotId, physicalName);
                        store.directory().deleteFile(physicalName);
                    }
                }

                restoreFiles(filesToRecover, store);
            } catch (IOException ex) {
                throw new IndexShardRestoreFailedException(shardId, "Failed to recover index", ex);
            }

            // read the snapshot data persisted
            try {
                Lucene.pruneUnreferencedFiles(restoredSegmentsFile.name(), store.directory());
            } catch (IOException e) {
                throw new IndexShardRestoreFailedException(shardId, "Failed to fetch index version after copying it over", e);
            }

            /// now, go over and clean files that are in the store, but were not in the snapshot
            try {
                for (String storeFile : store.directory().listAll()) {
                    if (Store.isAutogenerated(storeFile) || snapshotFiles.containPhysicalIndexFile(storeFile)) {
                        continue; //skip write.lock, checksum files and files that exist in the snapshot
                    }
                    try {
                        store.deleteQuiet("restore", storeFile);
                        store.directory().deleteFile(storeFile);
                    } catch (IOException e) {
                        logger.warn("[{}] [{}] failed to delete file [{}] during snapshot cleanup", shardId, snapshotId, storeFile);
                    }
                }
            } catch (IOException e) {
                logger.warn("[{}] [{}] failed to list directory - some of files might not be deleted", shardId, snapshotId);
            }
        } finally {
            store.decRef();
        }
    }

    protected void restoreFiles(List<BlobStoreIndexShardSnapshot.FileInfo> filesToRecover, Store store) throws IOException {
        // restore the files from the snapshot to the Lucene store
        for (final BlobStoreIndexShardSnapshot.FileInfo fileToRecover : filesToRecover) {
            logger.trace("[{}] [{}] restoring file [{}]", shardId, snapshotId, fileToRecover.name());
            restoreFile(fileToRecover, store);
        }
    }

    protected abstract InputStream fileInputStream(BlobStoreIndexShardSnapshot.FileInfo fileInfo);

    @SuppressWarnings("unchecked")
    private static Iterable<StoreFileMetaData> concat(Store.RecoveryDiff diff) {
        return Iterables.concat(diff.different, diff.missing);
    }

    /**
     * Restores a file
     *
     * @param fileInfo file to be restored
     */
    private void restoreFile(final BlobStoreIndexShardSnapshot.FileInfo fileInfo, final Store store) throws IOException {
        boolean success = false;

        try (InputStream stream = fileInputStream(fileInfo)) {
            try (IndexOutput indexOutput = store.createVerifyingOutput(fileInfo.physicalName(), fileInfo.metadata(), IOContext.DEFAULT)) {
                final byte[] buffer = new byte[bufferSize];
                int length;
                while ((length = stream.read(buffer)) > 0) {
                    indexOutput.writeBytes(buffer, 0, length);
                    recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.physicalName(), length);
                }
                Store.verify(indexOutput);
                indexOutput.close();
                store.directory().sync(Collections.singleton(fileInfo.physicalName()));
                success = true;
            } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                try {
                    store.markStoreCorrupted(ex);
                } catch (IOException e) {
                    logger.warn("store cannot be marked as corrupted", e);
                }
                throw ex;
            } finally {
                if (success == false) {
                    store.deleteQuiet(fileInfo.physicalName());
                }
            }
        }
    }

}
