/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * The bytes of one shard's latest local commit that a snapshot into a repository still has to upload as data blobs.
 *
 * @param bytes         the total length of the commit files the repository does not hold yet, without the files in {@code inlinedBytes}
 * @param inlinedBytes  the total length of the commit files the repository does not hold yet but which a snapshot stores inside the
 *                      shard-level metadata instead of as data blobs, so they are never uploaded and not part of {@code bytes}
 */
record ShardBacklog(long bytes, long inlinedBytes) {

    /**
     * @param commitFiles     the names and lengths of the files of the shard's latest commit
     * @param repositoryFiles the files the repository already holds for the shard
     */
    static ShardBacklog of(Map<String, Long> commitFiles, RepositoryShardFiles repositoryFiles) {
        long bytes = 0;
        long inlinedBytes = 0;
        for (Map.Entry<String, Long> commitFile : commitFiles.entrySet()) {
            final String fileName = commitFile.getKey();
            final long length = commitFile.getValue();
            if (repositoryFiles.contains(fileName, length)) {
                continue;
            }
            // The small-file rule of BlobStoreRepository: a snapshot keeps the contents of these files in the shard-level metadata
            // (StoreFileMetadata#hashEqualsContents), which is decided by the file name alone.
            if (Store.MetadataSnapshot.isReadAsHash(fileName)) {
                inlinedBytes += length;
            } else {
                bytes += length;
            }
        }
        return new ShardBacklog(bytes, inlinedBytes);
    }

    /**
     * Subtracts what the running shard snapshots of this shard have already uploaded. The statuses are only taken into account while they
     * describe progress against the repository files this backlog was computed from, so that nothing is subtracted twice once the
     * repository (and with it the files we compare with) has caught up with a finished snapshot.
     *
     * @param repositoryFiles the files this backlog was computed against
     * @param statuses        the statuses of the shard snapshots known to run for this shard and repository
     */
    ShardBacklog minusRunningSnapshots(RepositoryShardFiles repositoryFiles, Collection<IndexShardSnapshotStatus> statuses) {
        long processedBytes = 0;
        for (IndexShardSnapshotStatus status : statuses) {
            final boolean basedOnSameFiles = Objects.equals(status.generation(), repositoryFiles.generation());
            final boolean countsAgainstFiles = switch (status.getStage()) {
                // the status started from the files we compare with, so its progress is progress against this backlog
                case STARTED, FINALIZE -> basedOnSameFiles;
                // the shard-level metadata is written but the repository data does not point at it yet: until it does, the uploaded
                // files are still missing from the files we compare with
                case DONE -> basedOnSameFiles == false;
                // nothing uploaded yet, or an unsuccessful snapshot whose uploads are not reused
                default -> false;
            };
            if (countsAgainstFiles) {
                processedBytes += status.asCopy().getProcessedSize();
            }
        }
        // The processed size also contains the files that go into the shard-level metadata, which are not in this backlog.
        final long uploadedBytes = Math.max(0, processedBytes - inlinedBytes);
        return new ShardBacklog(Math.max(0, bytes - uploadedBytes), inlinedBytes);
    }
}
