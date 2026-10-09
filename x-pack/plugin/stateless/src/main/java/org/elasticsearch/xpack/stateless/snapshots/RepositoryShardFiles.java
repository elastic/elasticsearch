/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;

import java.util.HashSet;
import java.util.Set;

/**
 * The files of one shard that a repository already holds, as listed by the shard's latest shard-level metadata blob
 * ({@code index-{generation}}, see {@link BlobStoreIndexShardSnapshots}). A file is identified by its physical name and its length. This is
 * the identity a snapshot uses to reuse a file, minus the checksum, which is not needed to estimate bytes.
 *
 * @param generation the shard generation this list was read from, or {@link ShardGenerations#NEW_SHARD_GEN} if the repository has no
 *                   shard-level metadata for the shard
 * @param files      the files the repository holds
 */
record RepositoryShardFiles(ShardGeneration generation, Set<FileKey> files) {

    record FileKey(String physicalName, long length) {}

    /**
     * What a repository holds of a shard it has no shard-level metadata for, e.g. a new index or a split child: nothing.
     */
    static final RepositoryShardFiles NONE = new RepositoryShardFiles(ShardGenerations.NEW_SHARD_GEN, Set.of());

    static RepositoryShardFiles of(ShardGeneration generation, BlobStoreIndexShardSnapshots shardSnapshots) {
        final Set<FileKey> files = new HashSet<>();
        for (SnapshotFiles snapshotFiles : shardSnapshots.snapshots()) {
            snapshotFiles.indexFiles().forEach(fileInfo -> files.add(new FileKey(fileInfo.physicalName(), fileInfo.length())));
        }
        return new RepositoryShardFiles(generation, Set.copyOf(files));
    }

    boolean contains(String physicalName, long length) {
        return files.contains(new FileKey(physicalName, length));
    }
}
