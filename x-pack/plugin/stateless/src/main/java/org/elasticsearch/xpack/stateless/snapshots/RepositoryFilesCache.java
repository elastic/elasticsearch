/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * What one repository holds of the shards on this node, kept up to date without ever guessing.
 * <p>
 * The repository's latest shard generations come from its {@link RepositoryData}, which is reloaded when the repository generation in the
 * cluster state changes (a snapshot finished, or one was deleted). The file list of a shard ({@code index-{generation}}) is read again
 * only for shards whose generation changed. Until that read is done the previously cached list is still returned, which is a little
 * stale but close, so the reported backlog does not jump just because the repository changed. Only a shard that has never been read on
 * this node, because it just arrived or this node just started, has an unknown list: {@link #getShardFiles} returns {@code null} for
 * it and starts the read, instead of pretending the repository holds nothing or everything.
 * <p>
 * All repository reads run on the executor given to the constructor, which is what limits how many run at the same time.
 */
class RepositoryFilesCache {

    private static final Logger logger = LogManager.getLogger(RepositoryFilesCache.class);

    /**
     * The reads this cache needs from a repository, which may do blocking I/O.
     */
    interface Reader {
        /**
         * Reads the repository data of the given repository generation.
         */
        RepositoryData readRepositoryData(long repositoryGeneration) throws IOException;

        /**
         * Reads the shard-level metadata of the given shard generation.
         */
        BlobStoreIndexShardSnapshots readShardSnapshots(IndexId indexId, int shardId, ShardGeneration shardGeneration) throws IOException;
    }

    private record ShardRead(ShardId shardId, ShardGeneration generation) {}

    private final String repositoryName;
    private final Reader reader;
    private final Executor readExecutor;

    // The repository generation that is loaded or being loaded, and the repository data once loaded
    private long requestedGeneration = RepositoryData.UNKNOWN_REPO_GEN;
    private volatile RepositoryData repositoryData;

    // Every local shard we have been asked about, and the file lists we have read so far
    private final Set<ShardId> knownShards = ConcurrentHashMap.newKeySet();
    private final Map<ShardId, RepositoryShardFiles> shardFiles = new ConcurrentHashMap<>();
    private final Set<ShardRead> readsInFlight = ConcurrentHashMap.newKeySet();

    RepositoryFilesCache(String repositoryName, Reader reader, Executor readExecutor) {
        this.repositoryName = repositoryName;
        this.reader = reader;
        this.readExecutor = readExecutor;
    }

    /**
     * Tells the cache the repository generation in the cluster state, loading the repository data again if that is a new one.
     */
    void onRepositoryGeneration(long repositoryGeneration) {
        if (repositoryGeneration < RepositoryData.EMPTY_REPO_GEN) {
            return; // the repository generation is not known (yet), so there is nothing to load
        }
        synchronized (this) {
            if (repositoryGeneration == requestedGeneration) {
                return;
            }
            requestedGeneration = repositoryGeneration;
        }
        try {
            readExecutor.execute(() -> loadRepositoryData(repositoryGeneration));
        } catch (Exception e) {
            onRepositoryDataFailure(repositoryGeneration, e);
        }
    }

    private void loadRepositoryData(long repositoryGeneration) {
        final RepositoryData loaded;
        try {
            loaded = reader.readRepositoryData(repositoryGeneration);
        } catch (Exception e) {
            onRepositoryDataFailure(repositoryGeneration, e);
            return;
        }
        synchronized (this) {
            if (repositoryGeneration != requestedGeneration) {
                return; // a newer generation was requested meanwhile
            }
            repositoryData = loaded;
        }
        // read the new file lists of the shards we know about right away, rather than when they are next asked for
        List.copyOf(knownShards).forEach(this::getShardFiles);
    }

    private synchronized void onRepositoryDataFailure(long repositoryGeneration, Exception e) {
        // e.g. a NoSuchFileException because the generation has been replaced by a newer one, or an unreachable repository: ask again
        // the next time we hear about the generation
        logger.debug(() -> "[" + repositoryName + "] failed to load repository data of generation [" + repositoryGeneration + "]", e);
        if (repositoryGeneration == requestedGeneration) {
            requestedGeneration = RepositoryData.UNKNOWN_REPO_GEN;
        }
    }

    /**
     * @return the files the repository holds of the given shard, which may be the list of an older shard generation while the current one
     *         is being read, or {@code null} if this node has never read the files of the shard, in which case they are being read.
     */
    @Nullable
    RepositoryShardFiles getShardFiles(ShardId shardId) {
        knownShards.add(shardId);
        final RepositoryData data = repositoryData;
        if (data == null) {
            return null; // not even the shard generations are known yet
        }
        final IndexId indexId = data.getIndices().get(shardId.getIndexName());
        final ShardGeneration latest = indexId == null ? null : data.shardGenerations().getShardGen(indexId, shardId.id());
        if (latest == null || latest.equals(ShardGenerations.NEW_SHARD_GEN) || latest.equals(ShardGenerations.DELETED_SHARD_GEN)) {
            // The repository has no shard-level metadata for this shard, so it holds none of its files
            return RepositoryShardFiles.NONE;
        }
        final RepositoryShardFiles cached = shardFiles.get(shardId);
        if (cached == null || cached.generation().equals(latest) == false) {
            readShardFiles(indexId, shardId, latest);
        }
        return cached;
    }

    private void readShardFiles(IndexId indexId, ShardId shardId, ShardGeneration generation) {
        final var read = new ShardRead(shardId, generation);
        if (readsInFlight.add(read) == false) {
            return;
        }
        try {
            readExecutor.execute(() -> {
                try {
                    final var snapshots = reader.readShardSnapshots(indexId, shardId.id(), generation);
                    if (knownShards.contains(shardId)) {
                        shardFiles.put(shardId, RepositoryShardFiles.of(generation, snapshots));
                    }
                } catch (Exception e) {
                    // e.g. a NoSuchFileException because the generation has been replaced by a newer one: the next evaluation asks again
                    logger.debug(() -> "[" + repositoryName + "] failed to read the files of shard " + shardId + " at " + generation, e);
                } finally {
                    readsInFlight.remove(read);
                }
            });
        } catch (Exception e) {
            readsInFlight.remove(read);
            logger.debug(() -> "[" + repositoryName + "] failed to start reading the files of shard " + shardId, e);
        }
    }

    /**
     * Forgets the shards that are not in the given set, e.g. because they moved to another node.
     */
    void retainShards(Set<ShardId> shardsOnThisNode) {
        knownShards.retainAll(shardsOnThisNode);
        shardFiles.keySet().retainAll(shardsOnThisNode);
    }
}
