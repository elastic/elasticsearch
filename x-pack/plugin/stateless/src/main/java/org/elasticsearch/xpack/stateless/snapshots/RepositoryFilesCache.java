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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * What one repository holds of the shards on this node, kept up to date without ever guessing.
 * <p>
 * The latest shard generations of the repository are given to {@link #onShardGenerations}, which gets them from the master (see
 * {@link ShardGenerationsRefresher}), so this node never reads the repository's root blob. The file list of a shard
 * ({@code index-{generation}}) is read only for shards whose generation changed. Until that read is done the previously cached list is
 * still returned, which is a little stale but close, so the reported backlog does not jump just because the repository changed. Only a
 * shard that has never been read on this node, because it just arrived or this node just started, has an unknown list:
 * {@link #getShardFiles} returns {@code null} for it and starts the read, instead of pretending the repository holds nothing or
 * everything.
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
         * Reads the shard-level metadata of the given shard generation.
         */
        BlobStoreIndexShardSnapshots readShardSnapshots(IndexId indexId, int shardId, ShardGeneration shardGeneration) throws IOException;
    }

    private record ShardRead(ShardId shardId, ShardGeneration generation) {}

    private final String repositoryName;
    private final Reader reader;
    private final Executor readExecutor;

    // The repository generation the shard generations are from, and the shard generations: a shard that is a key has been answered by the
    // master, and has the generation of its value, or none (null) if the repository holds no shard-level metadata for it. A shard that is
    // not a key is unknown. Replaced as a whole, so that all of them are from the same repository generation.
    private volatile long repositoryGeneration = RepositoryData.UNKNOWN_REPO_GEN;
    private volatile Map<ShardId, RepositoryShardGeneration> shardGenerations = Map.of();

    // Every local shard we have been asked about, and the file lists we have read so far
    private final Set<ShardId> knownShards = ConcurrentHashMap.newKeySet();
    private final Map<ShardId, RepositoryShardFiles> shardFiles = new ConcurrentHashMap<>();
    private final Set<ShardRead> readsInFlight = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    RepositoryFilesCache(String repositoryName, Reader reader, Executor readExecutor) {
        this.repositoryName = repositoryName;
        this.reader = reader;
        this.readExecutor = readExecutor;
    }

    /**
     * Replaces the shard generations with the latest ones of the repository, and starts reading the file lists of the shards whose
     * generation changed.
     *
     * @param repositoryGeneration the generation of the repository the shard generations are from
     * @param latest               the shard generation of each shard, or {@code null} for a shard the repository has no shard-level
     *                             metadata for
     */
    void onShardGenerations(long repositoryGeneration, Map<ShardId, RepositoryShardGeneration> latest) {
        if (closed) {
            return;
        }
        this.shardGenerations = Collections.unmodifiableMap(new HashMap<>(latest));
        this.repositoryGeneration = repositoryGeneration;
        // read the new file lists of the shards we know about right away, rather than when they are next asked for
        List.copyOf(knownShards).forEach(this::getShardFiles);
    }

    /**
     * @return the repository generation of the latest shard generations, or {@link RepositoryData#UNKNOWN_REPO_GEN} if there are none yet
     */
    long getRepositoryGeneration() {
        return repositoryGeneration;
    }

    /**
     * @return whether the repository generation of the given shard is known, even if it is that the repository holds nothing of the shard
     */
    boolean hasShardGeneration(ShardId shardId) {
        return shardGenerations.containsKey(shardId);
    }

    /**
     * @return the files the repository holds of the given shard, which may be the list of an older shard generation while the current one
     *         is being read, or {@code null} if this node has never read the files of the shard, in which case they are being read.
     */
    @Nullable
    RepositoryShardFiles getShardFiles(ShardId shardId) {
        knownShards.add(shardId);
        final Map<ShardId, RepositoryShardGeneration> generations = shardGenerations;
        if (generations.containsKey(shardId) == false) {
            return null; // not even the shard generation is known yet
        }
        final RepositoryShardGeneration latest = generations.get(shardId);
        if (latest == null) {
            // The repository has no shard-level metadata for this shard, so it holds none of its files
            return RepositoryShardFiles.NONE;
        }
        final RepositoryShardFiles cached = shardFiles.get(shardId);
        if (cached == null || cached.generation().equals(latest.generation()) == false) {
            readShardFiles(shardId, latest);
        }
        return cached;
    }

    private void readShardFiles(ShardId shardId, RepositoryShardGeneration latest) {
        final var read = new ShardRead(shardId, latest.generation());
        if (readsInFlight.add(read) == false) {
            return;
        }
        try {
            readExecutor.execute(() -> {
                try {
                    if (closed) {
                        return;
                    }
                    final var snapshots = reader.readShardSnapshots(latest.indexId(), shardId.id(), latest.generation());
                    if (knownShards.contains(shardId)) {
                        shardFiles.put(shardId, RepositoryShardFiles.of(latest.generation(), snapshots));
                    }
                } catch (Exception e) {
                    // e.g. a NoSuchFileException because the generation has been replaced by a newer one: the next evaluation asks again
                    logger.debug(
                        () -> "[" + repositoryName + "] failed to read the files of shard " + shardId + " at " + latest.generation(),
                        e
                    );
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
        final var generations = shardGenerations;
        if (shardsOnThisNode.containsAll(generations.keySet()) == false) {
            final Map<ShardId, RepositoryShardGeneration> retained = new HashMap<>(generations);
            retained.keySet().retainAll(shardsOnThisNode);
            shardGenerations = Collections.unmodifiableMap(retained);
        }
    }

    /**
     * Stops reading, e.g. because the tracking of the repository was turned off. Reads that are already queued do nothing.
     */
    void close() {
        closed = true;
    }
}
