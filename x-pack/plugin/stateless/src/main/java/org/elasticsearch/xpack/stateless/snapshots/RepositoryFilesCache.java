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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * All repository reads run on the read executor given to the constructor, which is what limits how many run at the same time. The state
 * of the cache is only ever changed, and read, on the state executor: a single thread at a time, which the results of reads are handed
 * back to, so that the cache needs no synchronization and an update cannot be lost to a concurrent one.
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
    private final Executor stateExecutor;

    // The repository generation the shard generations are from, and the shard generations: a shard that is a key has been answered by the
    // master, and has the generation of its value, or none (null) if the repository holds no shard-level metadata for it. A shard that is
    // not a key is unknown. Replaced as a whole, so that all of them are from the same repository generation.
    private long repositoryGeneration = RepositoryData.UNKNOWN_REPO_GEN;
    private Map<ShardId, RepositoryShardGeneration> shardGenerations = new HashMap<>();

    // Every local shard we have been asked about, and the file lists we have read so far
    private final Set<ShardId> knownShards = new HashSet<>();
    private final Map<ShardId, RepositoryShardFiles> shardFiles = new HashMap<>();
    private final Set<ShardRead> readsInFlight = new HashSet<>();
    // read by the reads that have been queued, which run on other threads
    private volatile boolean closed;

    /**
     * @param readExecutor  runs the reads of the repository, and limits how many of them run at the same time
     * @param stateExecutor runs everything that uses or changes the state of the cache, one task at a time
     */
    RepositoryFilesCache(String repositoryName, Reader reader, Executor readExecutor, Executor stateExecutor) {
        this.repositoryName = repositoryName;
        this.reader = reader;
        this.readExecutor = readExecutor;
        this.stateExecutor = stateExecutor;
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
        this.shardGenerations = new HashMap<>(latest);
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
     * @return whether the cached file list of the given shard is the one of the latest shard generation, so that it is what the
     *         repository holds, as far as the shard generations are from, and no read of a newer list is needed. It is not if the shard
     *         generation is not known, or the list has not been read yet, or is that of an older generation.
     */
    boolean isUpToDate(ShardId shardId) {
        if (shardGenerations.containsKey(shardId) == false) {
            return false;
        }
        final RepositoryShardGeneration latest = shardGenerations.get(shardId);
        if (latest == null) {
            return true; // the repository holds no shard-level metadata, which getShardFiles returns for it right away
        }
        final RepositoryShardFiles cached = shardFiles.get(shardId);
        return cached != null && cached.generation().equals(latest.generation());
    }

    /**
     * @return the files the repository holds of the given shard, which may be the list of an older shard generation while the current one
     *         is being read, or {@code null} if this node has never read the files of the shard, in which case they are being read.
     */
    @Nullable
    RepositoryShardFiles getShardFiles(ShardId shardId) {
        knownShards.add(shardId);
        if (shardGenerations.containsKey(shardId) == false) {
            return null; // not even the shard generation is known yet
        }
        final RepositoryShardGeneration latest = shardGenerations.get(shardId);
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
                RepositoryShardFiles files = null;
                if (closed == false) {
                    try {
                        files = RepositoryShardFiles.of(
                            latest.generation(),
                            reader.readShardSnapshots(latest.indexId(), shardId.id(), latest.generation())
                        );
                    } catch (Exception e) {
                        // e.g. a NoSuchFileException because the generation has been replaced by a newer one: the next evaluation asks
                        // again
                        logger.debug(
                            () -> "[" + repositoryName + "] failed to read the files of shard " + shardId + " at " + latest.generation(),
                            e
                        );
                    }
                }
                final var readFiles = files;
                stateExecutor.execute(() -> onShardFilesRead(read, readFiles));
            });
        } catch (Exception e) {
            readsInFlight.remove(read);
            logger.debug(() -> "[" + repositoryName + "] failed to start reading the files of shard " + shardId, e);
        }
    }

    private void onShardFilesRead(ShardRead read, @Nullable RepositoryShardFiles files) {
        readsInFlight.remove(read);
        // a shard that left this node meanwhile is not kept
        if (files != null && closed == false && knownShards.contains(read.shardId())) {
            shardFiles.put(read.shardId(), files);
        }
    }

    /**
     * Forgets the shards that are not in the given set, e.g. because they moved to another node.
     */
    void retainShards(Set<ShardId> shardsOnThisNode) {
        knownShards.retainAll(shardsOnThisNode);
        shardFiles.keySet().retainAll(shardsOnThisNode);
        shardGenerations.keySet().retainAll(shardsOnThisNode);
    }

    /**
     * Stops reading, e.g. because the tracking of the repository was turned off. Reads that are already queued do nothing.
     */
    void close() {
        closed = true;
    }
}
