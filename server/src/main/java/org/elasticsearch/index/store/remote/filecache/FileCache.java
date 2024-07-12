/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.remote.filecache;


import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.annotation.PublicApi;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.common.breaker.CircuitBreaker;
import org.elasticsearch.index.store.remote.utils.cache.CacheUsage;
import org.elasticsearch.index.store.remote.utils.cache.RefCountedCache;
import org.elasticsearch.index.store.remote.utils.cache.SegmentedCache;
import org.elasticsearch.index.store.remote.utils.cache.stats.CacheStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static org.elasticsearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION;


/**
 * File Cache (FC) is introduced to solve the problem that the local disk cannot hold
 * the entire dataset on remote store. It maintains a node level view of index files with priorities,
 * caching only those index files needed by queries. The file with the lowest priority
 * (Least Recently Used) in the FC is replaced first.
 *
 * <p>The two main interfaces of FC are put and get. When a new file index input is added
 * to the file cache, the file will be added at cache head, which means it has the highest
 * priority.
 * <p> The get function does not add file to cache, but it promotes the priority
 * of a given file (since it makes it the most recently used).
 *
 * <p>Once file cache reaches its capacity, it starts evictions. Eviction removes the file
 * items from cache tail and triggers a callback to clean up the file from disk. The
 * cleanup process also includes closing file’s descriptor.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.7.0")
public abstract class FileCache implements RefCountedCache<Path, CachedIndexInput> {
    private final SegmentedCache<Path, CachedIndexInput> theCache;

    private final CircuitBreaker circuitBreaker;

    public FileCache(SegmentedCache<Path, CachedIndexInput> cache, CircuitBreaker circuitBreaker) {
        this.theCache = cache;
        this.circuitBreaker = circuitBreaker;
    }

    public long capacity() {
        return theCache.capacity();
    }

    @Override
    public CachedIndexInput put(Path filePath, CachedIndexInput indexInput) {
        CachedIndexInput cachedIndexInput = theCache.put(filePath, indexInput);
        checkParentBreaker(filePath);
        return cachedIndexInput;
    }

    @Override
    public CachedIndexInput compute(
        Path key,
        BiFunction<? super Path, ? super CachedIndexInput, ? extends CachedIndexInput> remappingFunction
    ) {
        CachedIndexInput cachedIndexInput = theCache.compute(key, remappingFunction);
        checkParentBreaker(key);
        return cachedIndexInput;
    }

    /**
     * Given a file path, gets the corresponding file index input from FileCache.
     * This API also updates the priority for the given file
     *
     * @param filePath given file path
     * @return corresponding file index input from FileCache.
     */
    public CachedIndexInput get(Path filePath) {
        return theCache.get(filePath);
    }

    /**
     * Given a file path, remove the file from cache.
     * Even if the file is pinned or it's still in use, the reclaim
     * still take effect.
     *
     * @param filePath given file path
     */
    public void remove(final Path filePath) {
        theCache.remove(filePath);
    }

    @Override
    public void clear() {
        theCache.clear();
    }

    @Override
    public long size() {
        return theCache.size();
    }

    @Override
    public void incRef(Path key) {
        theCache.incRef(key);
    }

    @Override
    public void decRef(Path key) {
        theCache.decRef(key);
    }

    @Override
    public long prune() {
        return theCache.prune();
    }

    @Override
    public long prune(Predicate<Path> keyPredicate) {
        return theCache.prune(keyPredicate);
    }

    @Override
    public CacheUsage usage() {
        return theCache.usage();
    }

    @Override
    public CacheStats stats() {
        return theCache.stats();
    }

    /**
     * Ensures that the PARENT breaker is not tripped when an entry is added to the cache
     * @param filePath the path key for which entry is added
     */
    private void checkParentBreaker(Path filePath) {
        try {
            circuitBreaker.addEstimateBytesAndMaybeBreak(0, "filecache_entry");
        } catch (CircuitBreakingException ex) {
            theCache.remove(filePath);
            throw new CircuitBreakingException(
                "Unable to create file cache entries",
                ex.getBytesWanted(),
                ex.getByteLimit(),
                ex.getDurability()
            );
        }
    }


    public void restoreFromDirectory(List<Path> fileCacheDataPaths) {
        fileCacheDataPaths.stream()
            .filter(Files::isDirectory)
            .map(path -> path.resolve(LOCAL_STORE_LOCATION))
            .filter(Files::isDirectory)
            .flatMap(dir -> {
                try {
                    return Files.list(dir);
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        "Unable to process file cache directory. Please clear the file cache for node startup.",
                        e
                    );
                }
            })
            .filter(Files::isRegularFile)
            .forEach(path -> {
                try {
                    put(path.toAbsolutePath(), new RestoredCachedIndexInput(Files.size(path)));
                    decRef(path.toAbsolutePath());
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        "Unable to retrieve cache file details. Please clear the file cache for node startup.",
                        e
                    );
                }
            });
    }

    /**
     * Returns the current {@link FileCacheStats}
     */
    public FileCacheStats fileCacheStats() {
        CacheStats stats = stats();
        CacheUsage usage = usage();
        return new FileCacheStats(
            System.currentTimeMillis(),
            usage.activeUsage(),
            capacity(),
            usage.usage(),
            stats.evictionWeight(),
            stats.hitCount(),
            stats.missCount()
        );
    }

    /**
     * Placeholder for the existing file blocks that are in the disk-based
     * local cache at node startup time. We can't open a file handle to these
     * blocks at this point, so we store this placeholder object in the cache.
     * If a block is needed, then these entries will be replaced with a proper
     * entry that will open the actual file handle to create the IndexInput.
     * These entries are eligible for eviction so if nothing needs to reference
     * them they will be deleted when the disk-based local cache fills up.
     */
    private static class RestoredCachedIndexInput implements CachedIndexInput {
        private final long length;

        private RestoredCachedIndexInput(long length) {
            this.length = length;
        }

        @Override
        public IndexInput getIndexInput() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public void close() throws Exception {}
    }
}
