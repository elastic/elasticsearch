/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.cache.reader.MutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_MMAP;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;

/**
 * Factory for creating a stateless directory for use by the KnnIndexTester
 * benchmark tool. This class is loaded via reflection from the elasticsearch repo's
 * {@code modules-self-managed/vector} module, so the method signatures form a stable public API.
 *
 * <p>The returned directory supports both reads and writes: writes go to a local
 * NIOFSDirectory, and reads flow through the stateless blob cache
 * (StatelessSharedBlobCacheService). This faithfully reproduces the stateless
 * SearchDirectory read path for benchmarking purposes.
 */
public final class StatelessDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(StatelessDirectoryFactory.class);

    private StatelessDirectoryFactory() {}

    /**
     * Creates a directory with data and cache co-located in {@code dataPath}.
     * The cache is sized to fit all existing index files on disk.
     */
    public static Directory create(Path dataPath) throws IOException {
        return create(dataPath, dataPath);
    }

    /**
     * Creates a directory with the index data stored in {@code indexPath} and cache
     * infrastructure in {@code workPath}. The cache is sized to fit all existing index
     * files on disk (rounded up to 16MB alignment).
     *
     * @param indexPath path where index files are stored (and written to by IndexWriter)
     * @param workPath  scratch directory for cache and node environment files
     * @return a read-write directory backed by the stateless blob cache
     */
    public static Directory create(Path indexPath, Path workPath) throws IOException {
        return StatelessDirectory.create(indexPath, workPath);
    }

    /**
     * A read-write directory backed by the stateless blob cache. Writes go to a local
     * NIOFSDirectory; reads for committed files flow through the SearchDirectory /
     * StatelessSharedBlobCacheService, faithfully reproducing the stateless read path.
     * Uncommitted files (temp files, lock files) are read directly from disk.
     *
     * <p> This is a test utility intended for benchmarking (e.g. KnnIndexTester).
     */
    private static class StatelessDirectory extends FilterDirectory {

        static StatelessDirectory create(Path dataPath, Path workPath) throws IOException {
            var cacheSize = ByteSizeValue.ofBytes(computeRegionAlignedCacheSize(dataPath));
            var nodeSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), workPath)
                .putList(Environment.PATH_DATA_SETTING.getKey(), workPath.toString())
                .put(SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
                .put(SHARED_CACHE_MMAP.getKey(), true)
                .put("node.id.seed", 0L)
                .build();
            var nodeEnvironment = new NodeEnvironment(nodeSettings, new Environment(nodeSettings, null));
            var threadPool = new ThreadPool(
                Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "stateless-directory").build(),
                MeterRegistry.NOOP,
                new DefaultBuiltInExecutorBuilders(),
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, false)
            );
            var cacheService = new StatelessSharedBlobCacheService(
                nodeEnvironment,
                nodeSettings,
                threadPool,
                new BlobCacheMetrics(MeterRegistry.NOOP),
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            );

            logInitialCacheStats(dataPath, nodeSettings, cacheSize, cacheService);
            var fakeBlobStoreDirectory = new NIOFSDirectory(dataPath);
            var blobNameToFileName = new ConcurrentHashMap<String, String>();

            var cacheBlobReaderService = new CacheBlobReaderService(nodeSettings, cacheService, null, threadPool);
            var shardId = new ShardId(new Index("index", "_na_"), 0);

            var searchDirectory = new SearchDirectory(
                cacheService,
                cacheBlobReaderService,
                MutableObjectStoreUploadTracker.ALWAYS_UPLOADED,
                shardId
            );

            var blobStore = new FsBlobStore(8192, dataPath, true);
            var fakeBlobContainer = new FsBlobContainer(blobStore, BlobPath.EMPTY, dataPath) {
                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    return super.readBlob(purpose, resolveBlobName(blobName), position, length);
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
                    return super.readBlob(purpose, resolveBlobName(blobName));
                }

                private String resolveBlobName(String blobName) {
                    var fileName = blobNameToFileName.get(blobName);
                    if (fileName == null) {
                        throw new IllegalStateException("unknown blob [" + blobName + "]");
                    }
                    return fileName;
                }
            };
            searchDirectory.setBlobContainer(primaryTerm -> fakeBlobContainer);

            return new StatelessDirectory(
                searchDirectory,
                fakeBlobStoreDirectory,
                blobNameToFileName,
                threadPool,
                cacheService,
                nodeEnvironment
            );
        }

        private final SearchDirectory searchDirectory;
        private final Directory fakeBlobStoreDirectory;
        private final Map<String, String> blobNameToFileName;
        private final ThreadPool threadPool;
        private final StatelessSharedBlobCacheService cacheService;
        private final NodeEnvironment nodeEnvironment;
        private final Map<String, BlobFileRanges> metadata = new ConcurrentHashMap<>();
        private final AtomicInteger blobFileGenerationGenerator = new AtomicInteger();

        private StatelessDirectory(
            SearchDirectory searchDirectory,
            Directory fakeBlobStoreDirectory,
            Map<String, String> blobNameToFileName,
            ThreadPool threadPool,
            StatelessSharedBlobCacheService cacheService,
            NodeEnvironment nodeEnvironment
        ) {
            super(searchDirectory);
            this.searchDirectory = searchDirectory;
            this.fakeBlobStoreDirectory = fakeBlobStoreDirectory;
            this.blobNameToFileName = blobNameToFileName;
            this.threadPool = threadPool;
            this.cacheService = cacheService;
            this.nodeEnvironment = nodeEnvironment;
        }

        private BlobFileRanges newBlobFileRanges(String name) throws IOException {
            long fileLength = fakeBlobStoreDirectory.fileLength(name);
            int generation = blobFileGenerationGenerator.incrementAndGet();
            var blobName = StatelessCompoundCommit.PREFIX + generation;
            blobNameToFileName.put(blobName, name);
            return new BlobFileRanges(new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1, generation)), 0, fileLength));
        }

        private void refreshMetadata() {
            long total = metadata.values().stream().mapToLong(BlobFileRanges::fileLength).sum();
            searchDirectory.updateMetadata(Map.copyOf(metadata), total);
        }

        @Override
        public String[] listAll() throws IOException {
            return fakeBlobStoreDirectory.listAll();
        }

        @Override
        public long fileLength(String name) throws IOException {
            return fakeBlobStoreDirectory.fileLength(name);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            return new FilterIndexOutput(name, fakeBlobStoreDirectory.createOutput(name, context)) {
                @Override
                public void close() throws IOException {
                    super.close();
                    metadata.put(name, newBlobFileRanges(name));
                    refreshMetadata();
                }
            };
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            return fakeBlobStoreDirectory.createTempOutput(prefix, suffix, context);
        }

        @Override
        public void deleteFile(String name) throws IOException {
            fakeBlobStoreDirectory.deleteFile(name);
            if (metadata.remove(name) != null) {
                refreshMetadata();
            }
        }

        @Override
        public void rename(String source, String dest) throws IOException {
            fakeBlobStoreDirectory.rename(source, dest);
            metadata.remove(source);
            metadata.put(dest, newBlobFileRanges(dest));
            refreshMetadata();
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
            fakeBlobStoreDirectory.sync(names);
        }

        @Override
        public void syncMetaData() throws IOException {
            fakeBlobStoreDirectory.syncMetaData();
        }

        @Override
        public Lock obtainLock(String name) throws IOException {
            return fakeBlobStoreDirectory.obtainLock(name);
        }

        @Override
        public Set<String> getPendingDeletions() throws IOException {
            return fakeBlobStoreDirectory.getPendingDeletions();
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (metadata.containsKey(name) == false) {
                metadata.put(name, newBlobFileRanges(name));
                refreshMetadata();
            }
            return super.openInput(name, context);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(searchDirectory, fakeBlobStoreDirectory);
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
            cacheService.close();
            nodeEnvironment.close();
        }

        /**
         * Computes the cache size needed to hold all index files without eviction.
         * Each file occupies {@code ceil(fileSize / regionSize)} regions, so the
         * total is the sum of per-file region counts times the region size.
         */
        private static long computeRegionAlignedCacheSize(Path dataPath) throws IOException {
            long regionSize = SHARED_CACHE_REGION_SIZE_SETTING.getDefault(Settings.EMPTY).getBytes();
            long totalRegions = 0;
            try (var dir = new NIOFSDirectory(dataPath)) {
                for (String file : dir.listAll()) {
                    if (file.equals("write.lock") == false) {
                        totalRegions += (dir.fileLength(file) + regionSize - 1) / regionSize;
                    }
                }
            }
            return totalRegions * regionSize;
        }

        private static void logInitialCacheStats(
            Path dataPath,
            Settings nodeSettings,
            ByteSizeValue cacheSize,
            StatelessSharedBlobCacheService cacheService
        ) throws IOException {
            int regionSize = Math.toIntExact(SHARED_CACHE_REGION_SIZE_SETTING.get(nodeSettings).getBytes());
            int regionsAvailable = cacheService.getStats().numberOfRegions();
            int regionsNeeded = 0;
            try (var tmpDir = new NIOFSDirectory(dataPath)) {
                for (String file : tmpDir.listAll()) {
                    if (file.equals("write.lock") == false) {
                        long fileLen = tmpDir.fileLength(file);
                        int fileRegions = Math.toIntExact((fileLen + regionSize - 1) / regionSize);
                        regionsNeeded += fileRegions;
                        logger.info("  file={}, size={}, regions={}", file, ByteSizeValue.ofBytes(fileLen), fileRegions);
                    }
                }
            }
            int deficit = regionsNeeded - regionsAvailable;
            logger.info(
                "Cache capacity: cacheSize={}, regionSize={}, regionsAvailable={}, regionsNeeded={}, deficit={}{}",
                cacheSize,
                ByteSizeValue.ofBytes(regionSize),
                regionsAvailable,
                regionsNeeded,
                deficit,
                deficit > 0 ? " *** CACHE TOO SMALL - eviction will occur ***" : " (OK)"
            );
        }
    }

    /**
     * Logs cache stats (regions, evictions, writes, reads) for a directory
     * created by this factory. No-op if the directory is not a StatelessDirectory.
     *
     * @param dir   the directory to inspect
     * @param label a label to identify the logging context (e.g. "Before prewarm")
     */
    public static void logCacheStats(Directory dir, String label) {
        if (dir instanceof StatelessDirectory sd) {
            var stats = sd.cacheService.getStats();
            logger.info(
                "[{}] Cache stats: regions={}, evictions={}, writes={}, readBytes={}",
                label,
                stats.numberOfRegions(),
                stats.evictCount(),
                stats.writeCount(),
                stats.readBytes()
            );
        }
    }
}
