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
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.index.store.StoreMetricsDirectory;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.cache.reader.MutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
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
 * Factory for creating stateless directories for use by benchmarks, so that a benchmark can measure a stateless read path
 * without standing up a node. Both flavours are read-write — the benchmark writes its Lucene index through them — and are
 * named after the node role whose <em>read</em> path they reproduce:
 *
 * <ul>
 *   <li>{@link #newSearchDirectory(Path)} and its overloads reproduce {@link SearchDirectory}: reads of committed files
 *       flow through the stateless blob cache, as on a search node. Writes go to a local NIOFSDirectory, and each file
 *       becomes readable through the cache as soon as it is closed, so nothing is ever read from the local copy.</li>
 *   <li>{@link #newIndexDirectory(Path, Path)} reproduces {@link IndexDirectory}: files are read from the local copy for as
 *       long as they have not been uploaded to the object store, as on an indexing node. Nothing is ever uploaded here, so
 *       reads stay on the local path for the lifetime of the directory. Just like in production code, the index path is
 *       wiped when the directory is created.</li>
 * </ul>
 */
public final class StatelessDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(StatelessDirectoryFactory.class);

    /**
     * System property: fixed sleep (in milliseconds) injected into every
     * {@code BlobContainer.readBlob(...)} call, modeling per-region first-byte
     * latency to a remote object store. Defaults to 0 (no added latency).
     */
    public static final String FIRST_BYTE_LATENCY_MS_PROP = "es.stateless.bench.firstByteLatencyMs";

    /**
     * System property: explicit shared blob cache size in bytes. When unset,
     * the factory auto-sizes the cache to fit every existing index file (no
     * eviction). Setting this smaller than the working set forces a realistic
     * miss/hit mix and exercises the latency injected via
     * {@link #FIRST_BYTE_LATENCY_MS_PROP}.
     */
    public static final String CACHE_SIZE_BYTES_PROP = "es.stateless.bench.cacheSizeBytes";

    private StatelessDirectoryFactory() {}

    /**
     * Creates a directory with index data and cache co-located in {@code indexPath}.
     * The cache is sized to fit all existing index files on disk.
     */
    public static Directory newSearchDirectory(Path indexPath) throws IOException {
        return newSearchDirectory(indexPath, indexPath);
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
    public static Directory newSearchDirectory(Path indexPath, Path workPath) throws IOException {
        return newSearchDirectory(indexPath, workPath, Settings.EMPTY);
    }

    /**
     * Same as {@link #newSearchDirectory(Path, Path)} but lets the caller merge additional node
     * settings (for example {@code node.roles}) on top of the defaults. Caller-provided
     * keys overwrite the defaults set by this factory.
     */
    public static Directory newSearchDirectory(Path indexPath, Path workPath, Settings extraNodeSettings) throws IOException {
        return SearchNodeDirectory.create(indexPath, workPath, extraNodeSettings);
    }

    /**
     * Creates a directory that keeps the Lucene index, the node environment and the shared cache all under
     * {@code indexPath}. Fine for a scratch directory nothing else reads. Prefer {@link #newIndexDirectory(Path, Path)}
     * when {@code indexPath} is also opened as a plain Lucene index, since that keeps the node
     * environment and cache files out of it.
     */
    public static Directory newIndexDirectory(Path indexPath) throws IOException {
        return newIndexDirectory(indexPath, indexPath);
    }

    /**
     * Creates an {@link IndexDirectory} whose files are written locally and never uploaded to the object store, so that
     * {@link IndexDirectory#openInput} returns a {@code ReopeningIndexInput} reading from the local file. That is the read
     * path a stateless indexing node takes when it reopens a just-written segment file, for instance when a Lucene merge
     * reopens the flat vector file it has just written.
     *
     * <p><b>Any existing index at {@code indexPath} is destroyed.</b> {@link IndexDirectory} tracks only the files it has
     * itself created: {@link IndexDirectory#listAll()} never reports what is on disk, so pre-existing files would be
     * invisible to Lucene yet still collide when it creates an output of the same name. {@code indexPath} is therefore
     * wiped with {@link Lucene#cleanLuceneIndex}, exactly as {@code StatelessPlugin} does before wrapping the directory of
     * a promotable shard.
     *
     * <p>The directory is wrapped in a {@link StoreMetricsDirectory} to match the input chain a real shard sees (see
     * {@code Store}). The shared blob cache is created because {@link IndexDirectory} requires one, but it is never read
     * from on this path: no blob container is registered and no file is ever marked as uploaded, so
     * {@code IndexBlobStoreCacheDirectory.containsFile} returns {@code false} for every name.
     *
     * @param indexPath path where index files are written; wiped before use
     * @param workPath  scratch directory for the node environment and cache files
     * @return a read-write directory reproducing the stateless indexing-node local read path
     */
    public static Directory newIndexDirectory(Path indexPath, Path workPath) throws IOException {
        var localDirectory = new MMapDirectory(indexPath);
        boolean success = false;
        try {
            // IndexDirectory requires an empty local directory. Wipe before creating the infrastructure, so that
            // the node environment and cache files are not in the way when indexPath and workPath are the same directory.
            Lucene.cleanLuceneIndex(localDirectory);
            // The cache is never read on this path, so a single region is enough
            var infra = Infra.create(workPath, SHARED_CACHE_REGION_SIZE_SETTING.getDefault(Settings.EMPTY), Settings.EMPTY);
            try {
                var indexDirectory = new IndexDirectory(
                    localDirectory,
                    new IndexBlobStoreCacheDirectory(infra.cacheService(), new ShardId(new Index("index", "_na_"), 0)),
                    null,
                    true
                );
                var directory = new IndexNodeDirectory(
                    new StoreMetricsDirectory(indexDirectory, new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new)),
                    infra
                );
                success = true;
                return directory;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(infra);
                }
            }
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(localDirectory);
            }
        }
    }

    /**
     * The node-level infrastructure a stateless directory needs.
     * Owned by the directory that created it and closed with it.
     */
    private record Infra(
        Settings nodeSettings,
        NodeEnvironment nodeEnvironment,
        ThreadPool threadPool,
        StatelessSharedBlobCacheService cacheService
    ) implements Closeable {

        static Infra create(Path workPath, ByteSizeValue cacheSize, Settings extraNodeSettings) throws IOException {
            var nodeSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), workPath)
                .putList(Environment.PATH_DATA_SETTING.getKey(), workPath.toString())
                .put(SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
                .put(SHARED_CACHE_MMAP.getKey(), true)
                .put("node.id.seed", 0L)
                .put(extraNodeSettings)
                .build();
            var nodeEnvironment = new NodeEnvironment(nodeSettings, new Environment(nodeSettings, null));
            var threadPool = new ThreadPool(
                Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "stateless-directory").build(),
                MeterRegistry.NOOP,
                new DefaultBuiltInExecutorBuilders(),
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, false)
            );
            return new Infra(
                nodeSettings,
                nodeEnvironment,
                threadPool,
                TestUtils.newCacheService(nodeEnvironment, nodeSettings, threadPool)
            );
        }

        @Override
        public void close() throws IOException {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
            cacheService.close();
            nodeEnvironment.close();
        }
    }

    /**
     * The read path of a stateless <em>indexing</em> node. All the behaviour comes from the {@link IndexDirectory} chain
     * this wraps; the only thing added here is releasing the {@link Infra} on close, which {@link IndexDirectory#close()}
     * does not do (it closes the cache directory and the local delegate, leaving the cache service, thread pool and node
     * environment open).
     */
    private static class IndexNodeDirectory extends FilterDirectory {

        private final Infra infra;

        IndexNodeDirectory(Directory in, Infra infra) {
            super(in);
            this.infra = infra;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(in, infra);
        }
    }

    /**
     * A read-write directory reproducing the read path of a stateless <em>search</em> node. Writes go to a local
     * NIOFSDirectory; reads for committed files flow through the {@link SearchDirectory} /
     * {@link StatelessSharedBlobCacheService}. Uncommitted files (temp files, lock files) are read directly from disk.
     */
    private static class SearchNodeDirectory extends FilterDirectory {

        static SearchNodeDirectory create(Path dataPath, Path workPath, Settings extraNodeSettings) throws IOException {
            long regionSize = SHARED_CACHE_REGION_SIZE_SETTING.getDefault(Settings.EMPTY).getBytes();
            Long cacheSizeOverride = Long.getLong(CACHE_SIZE_BYTES_PROP);
            long cacheSizeBytes = cacheSizeOverride != null
                ? ((cacheSizeOverride + regionSize - 1) / regionSize) * regionSize
                : computeRegionAlignedCacheSize(dataPath);
            var cacheSize = ByteSizeValue.ofBytes(cacheSizeBytes);
            long firstByteLatencyMs = Long.getLong(FIRST_BYTE_LATENCY_MS_PROP, 0L);
            var infra = Infra.create(workPath, cacheSize, extraNodeSettings);
            var nodeSettings = infra.nodeSettings();
            var threadPool = infra.threadPool();
            var cacheService = infra.cacheService();

            logInitialCacheStats(dataPath, nodeSettings, cacheSize, cacheService, cacheSizeOverride != null, firstByteLatencyMs);
            var fakeBlobStoreDirectory = new NIOFSDirectory(dataPath);
            var blobNameToFileName = new ConcurrentHashMap<String, String>();

            var cacheBlobReaderService = new CacheBlobReaderService(
                nodeSettings,
                cacheService,
                null,
                threadPool,
                TestUtils.unmeteredFillCacheMemoryPressure(nodeSettings, threadPool)
            );
            var shardId = new ShardId(new Index("index", "_na_"), 0);

            var searchDirectory = new SearchDirectory(
                cacheService,
                cacheBlobReaderService,
                MutableObjectStoreUploadTracker.ALWAYS_UPLOADED,
                shardId,
                false,
                IndexVersion.current()
            );

            var blobStore = new FsBlobStore(8192, dataPath, true);
            var fakeBlobContainer = new FsBlobContainer(blobStore, BlobPath.EMPTY, dataPath) {
                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    simulateFirstByteLatency(firstByteLatencyMs);
                    return super.readBlob(purpose, resolveBlobName(blobName), position, length);
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
                    simulateFirstByteLatency(firstByteLatencyMs);
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

            return new SearchNodeDirectory(searchDirectory, fakeBlobStoreDirectory, blobNameToFileName, infra);
        }

        private final SearchDirectory searchDirectory;
        private final Directory fakeBlobStoreDirectory;
        private final Map<String, String> blobNameToFileName;
        private final Infra infra;
        private final Map<String, BlobFileRanges> metadata = new ConcurrentHashMap<>();
        private final AtomicInteger blobFileGenerationGenerator = new AtomicInteger();

        private SearchNodeDirectory(
            SearchDirectory searchDirectory,
            Directory fakeBlobStoreDirectory,
            Map<String, String> blobNameToFileName,
            Infra infra
        ) {
            super(searchDirectory);
            this.searchDirectory = searchDirectory;
            this.fakeBlobStoreDirectory = fakeBlobStoreDirectory;
            this.blobNameToFileName = blobNameToFileName;
            this.infra = infra;
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
            IOUtils.close(searchDirectory, fakeBlobStoreDirectory, infra);
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
            StatelessSharedBlobCacheService cacheService,
            boolean cacheSizeOverridden,
            long firstByteLatencyMs
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
                "Cache capacity: cacheSize={} ({}), regionSize={}, regionsAvailable={}, regionsNeeded={}, deficit={}{}",
                cacheSize,
                cacheSizeOverridden ? "user-overridden via " + CACHE_SIZE_BYTES_PROP : "auto-sized",
                ByteSizeValue.ofBytes(regionSize),
                regionsAvailable,
                regionsNeeded,
                deficit,
                deficit > 0 ? " *** CACHE TOO SMALL - eviction will occur ***" : " (OK)"
            );
            logger.info("Simulated blob-store first-byte latency: {} ms (set via {})", firstByteLatencyMs, FIRST_BYTE_LATENCY_MS_PROP);
        }

        private static void simulateFirstByteLatency(long latencyMs) throws IOException {
            if (latencyMs <= 0) {
                return;
            }
            try {
                Thread.sleep(latencyMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while simulating blob-store latency");
            }
        }
    }

    /**
     * Logs cache stats (regions, evictions, writes, reads) for a directory
     * created by this factory. No-op if the directory is not a SearchNodeDirectory.
     *
     * @param dir   the directory to inspect
     * @param label a label to identify the logging context (e.g. "Before prewarm")
     */
    public static void logCacheStats(Directory dir, String label) {
        if (dir instanceof SearchNodeDirectory sd) {
            var stats = sd.infra.cacheService().getStats();
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

    /**
     * Returns a snapshot of the underlying shared blob cache stats for a directory
     * created by this factory, or {@code null} if the directory is not a SearchNodeDirectory.
     * Intended for benchmark callers that want to compute deltas around a query.
     */
    public static SharedBlobCacheService.Stats statsFor(Directory dir) {
        return dir instanceof SearchNodeDirectory sd ? sd.infra.cacheService().getStats() : null;
    }
}
