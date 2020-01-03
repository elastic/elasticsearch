/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.RefCounted;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

/**
 * {@link CacheService} maintains a cache entry for all files read from cached searchable snapshot directories (see {@link CacheDirectory})
 */
public class CacheService extends AbstractLifecycleComponent {

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_SIZE_SETTING = Setting.byteSizeSetting("searchable.snapshot.cache.size",
        new ByteSizeValue(1, ByteSizeUnit.GB),                  // TODO: size the default value according to disk space
        new ByteSizeValue(1, ByteSizeUnit.MB),                  // min
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),  // max
        Setting.Property.NodeScope);

    public static final Setting<Boolean> SNAPSHOT_CACHE_INVALIDATE_ON_SHUTDOWN =
        Setting.boolSetting("searchable.snapshot.cache.invalidate_on_shutdown", true, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private static final int CACHE_FILE_RANGE_SIZE = 1 << 15;

    private static final StandardOpenOption[] CACHE_FILE_OPEN_OPTIONS = new StandardOpenOption[]{
        StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, StandardOpenOption.SPARSE
    };

    private static final Logger logger = LogManager.getLogger(CacheService.class);

    private final Cache<String, CacheEntry> cache;
    private final ThreadPool threadPool;

    private volatile boolean invalidateOnShutdown = SNAPSHOT_CACHE_INVALIDATE_ON_SHUTDOWN.get(Settings.EMPTY);

    public CacheService(final Settings settings, final ClusterSettings clusterSettings, final ThreadPool threadPool) {
        this.cache = CacheBuilder.<String, CacheEntry>builder()
            .setMaximumWeight(SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes())
            .weigher((key, entry) -> entry.estimateWeight()) // TODO only evaluated on promotion/eviction...
            .removalListener(notification -> markAsEvicted(notification.getValue()))
            .build();
        clusterSettings.addSettingsUpdateConsumer(SNAPSHOT_CACHE_INVALIDATE_ON_SHUTDOWN, this::setInvalidateOnShutdown);
        this.threadPool = threadPool;
    }

    private void setInvalidateOnShutdown(boolean invalidateOnShutdown) {
        this.invalidateOnShutdown = invalidateOnShutdown;
    }

    @Override
    protected void doStart() {
        // TODO clean up (or rebuild) cache from disk as a node crash may leave cached files
    }

    @Override
    protected void doStop() {
        if (invalidateOnShutdown) {
            cache.invalidateAll();
        }
    }

    @Override
    protected void doClose() {
    }

    /**
     * Creates a new {@link CacheEntry} instance in cache which points to a specific sparse file on disk located within the cache directory.
     *
     * @param file   the Lucene cached file
     * @param length the length of the Lucene file to cache
     * @return a new {@link CacheEntry}
     */
    private CacheEntry getOrAddToCache(final Path file, final long length, final CheckedSupplier<IndexInput, IOException> source) {
        assert Files.notExists(file) : "Lucene cached file exists " + file;
        try {
            return cache.computeIfAbsent(toCacheKey(file), key -> {
                // generate a random UUID for the name of the cache file on disk
                final String uuid = UUIDs.randomBase64UUID();
                // resolve the cache file on disk w/ the expected cached file
                final Path path = file.getParent().resolve(uuid);
                assert Files.notExists(path) : "cache file already exists " + path;

                boolean success = false;
                try {
                    logger.trace(() -> new ParameterizedMessage("creating new cache file for [{}] at [{}]", file.getFileName(), path));
                    final FileChannel channel = FileChannel.open(path, CACHE_FILE_OPEN_OPTIONS);

                    final CacheEntry cacheEntry = new CacheEntry(path, length, source, channel, CACHE_FILE_RANGE_SIZE);
                    success = true;
                    return cacheEntry;
                } catch (IOException e) {
                    logger.error(() -> new ParameterizedMessage("failed to create cache file [{}]", path), e);
                    throw e;
                } finally {
                    if (success == false) {
                        IOUtils.deleteFilesIgnoringExceptions(path);
                    }
                }
            });
        } catch (ExecutionException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    /**
     * Reads bytes from a cached file.
     * <p>
     * The cached file is identified by its path ({@code filePath}). This path is used as a cache key by the cache service to know if an
     * entry already exists in cache for the given file.
     * <p>
     * When no entry exists in cache, the service creates a new entry for the file. Because cached files are created as sparse files on
     * disk, each cache entry needs to track the ranges of bytes that have already been cached and the ranges of bytes that will need to be
     * retrieved from a source. This is tracked in {@link CacheEntry} by {@link SparseFileTracker} which requires to know the length of the
     * file to cache to correctly track ranges.
     * <p>
     * When an entry already exists in cache, the service uses the {@link SparseFileTracker} of the {@link CacheEntry} to know if it can
     * serve the read operations using one or more ranges of bytes already cached on local disk or if it needs to retrieve the bytes from
     * the source (supplied as a {@link IndexInput}) first, then writes the bytes locally before returning them as the result of the read
     * operation.
     *
     * @param filePath   the {@link Path} of the cached file
     * @param fileLength the length of the cached file (required to compute ranges of bytes)
     * @param fileSource supplies the {@link IndexInput} to read the bytes from in case they are not already in cache
     * @param position   the position in the cached file where to start reading bytes from
     * @param b          the array to read bytes into
     * @param off        the offset in the array to start storing bytes
     * @param len        the number of bytes to read
     *
     * @throws IOException if something went wrong
     */
    public void readFromCache(final Path filePath, final long fileLength, final CheckedSupplier<IndexInput, IOException> fileSource,
                              final long position, final byte[] b, int off, int len) throws IOException {
        long pos = position;
        while (len > 0 && lifecycleState() == Lifecycle.State.STARTED) {
            final CacheEntry cacheEntry = getOrAddToCache(filePath, fileLength, fileSource);
            if (cacheEntry.tryIncRef() == false) {
                continue;
            }
            try {
                final int read = cacheEntry.fetchRange(pos, b, off, len);
                logger.trace(() -> new ParameterizedMessage("read {} bytes of file [name:{}, length:{}] at position [{}] from cache",
                    read, filePath.getFileName(), fileLength, position));
                pos += read;
                off += read;
                len -= read;
            } finally {
                cacheEntry.decRef();
            }
        }
    }

    /**
     * Mark the given {@link CacheEntry} as evicted: no new read or write operation can be executed on the file on disk, which will be
     * deleted from disk once all on-going reads are processed.
     */
    private void markAsEvicted(final CacheEntry cacheEntry) {
        logger.trace(() -> new ParameterizedMessage("marking cache entry [{}] as evicted", cacheEntry));
        cacheEntry.markAsEvicted();
    }

    /**
     * Remove from cache all entries that match the given predicate.
     *
     * @param predicate the predicate to evaluate
     */
    public void removeFromCache(final Predicate<String> predicate) {
        if (invalidateOnShutdown) {
            for (String cacheKey : cache.keys()) {
                if (predicate.test(cacheKey)) {
                    cache.invalidate(cacheKey);
                }
            }
        }
        cache.refresh();
    }

    /**
     * Computes the cache key associated to the given Lucene cached file
     *
     * @param cacheFile the cached file
     * @return the cache key
     */
    private static String toCacheKey(final Path cacheFile) {
        return cacheFile.toAbsolutePath().toString();
    }

    private class CacheEntry implements RefCounted {

        private final CheckedSupplier<IndexInput, IOException> source;
        private final SparseFileTracker tracker;
        private final FileChannel channel;
        private final int sizeOfRange;
        private final Path path;

        private final AbstractRefCounted refCounter;
        private volatile boolean evicted;

        CacheEntry(Path path, long length, CheckedSupplier<IndexInput, IOException> source, FileChannel channel, int sizeOfRange) {
            this.tracker = new SparseFileTracker(path.toString(), length);
            this.source = Objects.requireNonNull(source);
            this.channel = Objects.requireNonNull(channel);
            this.path = Objects.requireNonNull(path);
            this.sizeOfRange = sizeOfRange;
            this.refCounter = new AbstractRefCounted(path.toString()) {
                @Override
                protected void closeInternal() {
                    assert refCount() == 0;
                    IOUtils.closeWhileHandlingException(CacheEntry.this.channel);
                    assert evicted || invalidateOnShutdown == false;
                    if (evicted) {
                        logger.trace(() -> new ParameterizedMessage("deleting cache file [{}]", path));
                        IOUtils.deleteFilesIgnoringExceptions(path);
                    }
                }
            };
            this.evicted = false;
        }

        private long estimateWeight() {
            final long lengthOfRanges = tracker.getLengthOfRanges();
            return (lengthOfRanges == 0L) ? sizeOfRange : lengthOfRanges;
        }

        private synchronized void markAsEvicted() {
            if (evicted == false) {
                evicted = true;
                decRef();
            }
        }

        @Override
        public synchronized boolean tryIncRef() {
            if (evicted) {
                return false;
            }
            return refCounter.tryIncRef();
        }

        @Override
        public synchronized void incRef() {
            if (tryIncRef() == false) {
                throw new IllegalStateException("Failed to increment reference counter, cache entry is already evicted");
            }
        }

        @Override
        public synchronized void decRef() {
            refCounter.decRef();
        }

        /**
         * Computes the start and the end of a range to which the given {@code position} belongs.
         *
         * @param position the reading position
         * @return the start and end range positions to fetch
         */
        private Tuple<Long, Long> computeRange(final long position) {
            final long start = (position / sizeOfRange) * sizeOfRange;
            return Tuple.tuple(start, Math.min(start + sizeOfRange, tracker.getLength()));
        }

        /**
         * Fetch the range corresponding to the position from the cache.
         *
         * @param position the position to start reading bytes from.
         * @param buffer   the array to read bytes into
         * @param offset   the offset in the array to start storing bytes
         * @param length   the number of bytes to read
         * @return the number of bytes read
         *
         * @throws IOException if something went wrong
         */
        private  int fetchRange(final long position, final byte[] buffer, final int offset, final int length) throws IOException {
            final CompletableFuture<Integer> future = new CompletableFuture<>();
            assert refCounter.refCount() > 0;

            final Tuple<Long, Long> range = computeRange(position);
            assert range.v2() - range.v1() <= sizeOfRange;

            logger.trace(() -> new ParameterizedMessage("fetching range [{}-{}] of [{}]", range.v1(), range.v2(), path.getFileName()));

            // wait for the range to be available and read it from disk
            final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(range.v1(), range.v2(), new ActionListener<>() {
                @Override
                public void onResponse(Void ignored) {
                    try {
                        final ByteBuffer dst = ByteBuffer.wrap(buffer);
                        dst.position(offset);
                        dst.limit(Math.toIntExact(offset + Math.min(length, range.v2() - position)));
                        final int read = Channels.readFromFileChannel(channel, position, dst);
                        logger.trace(() -> new ParameterizedMessage("read [{}] bytes from [{}]", read, path.getFileName()));
                        future.complete(read);
                    } catch (IOException e) {
                        future.completeExceptionally(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(() ->
                        new ParameterizedMessage("failed to fetch range [{}-{}] of [{}]", range.v1(), range.v2(), path.getFileName()), e);
                    future.completeExceptionally(e);
                }
            });

            if (gaps.isEmpty() == false) {
                fetchMissingRanges(gaps);
            }

            try {
                return future.get();
            } catch (InterruptedException| ExecutionException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new IOException("Failed to fetch range [{}-{}] of [{}]: " + toString(), e);
            }
        }

        /**
         * Fetches all missing ranges
         *
         * @param gaps the missing ranges to fetch
         */
        private void fetchMissingRanges(final List<SparseFileTracker.Gap> gaps) {
            assert gaps.isEmpty() || gaps.size() == 1;
            for (SparseFileTracker.Gap gap : gaps) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        gap.onFailure(e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        fetchAndWriteRange(gap.start, gap.end);
                        gap.onResponse(null);
                    }
                });
            }
        }

        /**
         * Fetches a missing range from the cache entry's source and writes it into the sparse cached file.
         * <p>
         * Even though {@link SparseFileTracker} prevents the same range to be concurrently fetched and written to disk, this method
         * is synchronized for extra safety.
         *
         * @param start the range start to fetch
         * @param end   the range end to fetch
         * @throws IOException if something went wrong
         */
        private synchronized void fetchAndWriteRange(final long start, final long end) throws IOException {
            logger.trace(() -> new ParameterizedMessage("fetching missing range [{}-{}] from source [{}]", start, end, source));

            final int copyBufferSize = 8192;
            byte[] copyBuffer = new byte[copyBufferSize];

            try (IndexInput input = source.get()) {
                if (start > 0) {
                    input.seek(start);
                }
                long remaining = end - start;
                long pos = start;
                while (remaining > 0) {
                    int len = (remaining < copyBufferSize) ? (int) remaining : copyBufferSize;
                    logger.trace(() -> new ParameterizedMessage("reading {} bytes from [{}]", len, input));
                    input.readBytes(copyBuffer, 0, len);

                    logger.trace(() -> new ParameterizedMessage("writing {} bytes range [{}-{}] to [{}]", len, start, end, path));
                    Channels.writeToChannel(copyBuffer, 0, len, channel.position(pos));
                    remaining -= len;
                    pos += len;
                }
            }
        }
    }
}
