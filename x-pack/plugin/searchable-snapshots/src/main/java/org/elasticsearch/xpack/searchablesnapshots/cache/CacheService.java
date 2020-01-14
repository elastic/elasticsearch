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
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
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

    private static final int CACHE_FILE_RANGE_SIZE = 1 << 15;

    private static final Set<StandardOpenOption> CACHE_FILE_OPEN_OPTIONS =
        Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE/*, StandardOpenOption.SPARSE*/);

    private static final Logger logger = LogManager.getLogger(CacheService.class);

    private final Cache<String, CacheFile> cache;

    public CacheService(final Settings settings) {
        this.cache = CacheBuilder.<String, CacheFile>builder()
            .setMaximumWeight(SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes())
            .weigher((key, entry) -> entry.getLength())
            .removalListener(notification -> markAsEvicted(notification.getValue()))
            .build();
    }

    @Override
    protected void doStart() {
        // NORELEASE TODO clean up (or rebuild) cache from disk as a node crash may leave cached files
    }

    @Override
    protected void doStop() {
        cache.invalidateAll();
    }

    @Override
    protected void doClose() {
    }

    /**
     * Creates a new {@link CacheFile} instance in cache which points to a specific sparse file on disk located within the cache directory.
     *
     * @param file   the Lucene cached file
     * @param length the length of the Lucene file to cache
     * @return a new {@link CacheFile}
     */
    private CacheFile getOrAddToCache(final Path file, final long length) {
        assert Files.notExists(file) : "Lucene cached file exists " + file;
        try {
            return cache.computeIfAbsent(toCacheKey(file), key -> {
                // generate a random UUID for the name of the cache file on disk
                final String uuid = UUIDs.randomBase64UUID();
                // resolve the cache file on disk w/ the expected cached file
                final Path path = file.getParent().resolve(uuid);
                assert Files.notExists(path) : "cache file already exists " + path;

                final String fileName = file.getFileName().toString();
                return new CacheFile(fileName, length, path, CACHE_FILE_OPEN_OPTIONS, CACHE_FILE_RANGE_SIZE);
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
     * retrieved from a source. This is tracked in {@link CacheFile} by {@link SparseFileTracker} which requires to know the length of the
     * file to cache to correctly track ranges.
     * <p>
     * When an entry already exists in cache, the service uses the {@link SparseFileTracker} of the {@link CacheFile} to know if it can
     * serve the read operations using one or more ranges of bytes already cached on local disk or if it needs to retrieve the bytes from
     * the source (supplied as a {@link IndexInput}) first, then writes the bytes locally before returning them as the result of the read
     * operation.
     *
     * @param filePath       the {@link Path} of the cached file
     * @param fileLength     the length of the cached file (required to compute ranges of bytes)
     * @param cacheReference the last reference to a cache entry
     * @param cacheSource    supplies the {@link IndexInput} to read the bytes from in case they are not already in cache
     * @param position       the position in the cached file where to start reading bytes from
     * @param b              the array to read bytes into
     * @param offset         the offset in the array to start storing bytes
     * @param length         the number of bytes to read
     * @return a {@link Releasable} that must be released by the caller to indicate that it's done with the current cache file.
     * @throws IOException if something went wrong
     */
    public Releasable readFromCache(final Path filePath, final long fileLength,
                                    final Releasable cacheReference, final CheckedSupplier<IndexInput, IOException> cacheSource,
                                    final long position, final byte[] b, final int offset, final int length) throws IOException {
        long pos = position;
        int off = offset;
        int len = length;

        Releasable previousCacheFile = cacheReference;
        while (len > 0) {
            final Lifecycle.State state = lifecycleState();
            if (state != Lifecycle.State.STARTED) {
                throw new IOException("Failed to read data from cache: cache service is [" + state + "]");
            }

            int read;

            final CacheFile cacheFile = getOrAddToCache(filePath, fileLength);
            if (cacheFile.tryIncRef()) {
                read = fetchRange(cacheFile, cacheSource, pos, b, off, len);
                if (previousCacheFile != null) {
                    Releasables.close(previousCacheFile);
                }
                previousCacheFile = Releasables.releaseOnce(cacheFile::decRef);

            } else {
                // the cache entry provided by the cache is already evicted:
                // fill the buffer by reading the source directly
                final ByteBuffer dest = ByteBuffer.wrap(b, off, len);
                read = copySourceTo(cacheSource, pos, pos + len, (src, p) -> {
                    assert dest.remaining() >= src.remaining();
                    dest.put(src);
                });
            }

            pos += read;
            off += read;
            len -= read;
        }
        assert len == 0 : "partial read operation [" + len + "]";
        return previousCacheFile;
    }

    /**
     * Mark the given {@link CacheFile} as evicted: no new read or write operation can be executed on the file on disk, which will be
     * deleted from disk once all on-going reads are processed.
     */
    private void markAsEvicted(final CacheFile cacheFile) {
        logger.trace(() -> new ParameterizedMessage("marking cache entry [{}] as evicted", cacheFile));
        cacheFile.markAsEvicted();
    }

    /**
     * Remove from cache all entries that match the given predicate.
     *
     * @param predicate the predicate to evaluate
     */
    public void removeFromCache(final Predicate<String> predicate) {
        for (String cacheKey : cache.keys()) {
            if (predicate.test(cacheKey)) {
                cache.invalidate(cacheKey);
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
    private int fetchRange(final CacheFile cacheFile, final CheckedSupplier<IndexInput, IOException> fileSource,
                           final long position, final byte[] buffer, final int offset, final int length) throws IOException {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        assert cacheFile.refCount() > 0;

        final Tuple<Long, Long> range = cacheFile.computeRange(position);
        assert range.v2() - range.v1() <= CACHE_FILE_RANGE_SIZE;

        final ActionListener<Void> listener = ActionListener.wrap(aVoid -> {
                final ByteBuffer dst = ByteBuffer.wrap(buffer, offset, Math.toIntExact(Math.min(length, range.v2() - position)));
                future.complete(cacheFile.readFrom(dst, position));
            },
            e -> {
                try {
                    // something went wrong when processing the range, try to serve the read operation directly from source
                    final ByteBuffer dst = ByteBuffer.wrap(buffer, offset, Math.toIntExact(Math.min(length, range.v2() - position)));
                    future.complete(
                        copySourceTo(fileSource, position, position + dst.remaining(), (src, pos) -> {
                            assert dst.remaining() >= src.remaining();
                            dst.put(src);
                        }));
                } catch (IOException ex) {
                    logger.error(() ->
                        new ParameterizedMessage("failed to fetch range [{}-{}] of [{}]", range.v1(), range.v2(), cacheFile.getName()), e);
                    future.completeExceptionally(ex);

                }
            });

        logger.trace(() -> new ParameterizedMessage("fetching range [{}-{}] of [{}]", range.v1(), range.v2(), cacheFile.getName()));

        final List<SparseFileTracker.Gap> gaps = cacheFile.waitForRange(range.v1(), range.v2(), listener);
        if (gaps.isEmpty() == false) {
            assert gaps.isEmpty() || gaps.size() == 1;
            for (SparseFileTracker.Gap gap : gaps) {
                try {
                    copySourceTo(fileSource, gap.start, gap.end, cacheFile::writeTo);
                    gap.onResponse(null);
                } catch (IOException e) {
                    gap.onFailure(e);
                }
            }
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
     * Reads the cache entry's source from the {@code start} position til the {@code end} position using an internal copy buffer and
     * passes it to the {@code copy} consumer every time the copy buffer is filled.
     *
     * @param start the position to start reading the source from
     * @param end   the position to stop reading the source from
     * @param copy  a consumer to consumer the bytes read
     * @return the total bytes read
     * @throws IOException if something went wrong
     */
    private int copySourceTo(final CheckedSupplier<IndexInput, IOException> source,
                             long start, long end, CheckedBiConsumer<ByteBuffer, Long, IOException> copy) throws IOException {
        final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(8192L, end - start))];

        int bytesCopied = 0;
        try (IndexInput input = source.get()) {
            if (start > 0) {
                input.seek(start);
            }
            long remaining = end - start;
            long pos = start;
            while (remaining > 0) {
                final int len = (remaining < copyBuffer.length) ? (int) remaining : copyBuffer.length;
                input.readBytes(copyBuffer, 0, len);

                copy.accept(ByteBuffer.wrap(copyBuffer).position(0).limit(len), pos);
                bytesCopied += len;
                remaining -= len;
                pos += len;
            }
        }
        return bytesCopied;
    }
}
