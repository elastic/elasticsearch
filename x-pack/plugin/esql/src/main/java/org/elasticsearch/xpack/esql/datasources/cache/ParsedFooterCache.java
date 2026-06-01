/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * JVM-wide cache for parsed file metadata (e.g. Parquet {@code ParquetMetadata}, ORC
 * {@code OrcTail}). Sits at the same architectural layer as {@link FooterByteCache} but stores
 * the result of the format-specific footer parse rather than its raw bytes, so the (typically
 * Thrift/protobuf) deserialization runs at most once per {@code (path, fileLength)} key across:
 * <ul>
 *   <li>concurrent splits of the same file taken by N producer threads;</li>
 *   <li>back-to-back queries against the same file within the access TTL.</li>
 * </ul>
 *
 * <h2>Why parsed metadata and not just raw bytes</h2>
 * {@link FooterByteCache} eliminates redundant tail-byte reads from object storage, but the parse
 * still runs every time a reader is opened: N producers fanning out over a wide file each pay the
 * full deserialization cost, even though the cached bytes are identical. Caching the parsed
 * result collapses that into a single deserialization. {@link FooterByteCache} stays in place
 * because it also serves opportunistic partial-tail reads (page indexes, dictionary tails) that
 * are not full-footer parses, and because format readers fall back to byte reads on a cold cache.
 *
 * <h2>Sharing keys with {@link FooterByteCache}</h2>
 * The cache is keyed by {@link FooterByteCache.Key} ({@code (path, fileLength)}) so that the same
 * key construction logic used to hit the byte cache also hits this cache; both caches stay aligned
 * without an extra key type. Per-format singletons (one for Parquet, one for ORC, etc.) keep the
 * value type concrete and the cache's ownership unambiguous.
 *
 * <h2>Lifecycle</h2>
 * <ul>
 *   <li>Created as a singleton per format reader; no SPI plumbing is required to share entries
 *       across producers since every code path that needs a parsed footer already constructs a
 *       {@link FooterByteCache.Key} via the storage-object adapter.</li>
 *   <li>Access-based TTL — sourced from {@link FooterByteCache#EXPIRE_AFTER_ACCESS_SECONDS} so
 *       the two caches age out together; covers a single query's fan-out (where concurrent splits
 *       keep the entry alive) while ensuring that file modifications between queries trigger a
 *       fresh parse.</li>
 *   <li>Count-based LRU eviction — parsed metadata structures (e.g. Parquet {@code ParquetMetadata}
 *       or ORC {@code OrcTail}) do not expose a cheap byte size, so the cache caps the number of
 *       entries rather than total bytes. <b>Worst-case heap budget</b>: a single parsed footer
 *       for an extreme file (e.g. 100 columns × 200 row groups → ~20k column-chunk entries) can
 *       occupy ~10–20 MiB of heap. With {@link #DEFAULT_MAX_ENTRIES} = {@value #DEFAULT_MAX_ENTRIES}
 *       the absolute worst case is in the hundreds of MiB if every cached entry is for such an
 *       extreme file <i>and</i> all live concurrently inside the TTL window. Typical workloads
 *       (≤10 columns, ≤50 row groups, ≈200 KiB per entry) sit at a few MiB total. Adding a real
 *       weigher would require an estimator pass over the parsed structure on every {@code put} —
 *       not implemented here, but tracked for follow-up if heap pressure shows up in production.
 *       Note that the byte and parsed caches evict independently — TTL alignment keeps them
 *       timing-consistent but does not synchronize eviction events.</li>
 * </ul>
 *
 * <p>Cached values must be treated as immutable by all callers — callers that need to derive a
 * filtered view (e.g. only the row groups for a specific byte range) should build a new value
 * from the cached one rather than mutating the cached structure.</p>
 *
 * @param <T> the parsed metadata type held by this cache (e.g. {@code ParquetMetadata}).
 */
public final class ParsedFooterCache<T> {

    /**
     * Default maximum number of cached parsed footers across the JVM. Sized small enough that the
     * worst-case heap (see class Javadoc) stays bounded even when extremely wide files dominate
     * the working set, while still being large enough to absorb the typical fan-out of one query
     * over many distinct files.
     */
    public static final int DEFAULT_MAX_ENTRIES = 32;

    private final Cache<FooterByteCache.Key, T> cache;

    /** Creates a cache with the default maximum entry count. */
    public ParsedFooterCache() {
        this(DEFAULT_MAX_ENTRIES);
    }

    /**
     * Creates a cache with the given maximum entry count. Exposed for tests; production callers
     * should rely on {@link #DEFAULT_MAX_ENTRIES}.
     *
     * @throws IllegalArgumentException if {@code maxEntries <= 0}
     */
    public ParsedFooterCache(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive, got [" + maxEntries + "]");
        }
        // Single-source the TTL from FooterByteCache so the byte and parsed caches always age out
        // together — if the bytes are stale, the parse derived from them is stale too.
        this.cache = CacheBuilder.<FooterByteCache.Key, T>builder()
            .setMaximumWeight(maxEntries)
            .setExpireAfterAccess(TimeValue.timeValueSeconds(FooterByteCache.EXPIRE_AFTER_ACCESS_SECONDS))
            .build();
    }

    /**
     * Returns the cached parsed footer for the given key, or loads it via {@code loader}. The
     * loader is invoked at most once per key under concurrent access — additional callers for the
     * same key block until the first load completes and then receive its result. This is the
     * thundering-herd protection that lets a single producer parse the footer while N siblings
     * skip the parse entirely.
     *
     * @throws ExecutionException if the loader throws an exception or returns null
     */
    public T getOrLoad(FooterByteCache.Key key, CacheLoader<FooterByteCache.Key, T> loader) throws ExecutionException {
        return cache.computeIfAbsent(key, loader);
    }

    /**
     * Returns the cached parsed footer or {@code null} if not present. Does not start a load, but
     * may block briefly if another thread is currently loading the same key (consistent with
     * {@link FooterByteCache#get}).
     */
    public T get(FooterByteCache.Key key) {
        return cache.get(key);
    }

    /** Removes all entries. Intended for test isolation. */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    /**
     * Unwraps an {@link ExecutionException} thrown by {@link #getOrLoad} and reshapes the
     * <i>structural</i> failure modes back to their original types so callers see the same shapes
     * they would have seen had they parsed the footer synchronously: {@link Error} (including
     * {@code OutOfMemoryError}, given highest priority so JVM-level failures are never silently
     * swallowed), {@link IOException}, {@link CircuitBreakingException}, and
     * {@link ElasticsearchException}.
     *
     * <p>Any other cause — typically a plain {@link RuntimeException} from a format library
     * indicating a malformed file — is returned for format-specific wrapping. The two readers
     * differ here: Parquet wraps every such cause in {@code newInvalidParquetFileException}, ORC
     * rethrows it directly. Keeping that policy out of this helper avoids dragging
     * format-specific factories into the shared cache.
     *
     * <p>Always invoke as {@code throw new ...(rethrowStructural(e))} or
     * {@code throw rethrowStructural(e)} so the compiler treats the call as terminal — see the
     * callers in the format readers.
     *
     * @return the original cause (or the {@code ExecutionException} itself if the cause is null),
     *         for the caller to either rethrow or wrap
     * @throws IOException             if the cause is an {@link IOException}
     * @throws CircuitBreakingException if the cause is a {@link CircuitBreakingException}
     * @throws ElasticsearchException  if the cause is an {@link ElasticsearchException}
     */
    public static Throwable rethrowStructural(ExecutionException e) throws IOException {
        Throwable cause = e.getCause();
        ExceptionsHelper.maybeError(cause).ifPresent(error -> { throw error; });
        if (cause instanceof IOException io) {
            throw io;
        }
        if (cause instanceof CircuitBreakingException cbe) {
            throw cbe;
        }
        if (cause instanceof ElasticsearchException ese) {
            throw ese;
        }
        return cause != null ? cause : e;
    }
}
