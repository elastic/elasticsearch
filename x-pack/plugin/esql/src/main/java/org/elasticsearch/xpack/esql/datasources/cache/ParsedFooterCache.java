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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.ToLongFunction;

/**
 * Node-wide cache for parsed file metadata (e.g., Parquet {@code ParquetMetadata}, ORC
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
 * without an extra key type. Per-format instances (one for Parquet, one for ORC, etc.) keep the
 * value type concrete and the cache's ownership unambiguous.
 *
 * <h2>Lifecycle</h2>
 * <ul>
 *   <li>Created once per <em>root</em> format reader (the node-wide lazy singleton
 *       {@code FormatReaderRegistry} builds from node {@code Settings}) and shared by every
 *       derived reader via the reader copy constructors, exactly like the paired
 *       {@link FooterByteCache} (see its class Javadoc for why not per-query).</li>
 *   <li>Access-based TTL: constructed with the same value as the paired {@link FooterByteCache}
 *       so the two caches age out together; covers a single query's fan-out (where concurrent
 *       splits keep the entry alive) and bounds cross-query staleness: the key carries no
 *       modification time, so a same-length overwrite may be served until the TTL lapses.</li>
 *   <li>Byte-weighted LRU eviction: parsed metadata structures do not expose an exact byte
 *       size, so each format supplies a structural estimator (row groups × columns for Parquet,
 *       the analogous stripe shape for ORC) against a heap-relative budget
 *       ({@link ExternalSourceCacheSettings#FOOTER_PARSED_CACHE_SIZE}). Estimator precision only
 *       affects budget utilization, never correctness. An entry whose estimate exceeds the whole
 *       budget is refused rather than cached (see {@link #put} and {@link #getOrLoad}), because
 *       such an entry can only displace the entire working set and then itself. Note that the byte
 *       and parsed caches evict independently. TTL alignment keeps them timing-consistent but does
 *       not synchronize eviction events.</li>
 * </ul>
 *
 * <p>Cached values must be treated as immutable by all callers — callers that need to derive a
 * filtered view (e.g. only the row groups for a specific byte range) should build a new value
 * from the cached one rather than mutating the cached structure. Callers that already hold a
 * parsed footer can {@link #put} it so a later {@link #getOrLoad} skips deserialization;
 * inserted values must be the complete file metadata, not a range- or projection-filtered
 * derivative.</p>
 *
 * @param <T> the parsed metadata type held by this cache (e.g. {@code ParquetMetadata}).
 */
public final class ParsedFooterCache<T> {

    /**
     * Pairs a parsed footer with its weight, computed once at insertion. The backing {@link Cache}
     * re-invokes its weigher on every LRU link/unlink (including the relink performed on each
     * cache <em>hit</em>) under the global LRU lock, so a weigher that walks the footer structure
     * would run on every hot-path access. Storing the precomputed weight makes those calls O(1).
     */
    private record Weighted<T>(T value, long weight) {}

    private final Cache<FooterByteCache.Key, Weighted<T>> cache;
    private final ConcurrentMap<FooterByteCache.Key, CompletableFuture<Weighted<T>>> inFlightLoads = new ConcurrentHashMap<>();
    private final ToLongFunction<T> weigher;
    private final long maxWeightBytes;

    /**
     * Creates a cache sized from node settings
     * ({@link ExternalSourceCacheSettings#FOOTER_PARSED_CACHE_SIZE},
     * {@link ExternalSourceCacheSettings#FOOTER_CACHE_TTL}). This is the production entry point,
     * invoked by the format modules from their reader root constructors.
     *
     * @param weigher estimates a parsed footer's heap footprint in bytes; invoked once per entry
     *                at insertion (the computed weight is stored alongside the entry, so LRU
     *                maintenance never re-walks the structure). Must be cheap (walk counts, not
     *                object graphs) and must never return a negative value.
     */
    public static <T> ParsedFooterCache<T> fromSettings(Settings settings, ToLongFunction<T> weigher) {
        return new ParsedFooterCache<>(
            ExternalSourceCacheSettings.FOOTER_PARSED_CACHE_SIZE.get(settings).getBytes(),
            ExternalSourceCacheSettings.FOOTER_CACHE_TTL.get(settings),
            weigher
        );
    }

    /**
     * Creates a cache with an explicit byte budget and TTL. Exposed for tests; production callers
     * should go through {@link #fromSettings}.
     *
     * @throws IllegalArgumentException if {@code maxWeightBytes <= 0}
     */
    public ParsedFooterCache(long maxWeightBytes, TimeValue expireAfterAccess, ToLongFunction<T> weigher) {
        if (maxWeightBytes <= 0) {
            throw new IllegalArgumentException("maxWeightBytes must be positive, got [" + maxWeightBytes + "]");
        }
        this.weigher = weigher;
        this.maxWeightBytes = maxWeightBytes;
        this.cache = CacheBuilder.<FooterByteCache.Key, Weighted<T>>builder()
            .setMaximumWeight(maxWeightBytes)
            .setExpireAfterAccess(expireAfterAccess)
            .weigher((key, value) -> value.weight())
            .build();
    }

    /**
     * Returns the cached parsed footer for the given key, or loads it via {@code loader}. The
     * loader is invoked at most once per key under concurrent access — additional callers for the
     * same key block until the first load completes and then receive its result. This is the
     * thundering-herd protection that lets a single producer parse the footer while N siblings
     * skip the parse entirely.
     *
     * <p>Loads are coordinated outside the backing cache so the value can be weighed before cache
     * admission. An entry heavier than the whole budget is returned to the loading caller and any
     * concurrent waiters, but is not inserted and cannot evict the existing working set.
     *
     * @throws ExecutionException if the loader throws an exception or returns null
     */
    public T getOrLoad(FooterByteCache.Key key, CacheLoader<FooterByteCache.Key, T> loader) throws ExecutionException {
        Weighted<T> cached = cache.get(key);
        if (cached != null) {
            return cached.value();
        }

        CompletableFuture<Weighted<T>> newLoad = new CompletableFuture<>();
        CompletableFuture<Weighted<T>> inFlight = inFlightLoads.putIfAbsent(key, newLoad);
        if (inFlight != null) {
            return awaitLoad(inFlight).value();
        }

        try {
            cached = cache.get(key);
            if (cached != null) {
                newLoad.complete(cached);
                return cached.value();
            }

            T loaded = loader.load(key);
            if (loaded == null) {
                throw new NullPointerException("loader returned a null value");
            }
            Weighted<T> weighted = wrap(loaded);
            if (weighted.weight() <= maxWeightBytes) {
                Weighted<T> admitted = weighted;
                weighted = cache.computeIfAbsent(key, ignored -> admitted);
            }
            newLoad.complete(weighted);
            return weighted.value();
        } catch (Exception e) {
            newLoad.completeExceptionally(e);
            throw new ExecutionException(e);
        } catch (Error e) {
            newLoad.completeExceptionally(e);
            throw e;
        } finally {
            inFlightLoads.remove(key, newLoad);
        }
    }

    private static <T> Weighted<T> awaitLoad(CompletableFuture<Weighted<T>> inFlight) throws ExecutionException {
        try {
            return inFlight.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ExecutionException(e);
        }
    }

    /**
     * Returns the cached parsed footer or {@code null} if not present. Does not start a load, but
     * may block briefly if another thread is currently loading the same key (consistent with
     * {@link FooterByteCache#get}).
     */
    public T get(FooterByteCache.Key key) {
        Weighted<T> cached = cache.get(key);
        return cached == null ? null : cached.value();
    }

    /**
     * Stores an already-parsed footer for {@code key}, e.g. after an opportunistic tail parse.
     * Prefer this over {@code getOrLoad(key, k -> value)} when the caller already holds the
     * object: it avoids a checked {@link ExecutionException} and reads as an intentional seed.
     * A value whose weight exceeds the cache's whole byte budget is silently skipped, mirroring
     * {@link FooterByteCache#put}'s per-entry ceiling; anything that fits is inserted and the
     * byte-weighted LRU may evict it later.
     *
     * @throws IllegalArgumentException if {@code key} or {@code value} is null
     */
    public void put(FooterByteCache.Key key, T value) {
        if (key == null) {
            throw new IllegalArgumentException("cache key must not be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("parsed footer value must not be null");
        }
        Weighted<T> weighted = wrap(value);
        // An entry heavier than the entire budget can never be retained: the backing Cache links it
        // at the LRU head and then evicts from the tail while weight > maximumWeight, so it discards
        // every other footer, including the tiny-file working set later phases were meant to reuse,
        // before discarding itself and leaving the cache empty. Refuse it up front instead.
        if (weighted.weight() > maxWeightBytes) {
            return;
        }
        cache.put(key, weighted);
    }

    private Weighted<T> wrap(T value) {
        return new Weighted<>(value, weigher.applyAsLong(value));
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
