/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Node-wide cache for file footer bytes (Parquet footers, ORC postscripts) shared across columnar
 * format readers. Backed by the Elasticsearch {@link Cache} utility which provides:
 * <ul>
 *   <li>LRU eviction with a configurable byte budget via {@link CacheBuilder#setMaximumWeight}</li>
 *   <li>Thundering-herd protection via {@link Cache#computeIfAbsent} — concurrent callers for the
 *       same key coalesce into a single load; exactly one thread performs I/O while others wait
 *       for the result</li>
 *   <li>Time-based expiration via two clocks. The primary clock ({@link CacheBuilder#setExpireAfterAccess})
 *       expires an entry the configured TTL ({@link ExternalSourceCacheSettings#FOOTER_CACHE_TTL}) after it
 *       was last read — a working set that keeps being read keeps its entries, and entries no longer all expire
 *       together at one TTL. A write cap ({@link #WRITE_CAP_MULTIPLIER} × TTL) forces expiry at most
 *       {@code WRITE_CAP_MULTIPLIER × TTL} after an entry was written, regardless of reads. The clocks bound
 *       memory, not staleness: the key carries the object's version, so a rewritten object misses the entry
 *       whatever the clocks — see {@link Key}.</li>
 * </ul>
 *
 * <h2>Why one instance per root format reader instead of per-query?</h2>
 * A per-query cache would avoid cross-query sharing entirely (no need for TTL) and is the
 * cleaner design. However, the current architecture makes it impractical:
 * <ol>
 *   <li>A single query fans out into ~N independent split readers (one per thread), each of which
 *       creates its own {@code ParquetStorageObjectAdapter} or {@code OrcStorageObjectAdapter}.
 *       These adapters share no common parent or query-scoped context — they are created deep in
 *       the format reader call chain ({@code FormatReader.read(StorageObject, ...)}) with no
 *       mechanism to pass shared state between them.</li>
 *   <li>The thundering-herd benefit requires a single cache visible to all concurrent split readers.
 *       Without a shared object, each adapter would issue its own tail read — exactly the redundant
 *       I/O this cache exists to prevent.</li>
 * </ol>
 * Instead, the cache is created once per <em>root</em> format reader (the node-wide lazy
 * singleton {@code FormatReaderRegistry} builds from node {@code Settings}), and every derived
 * reader ({@code withConfig}, {@code withPushedFilter}, ...) shares the root's instance via its
 * copy constructor, so all concurrent queries on a node coalesce on one cache. When a
 * query-scoped context is introduced into the format reader API, this cache should migrate to an
 * instance held by that context. Until then, the version in the key keeps a rewritten object from reading another
 * version's footer, and the two clocks bound memory.
 *
 * <h2>Cache key design</h2>
 * The key is {@code (storageIdentity, path, fileLength, version)}. The version is the object's
 * {@link org.elasticsearch.xpack.esql.datasources.spi.StorageObject#version()}: an ETag, generation or
 * modification time that the listing or a probe already supplied, so no request is spent on it. A rewritten
 * object has a new version, so it misses and its footer is read again. An object without a version is not
 * cached at all. Modification time alone was not used, because it has one-second resolution on some stores,
 * and a same-second rewrite would keep the old version.
 *
 * <p>Cached {@code byte[]} entries must be treated as immutable by all callers.
 */
public class FooterByteCache {

    /**
     * Default max single-entry admission (2 MiB). Oversized footers may still be returned to the
     * caller; {@link #put} skips them and {@link #getOrLoad} evicts them so they do not occupy the
     * LRU. This is not a read limit.
     */
    public static final long DEFAULT_MAX_ENTRY_BYTES = 2L * 1024 * 1024;

    /**
     * The write-cap multiplier: both footer caches are built with a write cap of this many times the
     * access TTL. Shared by {@link ParsedFooterCache} so the two caches always agree on the policy.
     */
    static final long WRITE_CAP_MULTIPLIER = 6L;

    /**
     * Cache key identifying one version of a file by its storage configuration, path, total length and version.
     * The storage identity encodes endpoint and credential identity, so two data sources pointing at different
     * stores never share an entry for an object at the same path with the same length and version. The
     * modification time is excluded so that all range splits of the same version share one cache entry.
     */
    public record Key(StorageIdentity storageIdentity, String path, long fileLength, String version) {

        /**
         * Rejects a null identity: {@link StorageIdentity} is a plugin SPI, and a {@code null} from any
         * implementation would put all of its objects in one shared scope instead of failing.
         */
        public Key {
            Objects.requireNonNull(storageIdentity, "storageIdentity must not be null");
            Objects.requireNonNull(version, "version must not be null");
        }

        /**
         * Creates a key from a {@link org.elasticsearch.xpack.esql.datasources.spi.StorageObject},
         * using its storage identity, path string, version, and
         * {@link org.elasticsearch.xpack.esql.datasources.spi.StorageObject#lengthForFooterCacheKey()}.
         * Prefer this over {@link #keyFor(org.elasticsearch.xpack.esql.datasources.spi.StorageObject, long)} so range
         * views ({@code RangeStorageObject}) share one entry per file.
         *
         * @return the key, or {@code null} when the object has no version, in which case its footer is not cached
         */
        @Nullable
        public static Key keyFor(org.elasticsearch.xpack.esql.datasources.spi.StorageObject storageObject) throws IOException {
            String version = storageObject.version();
            if (version == null) {
                return null;
            }
            return new Key(
                storageObject.storageIdentity(),
                storageObject.path().toString(),
                storageObject.lengthForFooterCacheKey(),
                version
            );
        }

        /**
         * Creates a key from a {@link org.elasticsearch.xpack.esql.datasources.spi.StorageObject},
         * using its path string as the canonical identifier. All callers should prefer this factory
         * over constructing {@code Key} directly so that path canonicalization happens in one place.
         * {@code length} must be the full file size
         * ({@link org.elasticsearch.xpack.esql.datasources.spi.StorageObject#lengthForFooterCacheKey()}),
         * not a range-view span.
         *
         * @return the key, or {@code null} when the object has no version
         */
        @Nullable
        public static Key keyFor(org.elasticsearch.xpack.esql.datasources.spi.StorageObject storageObject, long length) {
            String version = storageObject.version();
            if (version == null) {
                return null;
            }
            return new Key(storageObject.storageIdentity(), storageObject.path().toString(), length, version);
        }
    }

    private final Cache<Key, byte[]> cache;
    private final long maxEntryBytes;
    private final TimeValue expireAfterAccess;
    private final TimeValue expireAfterWrite;

    /**
     * Creates a cache sized from node settings ({@link ExternalSourceCacheSettings#FOOTER_CACHE_SIZE},
     * {@link ExternalSourceCacheSettings#FOOTER_CACHE_TTL}). This is the production entry point,
     * invoked by the format modules from their reader root constructors, which is why it is
     * public while the sizing constructor stays package-private for tests.
     */
    public static FooterByteCache fromSettings(Settings settings) {
        long maxBytes = ExternalSourceCacheSettings.FOOTER_CACHE_SIZE.get(settings).getBytes();
        // The budget is heap-relative, so on small heaps a fixed 2 MiB ceiling would let a single
        // entry evict most of the cache. Never admit an entry larger than a quarter of the budget.
        long maxEntryBytes = Math.max(1L, Math.min(DEFAULT_MAX_ENTRY_BYTES, maxBytes / 4));
        return new FooterByteCache(maxBytes, maxEntryBytes, ExternalSourceCacheSettings.FOOTER_CACHE_TTL.get(settings));
    }

    FooterByteCache(long maxBytes, long maxEntryBytes, TimeValue expireAfterAccess) {
        this.maxEntryBytes = maxEntryBytes;
        this.expireAfterAccess = expireAfterAccess;
        long accessNanos = expireAfterAccess.nanos();
        long capNanos = accessNanos <= Long.MAX_VALUE / WRITE_CAP_MULTIPLIER ? accessNanos * WRITE_CAP_MULTIPLIER : Long.MAX_VALUE;
        this.expireAfterWrite = TimeValue.timeValueNanos(capNanos);
        this.cache = CacheBuilder.<Key, byte[]>builder()
            .setMaximumWeight(maxBytes)
            .setExpireAfterAccess(expireAfterAccess)
            .setExpireAfterWrite(expireAfterWrite)
            .weigher((key, value) -> value.length)
            .build();
    }

    /**
     * The expire-after-access TTL this cache was built with. The paired {@link ParsedFooterCache} is
     * constructed with the same value, but the two no longer age out together: a reader that answers from
     * the parsed cache does not read bytes, so a parsed entry can be renewed while its bytes go cold.
     */
    public TimeValue expireAfterAccess() {
        return expireAfterAccess;
    }

    /** The write cap this cache was built with: six times the access TTL. */
    public TimeValue expireAfterWrite() {
        return expireAfterWrite;
    }

    /** Maximum byte size for a single cache entry. */
    public long maxEntryBytes() {
        return maxEntryBytes;
    }

    /**
     * Returns cached footer bytes for the given key, or loads them via the provided loader.
     * The loader is invoked at most once per key, even under concurrent access — additional
     * callers for the same key wait until the first load completes.
     *
     * <p>If the loaded entry is empty or exceeds {@link #maxEntryBytes()}, it is returned to the
     * caller but immediately evicted so it does not consume cache budget or prevent future loads.
     * This means oversized entries briefly occupy the cache between {@code computeIfAbsent} and
     * {@code invalidate}. The alternative — manual {@code get}/{@code put} — was rejected because
     * it loses thundering-herd protection for <em>all</em> first loads: N concurrent callers
     * seeing {@code get(key) == null} would each invoke the loader independently.
     *
     * @throws ExecutionException if the loader throws an exception or returns null
     */
    public byte[] getOrLoad(Key key, CacheLoader<Key, byte[]> loader) throws ExecutionException {
        byte[] result = cache.computeIfAbsent(key, loader);
        if (result.length == 0 || result.length > maxEntryBytes) {
            cache.invalidate(key);
        }
        return result;
    }

    /**
     * Returns cached footer bytes or {@code null} if not present. Does not start a load,
     * but may block briefly if another thread is currently loading the same key.
     */
    public byte[] get(Key key) {
        return cache.get(key);
    }

    /**
     * Stores footer bytes directly, e.g. from an opportunistic read that reached the end of
     * a file. Entries larger than {@link #maxEntryBytes()} are silently skipped.
     */
    public void put(Key key, byte[] bytes) {
        if (bytes.length > 0 && bytes.length <= maxEntryBytes) {
            cache.put(key, bytes);
        }
    }

    /** Removes all entries. Intended for test isolation. */
    public void invalidateAll() {
        cache.invalidateAll();
    }
}
