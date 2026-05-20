/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.time.Instant;
import java.util.OptionalLong;

/**
 * Per-file row-count cache for line-oriented external text formats (CSV, TSV, NDJSON). Lets
 * {@code PushStatsToExternalSource} short-circuit {@code COUNT(*)} to a {@code LocalSourceExec}
 * after the file has been seen at least once.
 * <p>
 * Identity is {@code (path, length, mtimeMillis)}. Path + length alone (matching
 * {@code FooterByteCache.Key}) can't distinguish a same-length file mutation, and an
 * invalidation-on-schema-cache-miss approach races in multi-node clusters (the schema cache is
 * per-node, this cache is JVM-static; a warm query routed to a different node would
 * incorrectly invalidate). Including mtime turns "file changed but length is identical" into
 * an automatic cache miss across every node, no cross-cache coordination needed. The
 * access-based TTL from {@link FooterByteCache#EXPIRE_AFTER_ACCESS_SECONDS} bounds residual
 * staleness inside the brief window where two clients write versions with the same length and
 * same mtime resolution.
 */
public final class ExternalRowCountCache {

    /**
     * Well-known key under which file last-modified time (as epoch millis) is published into
     * {@code SourceMetadata.sourceMetadata()} by line-oriented text readers. The warm-path
     * augmentation in {@code ExternalSourceResolver.buildMetadataFromCache} reads this to
     * complete the {@code (path, length, mtime)} cache lookup without a fresh storage round-trip.
     */
    public static final String MTIME_MILLIS_KEY = "_stats.file_mtime_millis";

    /** Bound on the number of cache entries. */
    private static final int MAX_ENTRIES = 10_000;

    /**
     * Cache key. {@code mtimeMillis} is the file's last-modified time at write/read time; including
     * it makes same-length mutations resolve to a different key without a cross-cache invalidation
     * hook (and so works correctly in multi-node clusters where each node's schema-cache state is
     * independent of this JVM-static cache).
     */
    public record Key(String path, long length, long mtimeMillis) {}

    private static final Cache<Key, Long> CACHE = CacheBuilder.<Key, Long>builder()
        .setMaximumWeight(MAX_ENTRIES)
        .setExpireAfterAccess(TimeValue.timeValueSeconds(FooterByteCache.EXPIRE_AFTER_ACCESS_SECONDS))
        .build();

    private ExternalRowCountCache() {}

    /** Look up a cached row count by full file identity. */
    public static OptionalLong lookup(StorageObject object) {
        try {
            Instant mtime = object.lastModified();
            return lookup(object.path().toString(), object.length(), mtime == null ? 0L : mtime.toEpochMilli());
        } catch (Exception e) {
            // IOException from object.length() / lastModified() or any cache-internal failure → miss.
            return OptionalLong.empty();
        }
    }

    /**
     * Look up a cached row count given already-resolved key components. Used by the warm-query
     * optimizer path where the {@code SchemaCacheEntry} already carries length + mtime, avoiding
     * a fresh storage round-trip on every query.
     */
    public static OptionalLong lookup(String path, long length, long mtimeMillis) {
        try {
            Long v = CACHE.get(new Key(path, length, mtimeMillis));
            return v == null ? OptionalLong.empty() : OptionalLong.of(v);
        } catch (Exception e) {
            return OptionalLong.empty();
        }
    }

    /**
     * Record a row count for a fully-drained file. The gate (whole-file context, natural EOF,
     * zero parse errors observed) lives at the caller — see the iterator {@code close()} paths in
     * {@code CsvBatchIterator} and {@code NdJsonPageIterator}. This method writes unconditionally
     * once invoked.
     */
    public static void put(StorageObject object, long rowCount) {
        try {
            Instant mtime = object.lastModified();
            CACHE.put(new Key(object.path().toString(), object.length(), mtime == null ? 0L : mtime.toEpochMilli()), rowCount);
        } catch (Exception e) {
            // Cache write failures degrade silently — next query repopulates.
        }
    }

    /** Test isolation. */
    public static void clearForTests() {
        CACHE.invalidateAll();
    }
}
