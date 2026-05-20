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
 * once a file has been drained at least once.
 * <p>
 * Key is {@code (path, length, mtimeMillis)}. Including mtime makes same-length file mutations
 * resolve to a fresh key automatically — no cross-cache invalidation hook, so this JVM-static
 * cache stays correct when warm queries route to a different node from the cold producer.
 * {@link FooterByteCache#EXPIRE_AFTER_ACCESS_SECONDS} bounds residual staleness in the brief
 * window where two writes share both length and mtime resolution.
 */
public final class ExternalRowCountCache {

    /**
     * Well-known key under which line-oriented text readers publish file mtime (epoch millis) into
     * {@code SourceMetadata.sourceMetadata()}, so {@code ExternalSourceResolver.buildMetadataFromCache}
     * can reconstruct the cache key on warm queries without a fresh storage call.
     */
    public static final String MTIME_MILLIS_KEY = "_stats.file_mtime_millis";

    private static final int MAX_ENTRIES = 10_000;

    private record Key(String path, long length, long mtimeMillis) {}

    private static final Cache<Key, Long> CACHE = CacheBuilder.<Key, Long>builder()
        .setMaximumWeight(MAX_ENTRIES)
        .setExpireAfterAccess(TimeValue.timeValueSeconds(FooterByteCache.EXPIRE_AFTER_ACCESS_SECONDS))
        .build();

    private ExternalRowCountCache() {}

    public static OptionalLong lookup(StorageObject object) {
        try {
            Instant mtime = object.lastModified();
            return lookup(object.path().toString(), object.length(), mtime == null ? 0L : mtime.toEpochMilli());
        } catch (Exception e) {
            return OptionalLong.empty();
        }
    }

    /** Overload used by the warm-query path, where length + mtime are already resolved from the cached schema entry. */
    public static OptionalLong lookup(String path, long length, long mtimeMillis) {
        try {
            Long v = CACHE.get(new Key(path, length, mtimeMillis));
            return v == null ? OptionalLong.empty() : OptionalLong.of(v);
        } catch (Exception e) {
            return OptionalLong.empty();
        }
    }

    /**
     * Write the row count. The gate (whole-file context, natural EOF, zero observed parse errors)
     * lives at the caller — see the iterator {@code close()} paths.
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
