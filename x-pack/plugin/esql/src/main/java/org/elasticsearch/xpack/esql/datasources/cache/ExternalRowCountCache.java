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
 * Per-file row-count cache for line-oriented external text formats. Key is {@code (path, mtime)};
 * value is the row count. Lets {@code PushStatsToExternalSource} short-circuit {@code COUNT(*)}.
 * Coarse-resolution mtime collisions are bounded by {@link FooterByteCache#EXPIRE_AFTER_ACCESS_SECONDS}.
 */
public final class ExternalRowCountCache {

    /** mtime (epoch millis) published into {@code SourceMetadata.sourceMetadata()} so the warm-path cache lookup can avoid a storage round-trip. */
    public static final String MTIME_MILLIS_KEY = "_stats.file_mtime_millis";

    private static final int MAX_ENTRIES = 10_000;

    private record Key(String path, long mtimeMillis) {}

    private static final Cache<Key, Long> CACHE = CacheBuilder.<Key, Long>builder()
        .setMaximumWeight(MAX_ENTRIES)
        .setExpireAfterAccess(TimeValue.timeValueSeconds(FooterByteCache.EXPIRE_AFTER_ACCESS_SECONDS))
        .build();

    private ExternalRowCountCache() {}

    public static OptionalLong lookup(StorageObject object) {
        try {
            Instant mtime = object.lastModified();
            if (mtime == null) {
                return OptionalLong.empty();
            }
            return lookup(object.path().toString(), mtime.toEpochMilli());
        } catch (Exception e) {
            // IOException from lastModified() or any cache-internal failure → miss.
            return OptionalLong.empty();
        }
    }

    /** Overload used by the warm-query path, where mtime is already resolved from the cached schema entry. */
    public static OptionalLong lookup(String path, long mtimeMillis) {
        try {
            Long v = CACHE.get(new Key(path, mtimeMillis));
            return v == null ? OptionalLong.empty() : OptionalLong.of(v);
        } catch (Exception e) {
            return OptionalLong.empty();
        }
    }

    /** The gate (whole-file, natural EOF, zero errors) is the caller's. Null mtime drops the write — no trusted identity. */
    public static void put(StorageObject object, long rowCount) {
        try {
            Instant mtime = object.lastModified();
            if (mtime == null) {
                return;
            }
            CACHE.put(new Key(object.path().toString(), mtime.toEpochMilli()), rowCount);
        } catch (Exception e) {
            // Cache write failures degrade silently — next query repopulates.
        }
    }

    /** Test isolation. */
    public static void clearForTests() {
        CACHE.invalidateAll();
    }
}
