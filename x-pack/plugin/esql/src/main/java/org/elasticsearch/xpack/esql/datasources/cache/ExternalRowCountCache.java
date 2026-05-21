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
 * Key is {@code (path, mtimeMillis)}. mtime is the canonical "file version" discriminator on every
 * production filesystem and object store — it advances on every write, no extra storage round-trip,
 * works for stream-only compression formats (bzip2, zstd-streamed) where decompressed length is
 * unknown. {@link FooterByteCache#EXPIRE_AFTER_ACCESS_SECONDS} bounds residual staleness on the
 * coarsest filesystems (1-2s mtime resolution) where two writes inside the same second could share
 * an mtime value.
 */
public final class ExternalRowCountCache {

    /**
     * Well-known key under which line-oriented text readers publish file mtime (epoch millis) into
     * {@code SourceMetadata.sourceMetadata()}, so {@code ExternalSourceResolver.buildMetadataFromCache}
     * can reconstruct the cache key on warm queries without a fresh storage call.
     */
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

    /**
     * Write the row count. The gate (whole-file context, natural EOF, zero observed parse errors)
     * lives at the caller — see the iterator {@code close()} paths. A {@code null} mtime (e.g.
     * an HTTP source without a {@code Last-Modified} header) drops the write — no identity we
     * can trust, so we never serve a count keyed on it.
     */
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
