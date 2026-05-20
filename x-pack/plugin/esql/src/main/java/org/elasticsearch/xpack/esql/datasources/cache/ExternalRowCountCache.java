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

import java.util.OptionalLong;

/**
 * Per-file row-count cache for line-oriented external text formats (CSV, TSV, NDJSON). Lets
 * {@code PushStatsToExternalSource} short-circuit {@code COUNT(*)} to a {@code LocalSourceExec}
 * after the file has been seen at least once.
 * <p>
 * Identity is {@code (path, length)} — matching {@code FooterByteCache.Key} for Parquet. The
 * {@code (path, length)} convention is shared with {@code FooterByteCache}, {@code ParsedFooterCache}
 * and {@code ExternalSourceCacheService.schemaCache}; each of those bounds same-length-mutation
 * staleness with an {@code expireAfter*} TTL rather than treating identity as proof of immutability.
 * This cache inherits the same TTL discipline via {@link FooterByteCache#EXPIRE_AFTER_ACCESS_SECONDS}
 * so a file rewritten between queries with the same byte length stops serving its stale row count
 * once the entry idles past the access window. Within a single query, concurrent splits keep the
 * entry alive via access-time resets, so the warm short-circuit path is unaffected.
 */
public final class ExternalRowCountCache {

    private static final Cache<FooterByteCache.Key, Long> CACHE = CacheBuilder.<FooterByteCache.Key, Long>builder()
        .setMaximumWeight(10_000)
        .setExpireAfterAccess(TimeValue.timeValueSeconds(FooterByteCache.EXPIRE_AFTER_ACCESS_SECONDS))
        .build();

    private ExternalRowCountCache() {}

    /** Look up a cached row count by file identity. */
    public static OptionalLong lookup(StorageObject object) {
        try {
            Long v = CACHE.get(FooterByteCache.Key.keyFor(object, object.length()));
            return v == null ? OptionalLong.empty() : OptionalLong.of(v);
        } catch (Exception e) {
            // IOException from object.length() or any cache-internal failure → miss.
            return OptionalLong.empty();
        }
    }

    /**
     * Record a row count for a fully-drained file. The gate (whole-file context, natural EOF,
     * zero parse errors observed) lives at the caller — see the iterator {@code close()} paths in
     * {@code CsvBatchIterator} and {@code NdJsonPageIterator}. This method writes unconditionally.
     */
    public static void put(StorageObject object, long rowCount) {
        try {
            CACHE.put(FooterByteCache.Key.keyFor(object, object.length()), rowCount);
        } catch (Exception e) {
            // Cache write failures degrade silently — next query repopulates.
        }
    }

    /** Test isolation. */
    public static void clearForTests() {
        CACHE.invalidateAll();
    }
}
