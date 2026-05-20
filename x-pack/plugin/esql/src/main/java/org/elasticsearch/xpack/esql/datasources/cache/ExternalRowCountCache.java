/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.util.OptionalLong;

/**
 * Per-file row-count cache for line-oriented external text formats (CSV, TSV, NDJSON). Lets
 * {@code PushStatsToExternalSource} short-circuit {@code COUNT(*)} to a {@code LocalSourceExec}
 * after the file has been seen at least once.
 * <p>
 * Identity is {@code (path, length)} — matching {@code FooterByteCache.Key}'s convention for
 * Parquet. The codebase-wide assumption is that a file at a given {@code (path, length)} is
 * immutable; the same assumption already underpins {@code ParsedFooterCache}, the schema cache and
 * split-byte-range planning.
 */
public final class ExternalRowCountCache {

    private record Key(String path, long length) {}

    private static final Cache<Key, Long> CACHE = CacheBuilder.<Key, Long>builder().setMaximumWeight(10_000).build();

    private ExternalRowCountCache() {}

    /** Look up a cached row count by file identity. */
    public static OptionalLong lookup(StorageObject object) {
        try {
            Long v = CACHE.get(new Key(object.path().toString(), object.length()));
            return v == null ? OptionalLong.empty() : OptionalLong.of(v);
        } catch (Exception e) {
            return OptionalLong.empty();
        }
    }

    /**
     * Record an authoritative row count for a fully-drained file under a non-lossy error policy.
     * Caller is responsible for the FAIL_FAST gate; this method writes unconditionally.
     */
    public static void put(StorageObject object, long rowCount) {
        try {
            CACHE.put(new Key(object.path().toString(), object.length()), rowCount);
        } catch (Exception e) {
            // Cache write failures degrade silently — next query repopulates.
        }
    }

    /** Test isolation. */
    public static void clearForTests() {
        CACHE.invalidateAll();
    }
}
