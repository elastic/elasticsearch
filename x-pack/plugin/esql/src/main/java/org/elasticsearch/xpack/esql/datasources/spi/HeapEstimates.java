/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;

/**
 * Heap weights for the caches that hold datasource values. Lives in {@code spi} so both the cache and the
 * {@link FileList} implementations can charge the same estimate without the SPI depending on cache internals.
 */
public final class HeapEstimates {

    private HeapEstimates() {}

    /**
     * About 40 bytes for the {@code String} object and its backing array headers on a 64-bit JVM with compressed
     * references, plus two bytes per character. Both parts round up on purpose (compact Latin-1 strings use one byte
     * per character); this feeds a cache budget, where over-counting evicts a little early and under-counting lets the
     * cache outgrow its budget.
     */
    public static long stringBytes(@Nullable String s) {
        return 40 + (s != null ? s.length() * (long) Character.BYTES : 0);
    }
}
