/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class SecurityCacheMetrics {

    public static <K, V> List<AutoCloseable> registerAsyncCacheMetrics(MeterRegistry registry, Cache<K, V> cache, CacheType type) {
        return registerAsyncCacheMetrics(registry, type, cache::stats, cache::count);
    }

    /**
     * Registers hit, miss, eviction and entry count metrics for a cache read through suppliers, so the cache instance can be replaced
     * after registration.
     */
    public static List<AutoCloseable> registerAsyncCacheMetrics(
        MeterRegistry registry,
        CacheType type,
        Supplier<Cache.Stats> stats,
        IntSupplier count
    ) {
        final List<AutoCloseable> metrics = new ArrayList<>();
        metrics.add(
            registry.registerLongAsyncCounter(
                type.metricsPrefix + ".hit.total",
                "Total number of cache hits.",
                "count",
                () -> stats.get().getHits()
            )
        );
        metrics.add(
            registry.registerLongAsyncCounter(
                type.metricsPrefix + ".miss.total",
                "Total number of cache misses.",
                "count",
                () -> stats.get().getMisses()
            )
        );
        metrics.add(
            registry.registerLongAsyncCounter(
                type.metricsPrefix + ".eviction.total",
                "Total number of cache evictions.",
                "count",
                () -> stats.get().getEvictions()
            )
        );
        metrics.add(
            registry.registerLongAsyncGauge(
                type.metricsPrefix + ".count.current",
                "The current number of cache entries.",
                "count",
                () -> count.getAsInt()
            )
        );
        return metrics;
    }

    /** Registers the estimated bytes a weight-bounded cache retains and the limit it was built with. */
    public static List<AutoCloseable> registerCacheMemoryMetrics(
        MeterRegistry registry,
        CacheType type,
        LongSupplier weightBytes,
        LongSupplier maximumWeightBytes
    ) {
        return List.of(
            registry.registerLongAsyncGauge(
                type.metricsPrefix + ".memory.size",
                "Estimated heap retained by the cache entries.",
                "bytes",
                weightBytes
            ),
            registry.registerLongAsyncGauge(
                type.metricsPrefix + ".memory_limit.size",
                "Configured maximum heap for the cache entries.",
                "bytes",
                maximumWeightBytes
            )
        );
    }

    public enum CacheType {

        API_KEY_AUTH_CACHE("es.security.api_key.auth_cache"),

        API_KEY_DOCS_CACHE("es.security.api_key.doc_cache"),

        API_KEY_ROLE_DESCRIPTORS_CACHE("es.security.api_key.role_descriptor_cache"),

        INDEX_PRIVILEGES_CACHE("es.security.index_privileges.cache"),

        ;

        private final String metricsPrefix;

        CacheType(String metricsPrefix) {
            this.metricsPrefix = metricsPrefix;
        }

        public String metricsPrefix() {
            return metricsPrefix;
        }
    }

    private SecurityCacheMetrics() {
        throw new IllegalAccessError();
    }
}
