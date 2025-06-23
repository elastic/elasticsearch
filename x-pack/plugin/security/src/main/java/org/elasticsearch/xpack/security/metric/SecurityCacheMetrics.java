/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.List;

public final class SecurityCacheMetrics {

    public static <K, V> List<AutoCloseable> registerAsyncCacheMetrics(MeterRegistry registry, Cache<K, V> cache, CacheType type) {
        final List<AutoCloseable> metrics = new ArrayList<>();
        metrics.add(
            registry.registerLongAsyncCounter(
                type.metricsPrefix + ".hit.total",
                "Total number of cache hits.",
                "count",
                () -> new LongWithAttributes(cache.stats().getHits())
            )
        );
        metrics.add(
            registry.registerLongAsyncCounter(
                type.metricsPrefix + ".miss.total",
                "Total number of cache misses.",
                "count",
                () -> new LongWithAttributes(cache.stats().getMisses())
            )
        );
        metrics.add(
            registry.registerLongAsyncCounter(
                type.metricsPrefix + ".eviction.total",
                "Total number of cache evictions.",
                "count",
                () -> new LongWithAttributes(cache.stats().getEvictions())
            )
        );
        metrics.add(
            registry.registerLongGauge(
                type.metricsPrefix + ".count.current",
                "The current number of cache entries.",
                "count",
                () -> new LongWithAttributes(cache.count())
            )
        );
        return metrics;
    }

    public enum CacheType {

        API_KEY_AUTH_CACHE("es.security.api_key.auth_cache"),

        API_KEY_DOCS_CACHE("es.security.api_key.doc_cache"),

        API_KEY_ROLE_DESCRIPTORS_CACHE("es.security.api_key.role_descriptor_cache"),

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
