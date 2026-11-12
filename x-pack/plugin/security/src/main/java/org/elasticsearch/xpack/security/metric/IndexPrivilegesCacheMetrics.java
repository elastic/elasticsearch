/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * Publishes the state of the cache of resolved index privileges, see {@link IndexPrivilege#CACHE_SIZE_SETTING}, and releases the
 * instruments when the security plugin closes.
 */
public final class IndexPrivilegesCacheMetrics implements Closeable {

    private static final Logger logger = LogManager.getLogger(IndexPrivilegesCacheMetrics.class);

    private final List<AutoCloseable> metrics;

    public IndexPrivilegesCacheMetrics(MeterRegistry meterRegistry) {
        final List<AutoCloseable> metrics = new ArrayList<>();
        metrics.addAll(
            SecurityCacheMetrics.registerAsyncCacheMetrics(
                meterRegistry,
                SecurityCacheMetrics.CacheType.INDEX_PRIVILEGES_CACHE,
                IndexPrivilege::cacheStats,
                IndexPrivilege::cacheEntryCount
            )
        );
        metrics.addAll(
            SecurityCacheMetrics.registerCacheMemoryMetrics(
                meterRegistry,
                SecurityCacheMetrics.CacheType.INDEX_PRIVILEGES_CACHE,
                IndexPrivilege::cacheWeightBytes,
                IndexPrivilege::cacheMaximumWeightBytes
            )
        );
        this.metrics = List.copyOf(metrics);
    }

    @Override
    public void close() {
        for (AutoCloseable metric : metrics) {
            try {
                metric.close();
            } catch (Exception e) {
                logger.warn("failed to close index privileges cache metric", e);
            }
        }
    }
}
