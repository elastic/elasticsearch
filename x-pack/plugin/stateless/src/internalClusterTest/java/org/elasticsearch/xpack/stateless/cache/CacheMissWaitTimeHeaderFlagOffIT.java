/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.index.store.Store;

/**
 * Same scenarios as {@link CacheMissWaitTimeHeaderIT}, but requires the {@code directory_metrics}
 * feature flag to be <strong>disabled</strong> so we verify {@code cache_miss_wait_nanos} is
 * surfaced independently of that flag (and {@code store_bytes_read} is not).
 * <p>
 * Run via the {@code internalClusterTestDirectoryMetricsDisabled} Gradle task, which sets
 * {@code -Des.directory_metrics_feature_flag_enabled=false}.
 */
public class CacheMissWaitTimeHeaderFlagOffIT extends CacheMissWaitTimeHeaderIT {

    @Override
    public void testCacheMissWaitTimeHeader() throws InterruptedException {
        assumeFalse(
            "directory metrics flag must be disabled (use internalClusterTestDirectoryMetricsDisabled)",
            Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled()
        );
        assertCacheMissWaitTimeHeader("cache-miss-header-flag-off", false);
    }
}
