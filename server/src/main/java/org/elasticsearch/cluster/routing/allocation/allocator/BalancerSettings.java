/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.THRESHOLD_SETTING;
import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING;

public class BalancerSettings {
    public static final BalancerSettings DEFAULT = new BalancerSettings(ClusterSettings.createBuiltInClusterSettings());

    private volatile float indexBalanceFactor;
    private volatile float shardBalanceFactor;
    private volatile float writeLoadBalanceFactor;
    private volatile float diskUsageBalanceFactor;
    private volatile float threshold;

    public BalancerSettings(Settings settings) {
        this(ClusterSettings.createBuiltInClusterSettings(settings));
    }

    public BalancerSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(SHARD_BALANCE_FACTOR_SETTING, value -> this.shardBalanceFactor = value);
        clusterSettings.initializeAndWatch(INDEX_BALANCE_FACTOR_SETTING, value -> this.indexBalanceFactor = value);
        clusterSettings.initializeAndWatch(WRITE_LOAD_BALANCE_FACTOR_SETTING, value -> this.writeLoadBalanceFactor = value);
        clusterSettings.initializeAndWatch(DISK_USAGE_BALANCE_FACTOR_SETTING, value -> this.diskUsageBalanceFactor = value);
        clusterSettings.initializeAndWatch(THRESHOLD_SETTING, value -> this.threshold = value);
    }

    /**
     * Returns the index related weight factor.
     */
    public float getIndexBalanceFactor() {
        return indexBalanceFactor;
    }

    /**
     * Returns the shard related weight factor.
     */
    public float getShardBalanceFactor() {
        return shardBalanceFactor;
    }

    public float getWriteLoadBalanceFactor() {
        return writeLoadBalanceFactor;
    }

    public float getDiskUsageBalanceFactor() {
        return diskUsageBalanceFactor;
    }

    /**
     * Returns the currently configured delta threshold
     */
    public float getThreshold() {
        return threshold;
    }
}
