/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

public class BalancerSettings {

    public static final Setting<Float> SHARD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.shard",
        0.45f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Float> INDEX_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.index",
        0.55f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Float> WRITE_LOAD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.write_load",
        10.0f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Float> DISK_USAGE_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.disk_usage",
        2e-11f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Float> THRESHOLD_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.threshold",
        1.0f,
        1.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<TimeValue> INVALID_WEIGHTS_MINIMUM_LOG_INTERVAL = Setting.timeSetting(
        "cluster.routing.allocation.balance.invalid_weights_log_min_interval",
        TimeValue.timeValueMinutes(5),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final BalancerSettings DEFAULT = new BalancerSettings(ClusterSettings.createBuiltInClusterSettings());

    private volatile float indexBalanceFactor;
    private volatile float shardBalanceFactor;
    private volatile float writeLoadBalanceFactor;
    private volatile float diskUsageBalanceFactor;
    private volatile float threshold;
    private final boolean completeEarlyOnShardAssignmentChange;
    private final ClusterSettings clusterSettings;

    public BalancerSettings(Settings settings) {
        this(ClusterSettings.createBuiltInClusterSettings(settings));
    }

    public BalancerSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(SHARD_BALANCE_FACTOR_SETTING, value -> this.shardBalanceFactor = value);
        clusterSettings.initializeAndWatch(INDEX_BALANCE_FACTOR_SETTING, value -> this.indexBalanceFactor = value);
        clusterSettings.initializeAndWatch(WRITE_LOAD_BALANCE_FACTOR_SETTING, value -> this.writeLoadBalanceFactor = value);
        clusterSettings.initializeAndWatch(DISK_USAGE_BALANCE_FACTOR_SETTING, value -> this.diskUsageBalanceFactor = value);
        clusterSettings.initializeAndWatch(THRESHOLD_SETTING, value -> this.threshold = value);
        this.completeEarlyOnShardAssignmentChange = ClusterModule.DESIRED_BALANCE_ALLOCATOR.equals(
            clusterSettings.get(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING)
        );
        this.clusterSettings = clusterSettings;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
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

    public boolean completeEarlyOnShardAssignmentChange() {
        return completeEarlyOnShardAssignmentChange;
    }
}
