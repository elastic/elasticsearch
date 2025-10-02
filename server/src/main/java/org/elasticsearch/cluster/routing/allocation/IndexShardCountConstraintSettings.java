/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlag;

/**
 * Settings definitions for the index shard count allocation decider and associated infrastructure
 */
public class IndexShardCountConstraintSettings {

    private static final String SETTING_PREFIX = "cluster.routing.allocation.index_shard_count_decider.";
    private static final FeatureFlag INDEX_SHARD_COUNT_DECIDER_FEATURE_FLAG = new FeatureFlag("index_shard_count_decider");

    public enum IndexShardCountDeciderStatus {
        DISABLED,
        ENABLED;

        public boolean enabled() {
            return this == ENABLED;
        }

        public boolean disabled() {
            return this == DISABLED;
        }
    }

    public static final Setting<IndexShardCountDeciderStatus> INDEX_SHARD_COUNT_DECIDER_ENABLED_SETTING = Setting.enumSetting(
        IndexShardCountDeciderStatus.class,
        SETTING_PREFIX + "enabled",
        INDEX_SHARD_COUNT_DECIDER_FEATURE_FLAG.isEnabled() ? IndexShardCountDeciderStatus.ENABLED : IndexShardCountDeciderStatus.DISABLED,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting permits nodes to host more than ideally balanced number of index shards.
     * Maximum tolerated index shard count = ceil(ideal * skew_tolerance)
     * i.e. ideal = 4 shards, skew_tolerance = 1.3
     * maximum tolerated index shards = Math.ceil(4 * 1.3) = 6.
     */
    public static final Setting<Double> INDEX_SHARD_COUNT_DECIDER_LOAD_SKEW_TOLERANCE = Setting.doubleSetting(
        SETTING_PREFIX + "load_skew_tolerance",
        1.5d,
        1.0d,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile IndexShardCountDeciderStatus indexShardCountDeciderStatus;
    private volatile double loadSkewTolerance;

    public IndexShardCountConstraintSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(INDEX_SHARD_COUNT_DECIDER_ENABLED_SETTING,
            status -> this.indexShardCountDeciderStatus = status);
        clusterSettings.initializeAndWatch(INDEX_SHARD_COUNT_DECIDER_LOAD_SKEW_TOLERANCE,
            value -> this.loadSkewTolerance = value);
    }

    public IndexShardCountDeciderStatus getIndexShardCountDeciderStatus() {
        return this.indexShardCountDeciderStatus;
    }

    public double getLoadSkewTolerance() {
        return this.loadSkewTolerance;
    }

}
