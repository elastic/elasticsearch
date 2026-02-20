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

/**
 * Settings definitions for the index shard count allocation decider and associated infrastructure
 */
public class IndexBalanceConstraintSettings {

    private static final String SETTING_PREFIX = "cluster.routing.allocation.index_balance_decider.";

    public static final Setting<Boolean> INDEX_BALANCE_DECIDER_ENABLED_SETTING = Setting.boolSetting(
        SETTING_PREFIX + "enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting permits nodes to host more than ideally balanced number of index shards.
     * Maximum tolerated index shard count = ideal + skew_tolerance
     * i.e. ideal = 4 shards, skew_tolerance = 1
     * maximum tolerated index shards = 4 + 1 = 5.
     */
    public static final Setting<Integer> INDEX_BALANCE_DECIDER_EXCESS_SHARDS = Setting.intSetting(
        SETTING_PREFIX + "excess_shards",
        0,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean deciderEnabled;
    private volatile int excessShards;

    public IndexBalanceConstraintSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(INDEX_BALANCE_DECIDER_ENABLED_SETTING, enabled -> this.deciderEnabled = enabled);
        clusterSettings.initializeAndWatch(INDEX_BALANCE_DECIDER_EXCESS_SHARDS, value -> this.excessShards = value);
    }

    public boolean isDeciderEnabled() {
        return this.deciderEnabled;
    }

    public int getExcessShards() {
        return this.excessShards;
    }

}
