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
public class IndexBalanceConstraintSettings {

    private static final String SETTING_PREFIX = "cluster.routing.allocation.index_balance_decider.";
    private static final FeatureFlag INDEX_BALANCE_DECIDER_FEATURE_FLAG = new FeatureFlag("index_balance_decider");

    public static final Setting<Boolean> INDEX_BALANCE_DECIDER_ENABLED_SETTING = Setting.boolSetting(
        SETTING_PREFIX + "enabled",
        INDEX_BALANCE_DECIDER_FEATURE_FLAG.isEnabled(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting permits nodes to host more than ideally balanced number of index shards.
     * Maximum tolerated index shard count = ceil(ideal * skew_tolerance)
     * i.e. ideal = 4 shards, skew_tolerance = 1.3
     * maximum tolerated index shards = Math.ceil(4 * 1.3) = 6.
     */
    public static final Setting<Double> INDEX_BALANCE_DECIDER_LOAD_SKEW_TOLERANCE = Setting.doubleSetting(
        SETTING_PREFIX + "load_skew_tolerance",
        1.5d,
        1.0d,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean deciderEnabled;
    private volatile double loadSkewTolerance;

    public IndexBalanceConstraintSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(INDEX_BALANCE_DECIDER_ENABLED_SETTING, enabled -> this.deciderEnabled = enabled);
        clusterSettings.initializeAndWatch(INDEX_BALANCE_DECIDER_LOAD_SKEW_TOLERANCE, value -> this.loadSkewTolerance = value);
    }

    public boolean isDeciderEnabled() {
        return this.deciderEnabled;
    }

    public double getLoadSkewTolerance() {
        return this.loadSkewTolerance;
    }

}
