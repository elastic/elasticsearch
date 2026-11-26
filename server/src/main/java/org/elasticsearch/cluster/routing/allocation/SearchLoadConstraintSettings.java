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
 * Settings definitions for the search load allocation decider and associated infrastructure
 */
public class SearchLoadConstraintSettings {

    private static final String SETTING_PREFIX = "cluster.routing.allocation.search_load.";

    private static final String CPU_THRESHOLD_KEY = SETTING_PREFIX + "cpu_threshold";

    /**
     * The CPU threshold for search load (T_cpu): the share of a node's requested CPU that the search load of the shard copies on it
     * may use. The search load decider and the search tier autoscaler both read it, so they plan against the same value.
     * It must be above 0 and at most 1.
     */
    public static final Setting<Double> SEARCH_LOAD_CPU_THRESHOLD_SETTING = new Setting<>(
        CPU_THRESHOLD_KEY,
        "0.9",
        SearchLoadConstraintSettings::parseCpuThreshold,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    private static double parseCpuThreshold(String value) {
        final double threshold = Double.parseDouble(value);
        if (Double.isNaN(threshold) || threshold <= 0.0 || threshold > 1.0) {
            throw new IllegalArgumentException(
                "Failed to parse value [" + value + "] for setting [" + CPU_THRESHOLD_KEY + "] must be > 0 and <= 1"
            );
        }
        return threshold;
    }

    private volatile double cpuThreshold;

    public SearchLoadConstraintSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(SEARCH_LOAD_CPU_THRESHOLD_SETTING, value -> this.cpuThreshold = value);
    }

    /**
     * @return the CPU threshold (T_cpu), in (0, 1]
     */
    public double getCpuThreshold() {
        return cpuThreshold;
    }
}
