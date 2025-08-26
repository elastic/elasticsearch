/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.TimeValue;

/**
 * Settings definitions for the write load allocation decider and associated infrastructure
 */
public class WriteLoadConstraintSettings {

    private static final String SETTING_PREFIX = "cluster.routing.allocation.write_load_decider.";

    public enum WriteLoadDeciderStatus {
        /**
         * The decider is disabled
         */
        DISABLED,
        /**
         * Only the low write low threshold, to try to avoid allocating to a node exceeding
         * {@link #WRITE_LOAD_DECIDER_HIGH_UTILIZATION_THRESHOLD_SETTING}. Write-load hot-spot will not trigger rebalancing.
         */
        LOW_THRESHOLD_ONLY,
        /**
         * All write load decider development work is turned on.
         */
        ENABLED;

        public boolean fullyEnabled() {
            return this == ENABLED;
        }

        public boolean notFullyEnabled() {
            return this != ENABLED;
        }

        public boolean atLeastLowThresholdEnabled() {
            return this != DISABLED;
        }

        public boolean disabled() {
            return this == DISABLED;
        }
    }

    public static final Setting<WriteLoadDeciderStatus> WRITE_LOAD_DECIDER_ENABLED_SETTING = Setting.enumSetting(
        WriteLoadDeciderStatus.class,
        SETTING_PREFIX + "enabled",
        WriteLoadDeciderStatus.DISABLED,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The threshold over which we consider write thread pool utilization "high"
     */
    public static final Setting<RatioValue> WRITE_LOAD_DECIDER_HIGH_UTILIZATION_THRESHOLD_SETTING = new Setting<>(
        SETTING_PREFIX + "high_utilization_threshold",
        "90%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The duration for which we need to see "high" utilization before we consider the low threshold exceeded
     */
    public static final Setting<TimeValue> WRITE_LOAD_DECIDER_HIGH_UTILIZATION_DURATION_SETTING = Setting.timeSetting(
        SETTING_PREFIX + "high_utilization_duration",
        TimeValue.timeValueMinutes(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * When the decider is {@link WriteLoadDeciderStatus#ENABLED}, the write-load monitor will call
     * {@link RerouteService#reroute(String, Priority, ActionListener)} when we see tasks being delayed by this amount of time
     * (but no more often than {@link #WRITE_LOAD_DECIDER_REROUTE_INTERVAL_SETTING})
     */
    public static final Setting<TimeValue> WRITE_LOAD_DECIDER_QUEUE_LATENCY_THRESHOLD_SETTING = Setting.timeSetting(
        SETTING_PREFIX + "queue_latency_threshold",
        TimeValue.timeValueSeconds(30),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * How often the data node calculates the write-loads for the individual shards
     */
    public static final Setting<TimeValue> WRITE_LOAD_DECIDER_SHARD_WRITE_LOAD_POLLING_INTERVAL_SETTING = Setting.timeSetting(
        SETTING_PREFIX + "shard_write_load_polling_interval",
        TimeValue.timeValueSeconds(60),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The minimum amount of time between successive calls to reroute to address write load hot-spots
     */
    public static final Setting<TimeValue> WRITE_LOAD_DECIDER_REROUTE_INTERVAL_SETTING = Setting.timeSetting(
        SETTING_PREFIX + "reroute_interval",
        TimeValue.timeValueSeconds(60),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    WriteLoadDeciderStatus writeLoadDeciderStatus;
    TimeValue writeLoadDeciderRerouteIntervalSetting;
    double writeThreadPoolHighUtilizationThresholdSetting;

    public WriteLoadConstraintSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(WRITE_LOAD_DECIDER_ENABLED_SETTING, this::setWriteLoadConstraintEnabled);
        clusterSettings.initializeAndWatch(WRITE_LOAD_DECIDER_REROUTE_INTERVAL_SETTING, this::setWriteLoadDeciderRerouteIntervalSetting);
        clusterSettings.initializeAndWatch(
            WRITE_LOAD_DECIDER_HIGH_UTILIZATION_THRESHOLD_SETTING,
            this::setWriteThreadPoolHighUtilizationThresholdSetting
        );

    };

    private void setWriteLoadConstraintEnabled(WriteLoadDeciderStatus status) {
        this.writeLoadDeciderStatus = status;
    }

    public WriteLoadDeciderStatus getWriteLoadConstraintEnabled() {
        return this.writeLoadDeciderStatus;
    }

    public TimeValue getWriteLoadDeciderRerouteIntervalSetting() {
        return this.writeLoadDeciderRerouteIntervalSetting;
    }

    public double getWriteThreadPoolHighUtilizationThresholdSetting() {
        return this.writeThreadPoolHighUtilizationThresholdSetting;
    }

    private void setWriteLoadDeciderRerouteIntervalSetting(TimeValue timeValue) {
        this.writeLoadDeciderRerouteIntervalSetting = timeValue;
    }

    private void setWriteThreadPoolHighUtilizationThresholdSetting(RatioValue percent) {
        this.writeThreadPoolHighUtilizationThresholdSetting = percent.getAsRatio();
    }
}
