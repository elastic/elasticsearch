/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.settings;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.Marvel;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class MarvelSettings extends AbstractComponent {

    private static final String PREFIX = Marvel.NAME + ".agent.";

    public static final String MARVEL_INDICES_PREFIX = ".marvel-es-";
    public static final String MARVEL_DATA_INDEX_PREFIX = MARVEL_INDICES_PREFIX + "data-";
    public static final TimeValue MAX_LICENSE_GRACE_PERIOD = TimeValue.timeValueHours(7 * 24);

    /** Sampling interval between two collections (default to 10s) */
    public static final Setting<TimeValue> INTERVAL_SETTING =
            Setting.timeSetting(PREFIX + "interval", TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);
    /** Timeout value when collecting index statistics (default to 10m) */
    public static final Setting<TimeValue> INDEX_STATS_TIMEOUT_SETTING =
            Setting.timeSetting(PREFIX + "index.stats.timeout", TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);
    /** Timeout value when collecting total indices statistics (default to 10m) */
    public static final Setting<TimeValue> INDICES_STATS_TIMEOUT_SETTING =
            Setting.timeSetting(PREFIX + "indices.stats.timeout", TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);
    /** List of indices names whose stats will be exported (default to all indices) */
    public static final Setting<List<String>> INDICES_SETTING =
            Setting.listSetting(PREFIX + "indices", Collections.emptyList(), Function.identity(), true, Setting.Scope.CLUSTER);
    /** Timeout value when collecting the cluster state (default to 10m) */
    public static final Setting<TimeValue> CLUSTER_STATE_TIMEOUT_SETTING =
            Setting.timeSetting(PREFIX + "cluster.state.timeout", TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);
    /** Timeout value when collecting the recovery information (default to 10m) */
    public static final Setting<TimeValue> CLUSTER_STATS_TIMEOUT_SETTING =
            Setting.timeSetting(PREFIX + "cluster.stats.timeout", TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);
    /** Timeout value when collecting the recovery information (default to 10m) */
    public static final Setting<TimeValue> INDEX_RECOVERY_TIMEOUT_SETTING =
            Setting.timeSetting(PREFIX + "index.recovery.timeout", TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);
    /** Flag to indicate if only active recoveries should be collected (default to false: all recoveries are collected) */
    public static final Setting<Boolean> INDEX_RECOVERY_ACTIVE_ONLY_SETTING =
            Setting.boolSetting(PREFIX + "index.recovery.active_only", false, true, Setting.Scope.CLUSTER) ;
    /** List of collectors allowed to collect data (default to all)*/
    public static final Setting<List<String>> COLLECTORS_SETTING =
            Setting.listSetting(PREFIX + "collectors", Collections.emptyList(), Function.identity(), false, Setting.Scope.CLUSTER);

    private TimeValue indexStatsTimeout;
    private TimeValue indicesStatsTimeout;
    private TimeValue clusterStateTimeout;
    private TimeValue clusterStatsTimeout;
    private TimeValue recoveryTimeout;
    private boolean recoveryActiveOnly;
    private String[] indices;

    @Inject
    public MarvelSettings(Settings settings, ClusterSettings clusterSettings) {
        super(settings);

        logger.trace("initializing marvel settings");
        setIndexStatsTimeout(INDEX_STATS_TIMEOUT_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(INDEX_STATS_TIMEOUT_SETTING, this::setIndexStatsTimeout);
        setIndicesStatsTimeout(INDICES_STATS_TIMEOUT_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(INDICES_STATS_TIMEOUT_SETTING, this::setIndicesStatsTimeout);
        setIndices(INDICES_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(INDICES_SETTING, this::setIndices);
        setClusterStateTimeout(CLUSTER_STATE_TIMEOUT_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_STATE_TIMEOUT_SETTING, this::setClusterStateTimeout);
        setClusterStatsTimeout(CLUSTER_STATS_TIMEOUT_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_STATS_TIMEOUT_SETTING, this::setClusterStatsTimeout);
        setRecoveryTimeout(INDEX_RECOVERY_TIMEOUT_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(INDEX_RECOVERY_TIMEOUT_SETTING, this::setRecoveryTimeout);
        setRecoveryActiveOnly(INDEX_RECOVERY_ACTIVE_ONLY_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(INDEX_RECOVERY_ACTIVE_ONLY_SETTING, this::setRecoveryActiveOnly);
    }

    public TimeValue indexStatsTimeout() {
        return indexStatsTimeout;
    }

    public TimeValue indicesStatsTimeout() { return indicesStatsTimeout; }

    public String[] indices() {
        return indices;
    }

    public TimeValue clusterStateTimeout() {
        return clusterStateTimeout;
    }

    public TimeValue clusterStatsTimeout() {
        return clusterStatsTimeout;
    }

    public TimeValue recoveryTimeout() {
        return recoveryTimeout;
    }

    public boolean recoveryActiveOnly() { return recoveryActiveOnly; }

    private void setIndexStatsTimeout(TimeValue indexStatsTimeout) {
        this.indexStatsTimeout = indexStatsTimeout;
    }

    private void setIndicesStatsTimeout(TimeValue indicesStatsTimeout) {
        this.indicesStatsTimeout = indicesStatsTimeout;
    }

    private void setClusterStateTimeout(TimeValue clusterStateTimeout) {
        this.clusterStateTimeout = clusterStateTimeout;
    }

    private void setClusterStatsTimeout(TimeValue clusterStatsTimeout) {
        this.clusterStatsTimeout = clusterStatsTimeout;
    }

    private void setRecoveryTimeout(TimeValue recoveryTimeout) {
        this.recoveryTimeout = recoveryTimeout;
    }

    private void setRecoveryActiveOnly(boolean recoveryActiveOnly) {
        this.recoveryActiveOnly = recoveryActiveOnly;
    }

    private void setIndices(List<String> indices) {
        this.indices = indices.toArray(new String[0]);
    }

}
