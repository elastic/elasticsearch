/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class MarvelSettings extends AbstractComponent {

    public static final String MONITORING_INDICES_PREFIX = ".monitoring-es-";
    public static final String MONITORING_DATA_INDEX_PREFIX = ".monitoring-es-data-";
    public static final String LEGACY_DATA_INDEX_NAME = ".marvel-es-data";

    public static final String HISTORY_DURATION_SETTING_NAME = "history.duration";
    public static final TimeValue MAX_LICENSE_GRACE_PERIOD = TimeValue.timeValueHours(7 * 24);

    /**
     * Determines whether monitoring is enabled/disabled
     */
    public static final Setting<Boolean> ENABLED =
            new Setting<>(XPackPlugin.featureEnabledSetting(Marvel.NAME),

                    // By default, marvel is disabled on tribe nodes
                    (s) -> String.valueOf(!XPackPlugin.isTribeNode(s) && !XPackPlugin.isTribeClientNode(s)),

                    Booleans::parseBooleanExact,
                    false,
                    Setting.Scope.CLUSTER);

    /**
     * Sampling interval between two collections (default to 10s)
     */
    public static final Setting<TimeValue> INTERVAL =
            Setting.timeSetting(key("agent.interval"), TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);

    /**
     * Timeout value when collecting index statistics (default to 10m)
     */
    public static final Setting<TimeValue> INDEX_STATS_TIMEOUT =
            Setting.timeSetting(key("agent.index.stats.timeout"), TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);

    /**
     * Timeout value when collecting total indices statistics (default to 10m)
     */
    public static final Setting<TimeValue> INDICES_STATS_TIMEOUT =
            Setting.timeSetting(key("agent.indices.stats.timeout"), TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);

    /**
     * List of indices names whose stats will be exported (default to all indices)
     */
    public static final Setting<List<String>> INDICES =
            Setting.listSetting(key("agent.indices"), Collections.emptyList(), Function.identity(), true, Setting.Scope.CLUSTER);

    /**
     * Timeout value when collecting the cluster state (default to 10m)
     */
    public static final Setting<TimeValue> CLUSTER_STATE_TIMEOUT =
            Setting.timeSetting(key("agent.cluster.state.timeout"), TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);

    /**
     * Timeout value when collecting the recovery information (default to 10m)
     */
    public static final Setting<TimeValue> CLUSTER_STATS_TIMEOUT =
            Setting.timeSetting(key("agent.cluster.stats.timeout"), TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);

    /**
     * Timeout value when collecting the recovery information (default to 10m)
     */
    public static final Setting<TimeValue> INDEX_RECOVERY_TIMEOUT =
            Setting.timeSetting(key("agent.index.recovery.timeout"), TimeValue.timeValueSeconds(10), true, Setting.Scope.CLUSTER);

    /**
     * Flag to indicate if only active recoveries should be collected (default to false: all recoveries are collected)
     */
    public static final Setting<Boolean> INDEX_RECOVERY_ACTIVE_ONLY =
            Setting.boolSetting(key("agent.index.recovery.active_only"), false, true, Setting.Scope.CLUSTER) ;

    /**
     * List of collectors allowed to collect data (default to all)
     */
    public static final Setting<List<String>> COLLECTORS =
            Setting.listSetting(key("agent.collectors"), Collections.emptyList(), Function.identity(), false, Setting.Scope.CLUSTER);

    /**
     * The default retention duration of the monitoring history data
     */
    public static final Setting<TimeValue> HISTORY_DURATION =
            Setting.timeSetting(key(HISTORY_DURATION_SETTING_NAME), TimeValue.timeValueHours(7 * 24), true, Setting.Scope.CLUSTER);

    /**
     * The index setting that holds the template version
     */
    public static final Setting<Integer> INDEX_TEMPLATE_VERSION =
            Setting.intSetting("index.xpack.monitoring.template.version", MarvelTemplateUtils.TEMPLATE_VERSION, true, Setting.Scope.INDEX);

    /**
     * Settings/Options per configured exporter
     */
    public static final Setting<Settings> EXPORTERS_SETTINGS =
            Setting.groupSetting(key("agent.exporters."), true, Setting.Scope.CLUSTER);

    static void register(SettingsModule module) {
        module.registerSetting(INDICES);
        module.registerSetting(INTERVAL);
        module.registerSetting(INDEX_RECOVERY_TIMEOUT);
        module.registerSetting(INDEX_STATS_TIMEOUT);
        module.registerSetting(INDICES_STATS_TIMEOUT);
        module.registerSetting(INDEX_RECOVERY_ACTIVE_ONLY);
        module.registerSetting(COLLECTORS);
        module.registerSetting(CLUSTER_STATE_TIMEOUT);
        module.registerSetting(CLUSTER_STATS_TIMEOUT);
        module.registerSetting(HISTORY_DURATION);
        module.registerSetting(EXPORTERS_SETTINGS);
        module.registerSetting(ENABLED);
        module.registerSetting(INDEX_TEMPLATE_VERSION);

        module.registerSettingsFilter("xpack.monitoring.agent.exporters.*.auth.password");
    }


    private volatile TimeValue indexStatsTimeout;
    private volatile TimeValue indicesStatsTimeout;
    private volatile TimeValue clusterStateTimeout;
    private volatile TimeValue clusterStatsTimeout;
    private volatile TimeValue recoveryTimeout;
    private volatile boolean recoveryActiveOnly;
    private volatile String[] indices;

    @Inject
    public MarvelSettings(Settings settings, ClusterSettings clusterSettings) {
        super(settings);

        setIndexStatsTimeout(INDEX_STATS_TIMEOUT.get(settings));
        clusterSettings.addSettingsUpdateConsumer(INDEX_STATS_TIMEOUT, this::setIndexStatsTimeout);
        setIndicesStatsTimeout(INDICES_STATS_TIMEOUT.get(settings));
        clusterSettings.addSettingsUpdateConsumer(INDICES_STATS_TIMEOUT, this::setIndicesStatsTimeout);
        setIndices(INDICES.get(settings));
        clusterSettings.addSettingsUpdateConsumer(INDICES, this::setIndices);
        setClusterStateTimeout(CLUSTER_STATE_TIMEOUT.get(settings));
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_STATE_TIMEOUT, this::setClusterStateTimeout);
        setClusterStatsTimeout(CLUSTER_STATS_TIMEOUT.get(settings));
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_STATS_TIMEOUT, this::setClusterStatsTimeout);
        setRecoveryTimeout(INDEX_RECOVERY_TIMEOUT.get(settings));
        clusterSettings.addSettingsUpdateConsumer(INDEX_RECOVERY_TIMEOUT, this::setRecoveryTimeout);
        setRecoveryActiveOnly(INDEX_RECOVERY_ACTIVE_ONLY.get(settings));
        clusterSettings.addSettingsUpdateConsumer(INDEX_RECOVERY_ACTIVE_ONLY, this::setRecoveryActiveOnly);
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

    public boolean recoveryActiveOnly() {
        return recoveryActiveOnly;
    }

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

    private static String key(String key) {
        return XPackPlugin.featureSettingPrefix(Marvel.NAME) + "." + key;
    }

}
