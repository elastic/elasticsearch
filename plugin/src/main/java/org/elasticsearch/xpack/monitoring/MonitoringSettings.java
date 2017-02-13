/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.common.settings.Setting.Property;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.groupSetting;
import static org.elasticsearch.common.settings.Setting.listSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

public class MonitoringSettings extends AbstractComponent {

    public static final String LEGACY_DATA_INDEX_NAME = ".marvel-es-data";

    public static final String HISTORY_DURATION_SETTING_NAME = "history.duration";
    /**
     * The minimum amount of time allowed for the history duration.
     */
    public static final TimeValue HISTORY_DURATION_MINIMUM = TimeValue.timeValueHours(24);

    /**
     * Minimum value for sampling interval (1 second)
     */
    static final TimeValue MIN_INTERVAL = TimeValue.timeValueSeconds(1L);

    /**
     * Sampling interval between two collections (default to 10s)
     */
    public static final Setting<TimeValue> INTERVAL = new Setting<>(collectionKey("interval"), "10s",
            (s) -> {
                TimeValue value = TimeValue.parseTimeValue(s, null, collectionKey("interval"));
                if (TimeValue.MINUS_ONE.equals(value) || value.millis() >= MIN_INTERVAL.millis()) {
                    return value;
                }
                throw new IllegalArgumentException("Failed to parse monitoring interval [" + s + "], value must be >= " + MIN_INTERVAL);
            },
            Property.Dynamic, Property.NodeScope);

    /**
     * Timeout value when collecting index statistics (default to 10m)
     */
    public static final Setting<TimeValue> INDEX_STATS_TIMEOUT =
            timeSetting(collectionKey("index.stats.timeout"), TimeValue.timeValueSeconds(10), Property.Dynamic, Property.NodeScope);

    /**
     * Timeout value when collecting total indices statistics (default to 10m)
     */
    public static final Setting<TimeValue> INDICES_STATS_TIMEOUT =
            timeSetting(collectionKey("indices.stats.timeout"), TimeValue.timeValueSeconds(10), Property.Dynamic, Property.NodeScope);

    /**
     * List of indices names whose stats will be exported (default to all indices)
     */
    public static final Setting<List<String>> INDICES =
            listSetting(collectionKey("indices"), Collections.emptyList(), Function.identity(), Property.Dynamic, Property.NodeScope);

    /**
     * Timeout value when collecting the cluster state (default to 10m)
     */
    public static final Setting<TimeValue> CLUSTER_STATE_TIMEOUT =
            timeSetting(collectionKey("cluster.state.timeout"), TimeValue.timeValueSeconds(10), Property.Dynamic, Property.NodeScope);

    /**
     * Timeout value when collecting the recovery information (default to 10m)
     */
    public static final Setting<TimeValue> CLUSTER_STATS_TIMEOUT =
            timeSetting(collectionKey("cluster.stats.timeout"), TimeValue.timeValueSeconds(10), Property.Dynamic, Property.NodeScope);

    /**
     * Timeout value when collecting the recovery information (default to 10m)
     */
    public static final Setting<TimeValue> INDEX_RECOVERY_TIMEOUT =
            timeSetting(collectionKey("index.recovery.timeout"), TimeValue.timeValueSeconds(10), Property.Dynamic, Property.NodeScope);

    /**
     * Flag to indicate if only active recoveries should be collected (default to false: all recoveries are collected)
     */
    public static final Setting<Boolean> INDEX_RECOVERY_ACTIVE_ONLY =
            boolSetting(collectionKey("index.recovery.active_only"), false, Property.Dynamic, Property.NodeScope) ;

    /**
     * The default retention duration of the monitoring history data.
     * <p>
     * Expected values:
     * <ul>
     * <li>Default: 7 days</li>
     * <li>Minimum: 1 day</li>
     * </ul>
     *
     * @see #HISTORY_DURATION_MINIMUM
     */
    public static final Setting<TimeValue> HISTORY_DURATION =
            timeSetting(key(HISTORY_DURATION_SETTING_NAME),
                        TimeValue.timeValueHours(7 * 24), // default value (7 days)
                        HISTORY_DURATION_MINIMUM,         // minimum value
                        Property.Dynamic, Property.NodeScope);

    /**
     * Settings/Options per configured exporter
     */
    public static final Setting<Settings> EXPORTERS_SETTINGS =
            groupSetting(key("exporters."), Property.Dynamic, Property.NodeScope);

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(INDICES,
                INTERVAL,
                INDEX_RECOVERY_TIMEOUT,
                INDEX_STATS_TIMEOUT,
                INDICES_STATS_TIMEOUT,
                INDEX_RECOVERY_ACTIVE_ONLY,
                CLUSTER_STATE_TIMEOUT,
                CLUSTER_STATS_TIMEOUT,
                HISTORY_DURATION,
                EXPORTERS_SETTINGS);
    }

    public static List<String> getSettingsFilter() {
        return Arrays.asList(key("exporters.*.auth.*"), key("exporters.*.ssl.*"));
    }


    private volatile TimeValue indexStatsTimeout;
    private volatile TimeValue indicesStatsTimeout;
    private volatile TimeValue clusterStateTimeout;
    private volatile TimeValue clusterStatsTimeout;
    private volatile TimeValue recoveryTimeout;
    private volatile boolean recoveryActiveOnly;
    private volatile String[] indices;

    public MonitoringSettings(Settings settings, ClusterSettings clusterSettings) {
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

    /**
     * Prefix the {@code key} with the Monitoring prefix and "collection." .
     *
     * @param key The key to prefix
     * @return The key prefixed by the product prefixes + "collection." .
     * @see #key(String)
     */
    static String collectionKey(String key) {
        return key("collection." + key);
    }

    /**
     * Prefix the {@code key} with the Monitoring prefix.
     *
     * @param key The key to prefix
     * @return The key prefixed by the product prefixes.
     */
    static String key(String key) {
        return XPackPlugin.featureSettingPrefix(Monitoring.NAME) + "." + key;
    }

}
