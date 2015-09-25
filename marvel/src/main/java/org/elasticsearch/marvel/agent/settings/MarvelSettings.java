/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.settings;

import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.marvel.agent.settings.MarvelSetting.*;

public class MarvelSettings extends AbstractComponent implements NodeSettingsService.Listener {

    private static final String PREFIX = MarvelPlugin.NAME + ".agent.";

    public static final String MARVEL_INDICES_PREFIX = ".marvel-es-";
    public static final String MARVEL_DATA_INDEX_NAME = MARVEL_INDICES_PREFIX + "data";
    public static final TimeValue MAX_LICENSE_GRACE_PERIOD = TimeValue.timeValueHours(7 * 24);

    public static final String INTERVAL                     = PREFIX + "interval";
    public static final String STARTUP_DELAY                = PREFIX + "startup.delay";
    public static final String INDEX_STATS_TIMEOUT          = PREFIX + "index.stats.timeout";
    public static final String INDICES_STATS_TIMEOUT        = PREFIX + "indices.stats.timeout";
    public static final String INDICES                      = PREFIX + "indices";
    public static final String CLUSTER_STATE_TIMEOUT        = PREFIX + "cluster.state.timeout";
    public static final String CLUSTER_STATS_TIMEOUT        = PREFIX + "cluster.stats.timeout";
    public static final String INDEX_RECOVERY_TIMEOUT       = PREFIX + "index.recovery.timeout";
    public static final String INDEX_RECOVERY_ACTIVE_ONLY   = PREFIX + "index.recovery.active_only";
    public static final String COLLECTORS                   = PREFIX + "collectors";
    public static final String LICENSE_GRACE_PERIOD         = PREFIX + "license.grace.period";

    private Map<String, ? extends MarvelSetting> settings = Collections.EMPTY_MAP;

    @Inject
    public MarvelSettings(Settings clusterSettings, NodeSettingsService nodeSettingsService) {
        super(clusterSettings);

        logger.trace("initializing marvel settings");
        this.settings = defaultSettings();

        logger.trace("updating marvel settings with cluster settings");
        updateSettings(clusterSettings);

        logger.trace("registering the service as a node settings listener");
        nodeSettingsService.addListener(this);
    }

    private Map<String, MarvelSetting> defaultSettings() {
        Map<String, MarvelSetting> map = new HashMap<>();
        map.put(INTERVAL, timeSetting(INTERVAL, TimeValue.timeValueSeconds(10),
                "Sampling interval between two collections (default to 10s)"));
        map.put(STARTUP_DELAY, timeSetting(STARTUP_DELAY, null,
                "Waiting time before the agent start to collect data (default to sampling interval)"));
        map.put(INDEX_STATS_TIMEOUT, timeoutSetting(INDEX_STATS_TIMEOUT, TimeValue.timeValueMinutes(10),
                "Timeout value when collecting index statistics (default to 10m)"));
        map.put(INDICES_STATS_TIMEOUT, timeoutSetting(INDICES_STATS_TIMEOUT, TimeValue.timeValueMinutes(10),
                "Timeout value when collecting total indices statistics (default to 10m)"));
        map.put(INDICES, arraySetting(INDICES, Strings.EMPTY_ARRAY,
                "List of indices names whose stats will be exported (default to all indices)"));
        map.put(CLUSTER_STATE_TIMEOUT, timeoutSetting(CLUSTER_STATE_TIMEOUT, TimeValue.timeValueMinutes(10),
                "Timeout value when collecting the cluster state (default to 10m)"));
        map.put(CLUSTER_STATS_TIMEOUT, timeoutSetting(CLUSTER_STATS_TIMEOUT, TimeValue.timeValueMinutes(10),
                "Timeout value when collecting the cluster statistics (default to 10m)"));
        map.put(INDEX_RECOVERY_TIMEOUT, timeoutSetting(INDEX_RECOVERY_TIMEOUT, TimeValue.timeValueMinutes(10),
                "Timeout value when collecting the recovery information (default to 10m)"));
        map.put(INDEX_RECOVERY_ACTIVE_ONLY, booleanSetting(INDEX_RECOVERY_ACTIVE_ONLY, Boolean.FALSE,
                "Flag to indicate if only active recoveries should be collected (default to false: all recoveries are collected)"));
        map.put(COLLECTORS, arraySetting(COLLECTORS, Strings.EMPTY_ARRAY,
                "List of collectors allowed to collect data (default to all)"));
        map.put(LICENSE_GRACE_PERIOD, timeSetting(LICENSE_GRACE_PERIOD, MAX_LICENSE_GRACE_PERIOD,
                "Period during which the agent continues to collect data even if the license is expired (default to 7 days, cannot be greater than 7 days)"));
        return Collections.unmodifiableMap(map);
    }

    public static Map<String, Validator> dynamicSettings() {
        Map<String, Validator> dynamics = new HashMap<>();
        dynamics.put(INTERVAL, Validator.TIME);
        dynamics.put(INDEX_STATS_TIMEOUT, Validator.TIMEOUT);
        dynamics.put(INDICES_STATS_TIMEOUT, Validator.TIMEOUT);
        dynamics.put(INDICES + ".*", Validator.EMPTY);
        dynamics.put(CLUSTER_STATE_TIMEOUT, Validator.TIMEOUT);
        dynamics.put(CLUSTER_STATS_TIMEOUT, Validator.TIMEOUT);
        dynamics.put(INDEX_RECOVERY_TIMEOUT, Validator.TIMEOUT);
        dynamics.put(INDEX_RECOVERY_ACTIVE_ONLY, Validator.BOOLEAN);
        return dynamics;
    }

    @Override
    public void onRefreshSettings(Settings clusterSettings) {
        if (clusterSettings.names() == null || clusterSettings.names().isEmpty()) {
            return;
        }
        updateSettings(clusterSettings);
    }

    private void updateSettings(Settings clusterSettings) {
        for (MarvelSetting setting : settings.values()) {
            if (setting.onRefresh(clusterSettings)) {
                logger.info("{} updated", setting);
            }
        }
    }

    /**
     * Returns the setting corresponding to the given name
     *
     * @param name The given name
     * @return The associated setting, null if not found
     */
    MarvelSetting getSetting(String name) {
        MarvelSetting setting = settings.get(name);
        if (setting == null) {
            throw new IllegalArgumentException("no marvel setting initialized for [" + name + "]");
        }
        return setting;
    }

    /**
     * Returns the settings corresponding to the given name
     *
     * @param name The given name
     * @return The associated setting
     */
    <T> T getSettingValue(String name) {
        MarvelSetting setting = getSetting(name);
        if (setting == null) {
            throw new IllegalArgumentException("no marvel setting initialized for [" + name + "]");
        }
        return (T) setting.getValue();
    }

    Collection<? extends MarvelSetting> settings() {
        return settings.values();
    }

    public TimeValue interval() {
        return getSettingValue(INTERVAL);
    }

    public TimeValue startUpDelay() {
        return getSettingValue(STARTUP_DELAY);
    }

    public TimeValue indexStatsTimeout() {
        return getSettingValue(INDEX_STATS_TIMEOUT);
    }

    public TimeValue indicesStatsTimeout() {
        return getSettingValue(INDICES_STATS_TIMEOUT);
    }

    public String[] indices() {
        return getSettingValue(INDICES);
    }

    public TimeValue clusterStateTimeout() {
        return getSettingValue(CLUSTER_STATE_TIMEOUT);
    }

    public TimeValue clusterStatsTimeout() {
        return getSettingValue(CLUSTER_STATS_TIMEOUT);
    }

    public TimeValue recoveryTimeout() {
        return getSettingValue(INDEX_RECOVERY_TIMEOUT);
    }

    public boolean recoveryActiveOnly() {
        return getSettingValue(INDEX_RECOVERY_ACTIVE_ONLY);
    }

    public String[] collectors() {
        return getSettingValue(COLLECTORS);
    }

    public TimeValue licenseExpirationGracePeriod() {
        TimeValue delay = getSettingValue(LICENSE_GRACE_PERIOD);
        if ((delay.millis() >= 0) && (delay.millis() < MAX_LICENSE_GRACE_PERIOD.millis())) {
            return delay;
        }
        return MAX_LICENSE_GRACE_PERIOD;
    }
}
