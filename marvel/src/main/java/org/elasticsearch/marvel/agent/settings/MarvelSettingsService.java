/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.settings;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.agent.settings.MarvelSetting.StringArraySetting;
import org.elasticsearch.marvel.agent.settings.MarvelSetting.TimeValueSetting;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.List;

public class MarvelSettingsService extends AbstractComponent implements NodeSettingsService.Listener {

    private static final String PREFIX = MarvelPlugin.NAME + ".agent.";

    private final List<MarvelSetting> settings;

    final TimeValueSetting indexStatsTimeout = MarvelSetting.timeSetting(PREFIX + "index.stats.timeout", TimeValue.timeValueMinutes(10),
            "Timeout value when collecting indices statistics (default to 10m)");

    final StringArraySetting indices = MarvelSetting.arraySetting(PREFIX + "indices", Strings.EMPTY_ARRAY,
            "List of indices names whose stats will be exported (default to all indices)");

    final TimeValueSetting clusterStateTimeout = MarvelSetting.timeSetting(PREFIX + "cluster.state.timeout", TimeValue.timeValueMinutes(10),
            "Timeout value when collecting the cluster state (default to 10m)");

    MarvelSettingsService(Settings clusterSettings) {
        super(clusterSettings);

        // List of marvel settings
        ImmutableList.Builder<MarvelSetting> builder = ImmutableList.builder();
        builder.add(indexStatsTimeout);
        builder.add(indices);
        builder.add(clusterStateTimeout);
        this.settings = builder.build();

        logger.trace("initializing marvel settings:");
        for (MarvelSetting setting : settings) {
            // Initialize all settings and register them as a dynamic settings
            if (setting.onInit(clusterSettings)) {
                logger.trace("\t{} ({}) initialized to [{}]", setting.getName(), setting.getDescription(), setting.getValueAsString());
            } else {
                logger.trace("\t{} ({}) initialized", setting.getName(), setting.getDescription());
            }
        }
    }

    @Inject
    public MarvelSettingsService(Settings clusterSettings, NodeSettingsService nodeSettingsService, @ClusterDynamicSettings DynamicSettings dynamicSettings) {
        this(clusterSettings);

        logger.trace("registering dynamic marvel settings:");
        for (MarvelSetting setting : settings) {
            if (setting.isDynamic()) {
                logger.trace("dynamic setting [{}] registered", setting.getName());
                dynamicSettings.addDynamicSetting(setting.dynamicSettingName());
            }
        }

        logger.trace("registering the service as a node settings listener");
        nodeSettingsService.addListener(this);
    }

    @Override
    public void onRefreshSettings(Settings clusterSettings) {
        for (MarvelSetting setting : settings) {
            if (setting.onRefresh(clusterSettings)) {
                logger.trace("setting [{}] updated to [{}]", setting.getName(), setting.getValueAsString());
            }
        }
    }

    public TimeValue indexStatsTimeout() {
        return indexStatsTimeout.getValue();
    }

    public String[] indices() {
        return indices.getValue();
    }

    public TimeValue clusterStateTimeout() {
        return clusterStateTimeout.getValue();
    }
}
