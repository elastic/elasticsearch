/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;

import java.util.Map;

/**
 */
public class ConfigurationManager extends AbstractComponent {

    private final Client client;

    public final String CONFIG_TYPE = "config";
    public final String CONFIG_INDEX = AlertsStore.ALERT_INDEX;
    private final String GLOBAL_CONFIG_NAME = "global";
    private final Settings settings;
    private volatile boolean readyToRead = false;

    @Inject
    public ConfigurationManager(Settings settings, Client client) {
        super(settings);
        this.client = client;
        this.settings = settings;
    }

    /**
     * This method gets the config for a component name
     * @param componentName
     * @return The immutable settings loaded from the index
     */
    public Settings getConfigForComponent(String componentName) {
        ensureReady();
        try {
            client.admin().indices().prepareRefresh(CONFIG_INDEX).get();
        } catch (IndexMissingException ime) {
            logger.info("No index [" + CONFIG_INDEX + "] found");
            return null;
        }
        GetResponse response = client.prepareGet(CONFIG_INDEX, CONFIG_TYPE, componentName).get();
        if (!response.isExists()) {
            return null;
        }
        Map<String, Object> sourceMap = response.getSourceAsMap();
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
        for (Map.Entry<String, Object> configEntry : sourceMap.entrySet() ) {
            settingsBuilder.put(configEntry.getKey(), configEntry.getValue());
        }
        return settingsBuilder.build();
    }

    public Settings getGlobalConfig() {
        return getConfigForComponent(GLOBAL_CONFIG_NAME);
    }

    /**
     * This method looks in the indexed settings provided for a setting and if it's not there it will go to the
     * this.settings and load it from there using the default if not found
     */
    public TimeValue getOverriddenTimeValue(String settingName, Settings indexedSettings, TimeValue defaultValue) {
        if (indexedSettings == null || indexedSettings.get(settingName) == null) {
            return settings.getAsTime(settingName, defaultValue);
        } else {
            return indexedSettings.getAsTime(settingName, defaultValue);
        }
    }

    public int getOverriddenIntValue(String settingName, Settings indexedSettings, int defaultValue) {
        if (indexedSettings == null || indexedSettings.get(settingName) == null) {
            return settings.getAsInt(settingName, defaultValue);
        } else {
            return indexedSettings.getAsInt(settingName, defaultValue);
        }
    }

    public boolean getOverriddenBooleanValue(String settingName, Settings indexedSettings, boolean defaultValue) {
        if (indexedSettings == null || indexedSettings.get(settingName) == null) {
            return settings.getAsBoolean(settingName, defaultValue);
        } else {
            return indexedSettings.getAsBoolean(settingName, defaultValue);
        }
    }



    /**
     * This method determines if the config manager is ready to start loading configs by checking to make sure the
     * config index is in a readable state.
     * @param clusterState
     * @return true if ready to read or false if not
     */
    public boolean isReady(ClusterState clusterState) {
        if (readyToRead) {
            return true;
        } else {
            readyToRead = checkIndexState(clusterState);
            return readyToRead;
        }
    }

    private void ensureReady() {
        if (!readyToRead) {
            throw new ElasticsearchException("Config index [" + CONFIG_INDEX + "] is not known to be started");
        }
    }

    private boolean checkIndexState(ClusterState clusterState) {
        IndexMetaData configIndexMetadata = clusterState.getMetaData().index(CONFIG_INDEX);
        if (configIndexMetadata == null) {
            logger.info("No previous [" + CONFIG_INDEX + "]");

            return true;
        } else {
            if (clusterState.routingTable().index(CONFIG_INDEX).allPrimaryShardsActive()) {
                logger.info("Index [" + CONFIG_INDEX + "] is started.");

                return true;
            } else {
                return false;
            }
        }
    }
}
