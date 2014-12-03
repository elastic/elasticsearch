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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.IndexMissingException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 */
public class ConfigurationManager extends AbstractComponent {

    private final Client client;

    public static final String CONFIG_TYPE = "config";
    public static final String CONFIG_INDEX = AlertsStore.ALERT_INDEX;
    private final String GLOBAL_CONFIG_NAME = "global";
    private volatile boolean readyToRead = false;
    private volatile ImmutableOpenMap<String, List<ConfigurableComponentListener>> componentNameToListener;

    @Inject
    public ConfigurationManager(Settings settings, Client client) {
        super(settings);
        this.client = client;
        componentNameToListener = ImmutableOpenMap.<String, List<ConfigurableComponentListener>>builder().build();
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
     * Notify the listeners of a new config
     * @param componentName
     * @param settingsSource
     */
    public void newConfig(String componentName, BytesReference settingsSource) {
        Map<String, Object> settingsMap = XContentHelper.convertToMap(settingsSource, true).v2();
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
        for (Map.Entry<String, Object> configEntry : settingsMap.entrySet() ) {
            settingsBuilder.put(configEntry.getKey(), configEntry.getValue());
        }
        Settings settings = settingsBuilder.build();
        for (ConfigurableComponentListener componentListener : componentNameToListener.get(componentName)) {
            componentListener.receiveConfigurationUpdate(settings);
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

    /**
     * Registers an component to receive config updates
     * @param componentName
     * @param configListener
     */
    public synchronized void registerListener(String componentName, ConfigurableComponentListener configListener) {
        if (componentNameToListener.get(componentName) == null ){
            List<ConfigurableComponentListener> componentListeners = new CopyOnWriteArrayList<>();
            componentListeners.add(configListener);
            ImmutableOpenMap.Builder componentNameToListenerBuilder = ImmutableOpenMap.builder(componentNameToListener)
                    .fPut(componentName, componentListeners);
            componentNameToListener = componentNameToListenerBuilder.build();
        } else if (!componentNameToListener.get(componentName).contains(configListener)) {
            componentNameToListener.get(componentName).add(configListener);
        }
    }
}
