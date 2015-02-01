/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Simple service to get settings that are persisted in the a special document in the .alerts index.
 * Also notifies known components about setting changes.
 *
 * The service requires on the fact that the alert service has been started.
 */
public class ConfigurationService extends AbstractComponent {

    public static final String CONFIG_TYPE = "config";
    public static final String GLOBAL_CONFIG_NAME = "global";

    private final ClientProxy client;
    private final CopyOnWriteArrayList<ConfigurableComponentListener> registeredComponents;

    @Inject
    public ConfigurationService(Settings settings, ClientProxy client) {
        super(settings);
        this.client = client;
        registeredComponents = new CopyOnWriteArrayList<>();
    }

    /**
     * This method gets the config
     * @return The immutable settings loaded from the index
     */
    public Settings getConfig() {
        try {
            client.admin().indices().prepareRefresh(AlertsStore.ALERT_INDEX).get();
        } catch (IndexMissingException ime) {
            logger.error("No index [" + AlertsStore.ALERT_INDEX + "] found");
            return null;
        }
        GetResponse response = client.prepareGet(AlertsStore.ALERT_INDEX, CONFIG_TYPE, GLOBAL_CONFIG_NAME).get();
        if (response.isExists()) {
            return ImmutableSettings.settingsBuilder().loadFromSource(response.getSourceAsString()).build();
        } else {
            return null;
        }
    }

    /**
     * Notify the listeners of a new config
     */
    public IndexResponse updateConfig(BytesReference settingsSource) throws IOException {

        IndexResponse indexResponse = client.prepareIndex(AlertsStore.ALERT_INDEX, ConfigurationService.CONFIG_TYPE, ConfigurationService.GLOBAL_CONFIG_NAME)
                .setSource(settingsSource).get();

        Settings settings = ImmutableSettings.settingsBuilder().loadFromSource(settingsSource.toUtf8()).build();
        for (ConfigurableComponentListener componentListener : registeredComponents) {
            componentListener.receiveConfigurationUpdate(settings);
        }

        return indexResponse;
    }

    /**
     * Registers an component to receive config updates
     */
    public void registerListener(ConfigurableComponentListener configListener) {
        if (!registeredComponents.contains(configListener)) {
            registeredComponents.add(configListener);
        }
    }
}
