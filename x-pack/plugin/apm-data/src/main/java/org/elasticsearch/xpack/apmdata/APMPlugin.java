/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apmdata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Plugin for APM (Application Performance Monitoring) data management in Elasticsearch.
 * <p>
 * This plugin manages the index templates and mappings required for APM data ingestion.
 * It creates and maintains an {@link APMIndexTemplateRegistry} that handles the lifecycle
 * of APM-related index templates.
 * </p>
 */
public class APMPlugin extends Plugin implements ActionPlugin {
    private static final Logger logger = LogManager.getLogger(APMPlugin.class);

    final SetOnce<APMIndexTemplateRegistry> registry = new SetOnce<>();

    private final boolean enabled;

    /**
     * Controls whether the APM data index template registry is enabled.
     * <p>
     * This setting is ignored if the APM data plugin itself is disabled via
     * {@link XPackSettings#APM_DATA_ENABLED}.
     * </p>
     */
    static final Setting<Boolean> APM_DATA_REGISTRY_ENABLED = Setting.boolSetting(
        "xpack.apm_data.registry.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Constructs a new APMPlugin with the specified settings.
     *
     * @param settings the node settings used to determine if APM data functionality is enabled
     */
    public APMPlugin(Settings settings) {
        this.enabled = XPackSettings.APM_DATA_ENABLED.get(settings);
    }

    /**
     * Creates and initializes the plugin components.
     * <p>
     * This method creates the {@link APMIndexTemplateRegistry} which manages APM index templates.
     * If the plugin is enabled, the registry is initialized and configured according to the
     * {@link #APM_DATA_REGISTRY_ENABLED} setting. If disabled, the registry is created but not initialized.
     * </p>
     *
     * @param services the plugin services providing access to cluster resources
     * @return an empty collection as this plugin does not export any components
     */
    @Override
    public Collection<?> createComponents(PluginServices services) {
        logger.info("APM ingest plugin is {}", enabled ? "enabled" : "disabled");
        Settings settings = services.environment().settings();
        ClusterService clusterService = services.clusterService();
        registry.set(
            new APMIndexTemplateRegistry(settings, clusterService, services.threadPool(), services.client(), services.xContentRegistry())
        );
        if (enabled) {
            APMIndexTemplateRegistry registryInstance = registry.get();
            registryInstance.setEnabled(APM_DATA_REGISTRY_ENABLED.get(settings));
            registryInstance.initialize();
        }
        return Collections.emptyList();
    }

    /**
     * Closes the plugin and releases resources.
     * <p>
     * This method ensures the APM index template registry is properly closed and
     * any associated resources are released.
     * </p>
     */
    @Override
    public void close() {
        registry.get().close();
    }

    /**
     * Returns the list of settings provided by this plugin.
     *
     * @return a list containing the {@link #APM_DATA_REGISTRY_ENABLED} setting
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(APM_DATA_REGISTRY_ENABLED);
    }
}
