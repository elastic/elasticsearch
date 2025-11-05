/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.stack;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

/**
 * Plugin for Elastic Stack monitoring and observability templates in Elasticsearch.
 * <p>
 * This plugin manages the index templates used by various Elastic Stack components
 * for monitoring, logging, and observability data. It maintains both current and
 * legacy template registries to ensure compatibility across versions.
 * </p>
 */
public class StackPlugin extends Plugin implements ActionPlugin {
    private final Settings settings;

    /**
     * Constructs a new StackPlugin with the specified settings.
     *
     * @param settings the node settings used to configure the template registries
     */
    public StackPlugin(Settings settings) {
        this.settings = settings;
    }

    /**
     * Returns the list of settings provided by this plugin.
     *
     * @return a list containing the stack templates enabled setting
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(StackTemplateRegistry.STACK_TEMPLATES_ENABLED);
    }

    /**
     * Creates and initializes the plugin components.
     * <p>
     * This method creates both legacy and current template registries for Elastic Stack
     * components. The legacy registry maintains backward compatibility with older versions,
     * while the current registry provides the latest template definitions.
     * </p>
     *
     * @param services the plugin services providing access to cluster resources
     * @return a list containing both the legacy and current stack template registries
     */
    @Override
    public Collection<?> createComponents(PluginServices services) {
        LegacyStackTemplateRegistry legacyStackTemplateRegistry = new LegacyStackTemplateRegistry(
            settings,
            services.clusterService(),
            services.threadPool(),
            services.client(),
            services.xContentRegistry()
        );
        legacyStackTemplateRegistry.initialize();
        StackTemplateRegistry stackTemplateRegistry = new StackTemplateRegistry(
            settings,
            services.clusterService(),
            services.threadPool(),
            services.client(),
            services.xContentRegistry()
        );
        stackTemplateRegistry.initialize();
        return List.of(legacyStackTemplateRegistry, stackTemplateRegistry);
    }
}
