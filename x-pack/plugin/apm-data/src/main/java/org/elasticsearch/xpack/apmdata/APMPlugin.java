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

public class APMPlugin extends Plugin implements ActionPlugin {
    private static final Logger logger = LogManager.getLogger(APMPlugin.class);

    final SetOnce<APMIndexTemplateRegistry> registry = new SetOnce<>();

    private final boolean enabled;

    // APM_DATA_REGISTRY_ENABLED controls enabling the index template registry.
    //
    // This setting will be ignored if the plugin is disabled.
    static final Setting<Boolean> APM_DATA_REGISTRY_ENABLED = Setting.boolSetting(
        "xpack.apm_data.registry.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public APMPlugin(Settings settings) {
        this.enabled = XPackSettings.APM_DATA_ENABLED.get(settings);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        logger.info("APM ingest plugin is {}", enabled ? "enabled" : "disabled");
        Settings settings = services.environment().settings();
        ClusterService clusterService = services.clusterService();
        registry.set(
            new APMIndexTemplateRegistry(
                settings,
                clusterService,
                services.threadPool(),
                services.client(),
                services.xContentRegistry(),
                services.featureService()
            )
        );
        if (enabled) {
            APMIndexTemplateRegistry registryInstance = registry.get();
            registryInstance.setEnabled(APM_DATA_REGISTRY_ENABLED.get(settings));
            registryInstance.initialize();
        }
        return Collections.emptyList();
    }

    @Override
    public void close() {
        registry.get().close();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(APM_DATA_REGISTRY_ENABLED);
    }
}
