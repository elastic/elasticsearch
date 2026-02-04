/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collection;
import java.util.List;

public class PrometheusPlugin extends Plugin {

    public static final FeatureFlag PROMETHEUS_FEATURE_FLAG = new FeatureFlag("prometheus");

    // Controls enabling the index template registry.
    // This setting will be ignored if the plugin is disabled.
    static final Setting<Boolean> PROMETHEUS_REGISTRY_ENABLED = Setting.boolSetting(
        "xpack.prometheus.registry.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(PrometheusPlugin.class);

    private final SetOnce<PrometheusIndexTemplateRegistry> registry = new SetOnce<>();
    private final boolean enabled;

    public PrometheusPlugin(Settings settings) {
        this.enabled = XPackSettings.PROMETHEUS_ENABLED.get(settings) && PROMETHEUS_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        logger.info("Prometheus plugin is {}", enabled ? "enabled" : "disabled");
        Settings settings = services.environment().settings();
        ClusterService clusterService = services.clusterService();
        registry.set(
            new PrometheusIndexTemplateRegistry(
                settings,
                clusterService,
                services.threadPool(),
                services.client(),
                services.xContentRegistry()
            )
        );
        if (enabled) {
            PrometheusIndexTemplateRegistry registryInstance = registry.get();
            registryInstance.setEnabled(PROMETHEUS_REGISTRY_ENABLED.get(settings));
            registryInstance.initialize();
        }
        return List.of();
    }

    @Override
    public void close() {
        if (registry.get() != null) {
            registry.get().close();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(PROMETHEUS_REGISTRY_ENABLED);
    }
}
