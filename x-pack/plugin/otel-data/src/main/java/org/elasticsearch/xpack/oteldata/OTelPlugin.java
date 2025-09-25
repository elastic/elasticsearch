/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.oteldata.otlp.OTLPMetricsRestAction;
import org.elasticsearch.xpack.oteldata.otlp.OTLPMetricsTransportAction;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class OTelPlugin extends Plugin implements ActionPlugin {

    // OTEL_DATA_REGISTRY_ENABLED controls enabling the index template registry.
    //
    // This setting will be ignored if the plugin is disabled.
    static final Setting<Boolean> OTEL_DATA_REGISTRY_ENABLED = Setting.boolSetting(
        "xpack.otel_data.registry.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(OTelPlugin.class);

    private final SetOnce<OTelIndexTemplateRegistry> registry = new SetOnce<>();
    private final boolean enabled;

    public OTelPlugin(Settings settings) {
        this.enabled = XPackSettings.OTEL_DATA_ENABLED.get(settings);
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new OTLPMetricsRestAction());
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        logger.info("OTel ingest plugin is {}", enabled ? "enabled" : "disabled");
        Settings settings = services.environment().settings();
        ClusterService clusterService = services.clusterService();
        registry.set(
            new OTelIndexTemplateRegistry(settings, clusterService, services.threadPool(), services.client(), services.xContentRegistry())
        );
        if (enabled) {
            OTelIndexTemplateRegistry registryInstance = registry.get();
            registryInstance.setEnabled(OTEL_DATA_REGISTRY_ENABLED.get(settings));
            registryInstance.initialize();
        }
        return List.of();
    }

    @Override
    public void close() {
        registry.get().close();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(OTEL_DATA_REGISTRY_ENABLED);
    }

    @Override
    public Collection<ActionHandler> getActions() {
        return List.of(new ActionHandler(OTLPMetricsTransportAction.TYPE, OTLPMetricsTransportAction.class));
    }
}
