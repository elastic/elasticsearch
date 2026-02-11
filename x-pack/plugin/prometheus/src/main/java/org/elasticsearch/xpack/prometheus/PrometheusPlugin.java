/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

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
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.prometheus.rest.PrometheusRemoteWriteRestAction;
import org.elasticsearch.xpack.prometheus.rest.PrometheusRemoteWriteTransportAction;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class PrometheusPlugin extends Plugin implements ActionPlugin {

    public static final FeatureFlag PROMETHEUS_FEATURE_FLAG = new FeatureFlag("prometheus");

    // Controls enabling the index template registry.
    // This setting will be ignored if the plugin is disabled.
    static final Setting<Boolean> PROMETHEUS_REGISTRY_ENABLED = Setting.boolSetting(
        "xpack.prometheus.registry.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final SetOnce<PrometheusIndexTemplateRegistry> indexTemplateRegistry = new SetOnce<>();
    private final boolean enabled;

    public PrometheusPlugin(Settings settings) {
        this.enabled = XPackSettings.PROMETHEUS_ENABLED.get(settings) && PROMETHEUS_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        Settings settings = services.environment().settings();
        ClusterService clusterService = services.clusterService();
        indexTemplateRegistry.set(
            new PrometheusIndexTemplateRegistry(
                settings,
                clusterService,
                services.threadPool(),
                services.client(),
                services.xContentRegistry()
            )
        );
        if (enabled) {
            PrometheusIndexTemplateRegistry registryInstance = indexTemplateRegistry.get();
            registryInstance.setEnabled(PROMETHEUS_REGISTRY_ENABLED.get(settings));
            registryInstance.initialize();
        }
        return List.of();
    }

    @Override
    public void close() {
        if (indexTemplateRegistry.get() != null) {
            indexTemplateRegistry.get().close();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(PROMETHEUS_REGISTRY_ENABLED);
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
        if (enabled) {
            return List.of(new PrometheusRemoteWriteRestAction());
        }
        return List.of();
    }

    @Override
    public Collection<ActionHandler> getActions() {
        if (enabled) {
            return List.of(new ActionHandler(PrometheusRemoteWriteTransportAction.TYPE, PrometheusRemoteWriteTransportAction.class));
        }
        return List.of();
    }
}
