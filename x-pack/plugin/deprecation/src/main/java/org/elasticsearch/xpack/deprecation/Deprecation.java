/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.RateLimitingFilter;
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
import org.elasticsearch.xpack.deprecation.logging.DeprecationCacheResetAction;
import org.elasticsearch.xpack.deprecation.logging.DeprecationIndexingComponent;
import org.elasticsearch.xpack.deprecation.logging.DeprecationIndexingTemplateRegistry;
import org.elasticsearch.xpack.deprecation.logging.RestDeprecationCacheResetAction;
import org.elasticsearch.xpack.deprecation.logging.TransportDeprecationCacheResetAction;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.deprecation.TransportDeprecationInfoAction.SKIP_DEPRECATIONS_SETTING;
import static org.elasticsearch.xpack.deprecation.logging.DeprecationIndexingComponent.DEPRECATION_INDEXING_FLUSH_INTERVAL;

/**
 * The plugin class for the Deprecation API
 */
public class Deprecation extends Plugin implements ActionPlugin {

    public static final Setting<Boolean> WRITE_DEPRECATION_LOGS_TO_INDEX = Setting.boolSetting(
        "cluster.deprecation_indexing.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> USE_X_OPAQUE_ID_IN_FILTERING = Setting.boolSetting(
        "cluster.deprecation_indexing.x_opaque_id_used.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(DeprecationInfoAction.INSTANCE, TransportDeprecationInfoAction.class),
            new ActionHandler(NodesDeprecationCheckAction.INSTANCE, TransportNodeDeprecationCheckAction.class),
            new ActionHandler(DeprecationCacheResetAction.INSTANCE, TransportDeprecationCacheResetAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
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

        return List.of(new RestDeprecationInfoAction(), new RestDeprecationCacheResetAction());
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        final DeprecationIndexingTemplateRegistry templateRegistry = new DeprecationIndexingTemplateRegistry(
            services.environment().settings(),
            services.clusterService(),
            services.threadPool(),
            services.client(),
            services.xContentRegistry(),
            services.projectResolver()
        );
        templateRegistry.initialize();

        final RateLimitingFilter rateLimitingFilterForIndexing = new RateLimitingFilter();
        // enable on start.
        rateLimitingFilterForIndexing.setUseXOpaqueId(USE_X_OPAQUE_ID_IN_FILTERING.get(services.environment().settings()));
        services.clusterService()
            .getClusterSettings()
            .addSettingsUpdateConsumer(USE_X_OPAQUE_ID_IN_FILTERING, rateLimitingFilterForIndexing::setUseXOpaqueId);

        final DeprecationIndexingComponent component = DeprecationIndexingComponent.createDeprecationIndexingComponent(
            services.client(),
            services.environment().settings(),
            rateLimitingFilterForIndexing,
            WRITE_DEPRECATION_LOGS_TO_INDEX.get(services.environment().settings()), // pass the default on startup
            services.clusterService()
        );

        return List.of(component, rateLimitingFilterForIndexing);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            USE_X_OPAQUE_ID_IN_FILTERING,
            WRITE_DEPRECATION_LOGS_TO_INDEX,
            SKIP_DEPRECATIONS_SETTING,
            DEPRECATION_INDEXING_FLUSH_INTERVAL
        );
    }
}
