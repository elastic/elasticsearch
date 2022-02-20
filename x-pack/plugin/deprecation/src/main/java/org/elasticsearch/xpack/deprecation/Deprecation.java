/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.RateLimitingFilter;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.deprecation.logging.DeprecationCacheResetAction;
import org.elasticsearch.xpack.deprecation.logging.DeprecationIndexingComponent;
import org.elasticsearch.xpack.deprecation.logging.DeprecationIndexingTemplateRegistry;
import org.elasticsearch.xpack.deprecation.logging.RestDeprecationCacheResetAction;
import org.elasticsearch.xpack.deprecation.logging.TransportDeprecationCacheResetAction;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.deprecation.DeprecationChecks.SKIP_DEPRECATIONS_SETTING;

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
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(DeprecationInfoAction.INSTANCE, TransportDeprecationInfoAction.class),
            new ActionHandler<>(NodesDeprecationCheckAction.INSTANCE, TransportNodeDeprecationCheckAction.class),
            new ActionHandler<>(DeprecationCacheResetAction.INSTANCE, TransportDeprecationCacheResetAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {

        return List.of(new RestDeprecationInfoAction(), new RestDeprecationCacheResetAction());
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        final DeprecationIndexingTemplateRegistry templateRegistry = new DeprecationIndexingTemplateRegistry(
            environment.settings(),
            clusterService,
            threadPool,
            client,
            xContentRegistry
        );
        templateRegistry.initialize();

        final RateLimitingFilter rateLimitingFilterForIndexing = new RateLimitingFilter();
        // enable on start.
        rateLimitingFilterForIndexing.setUseXOpaqueId(USE_X_OPAQUE_ID_IN_FILTERING.get(environment.settings()));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(USE_X_OPAQUE_ID_IN_FILTERING, rateLimitingFilterForIndexing::setUseXOpaqueId);

        final DeprecationIndexingComponent component = DeprecationIndexingComponent.createDeprecationIndexingComponent(
            client,
            environment.settings(),
            rateLimitingFilterForIndexing,
            WRITE_DEPRECATION_LOGS_TO_INDEX.get(environment.settings()), // pass the default on startup
            clusterService
        );

        return List.of(component, rateLimitingFilterForIndexing);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(USE_X_OPAQUE_ID_IN_FILTERING, WRITE_DEPRECATION_LOGS_TO_INDEX, SKIP_DEPRECATIONS_SETTING);
    }
}
