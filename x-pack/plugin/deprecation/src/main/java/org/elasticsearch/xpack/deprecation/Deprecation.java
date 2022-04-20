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
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
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
import org.elasticsearch.plugins.BuiltinTemplatePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.deprecation.logging.DeprecationCacheResetAction;
import org.elasticsearch.xpack.deprecation.logging.DeprecationIndexingComponent;
import org.elasticsearch.xpack.deprecation.logging.DeprecationIndexingTemplateRegistry;
import org.elasticsearch.xpack.deprecation.logging.RestDeprecationCacheResetAction;
import org.elasticsearch.xpack.deprecation.logging.TransportDeprecationCacheResetAction;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.DEPRECATION_ORIGIN;
import static org.elasticsearch.xpack.core.template.IndexTemplateRegistry.parseComposableTemplates;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.SKIP_DEPRECATIONS_SETTING;

/**
 * The plugin class for the Deprecation API
 */
public class Deprecation extends Plugin implements ActionPlugin, BuiltinTemplatePlugin {

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

    // history (please add a comment why you increased the version here)
    // version 1: initial
    public static final int INDEX_TEMPLATE_VERSION = 1;

    public static final String DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE = "xpack.deprecation.indexing.template.version";

    public static final String DEPRECATION_INDEXING_MAPPINGS_NAME = ".deprecation-indexing-mappings";
    public static final String DEPRECATION_INDEXING_SETTINGS_NAME = ".deprecation-indexing-settings";
    public static final String DEPRECATION_INDEXING_TEMPLATE_NAME = ".deprecation-indexing-template";

    private static final Map<String, ComponentTemplate> COMPONENT_TEMPLATE_CONFIGS;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                DEPRECATION_INDEXING_MAPPINGS_NAME,
                "/org/elasticsearch/xpack/deprecation/deprecation-indexing-mappings.json",
                INDEX_TEMPLATE_VERSION,
                DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                DEPRECATION_INDEXING_SETTINGS_NAME,
                "/org/elasticsearch/xpack/deprecation/deprecation-indexing-settings.json",
                INDEX_TEMPLATE_VERSION,
                DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
            )
        )) {
            try {
                componentTemplates.put(
                    config.getTemplateName(),
                    ComponentTemplate.parse(JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes()))
                );
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        COMPONENT_TEMPLATE_CONFIGS = Map.copyOf(componentTemplates);
    }

    @Override
    public Map<String, ComponentTemplate> getComponentTemplates() {
        return COMPONENT_TEMPLATE_CONFIGS;
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(
            DEPRECATION_INDEXING_TEMPLATE_NAME,
            "/org/elasticsearch/xpack/deprecation/deprecation-indexing-template.json",
            INDEX_TEMPLATE_VERSION,
            DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
        )
    );

    @Override
    public Map<String, ComposableIndexTemplate> getComposableIndexTemplates() {
        return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
    }

    @Override
    public String getOrigin() {
        return DEPRECATION_ORIGIN;
    }
}
