/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.stack;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.BuiltinTemplatePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.template.IndexTemplateRegistry.parseComposableTemplates;

public class StackPlugin extends Plugin implements ActionPlugin, BuiltinTemplatePlugin {

    // The stack template registry version. This number must be incremented when we make changes
    // to built-in templates.
    public static final int REGISTRY_VERSION = 1;
    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.stack.template.version";

    //////////////////////////////////////////////////////////
    // Logs components (for matching logs-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "logs-mappings";
    public static final String LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME = "logs-settings";
    public static final String LOGS_INDEX_TEMPLATE_NAME = "logs";

    //////////////////////////////////////////////////////////
    // Metrics components (for matching metric-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "metrics-mappings";
    public static final String METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "metrics-settings";
    public static final String METRICS_INDEX_TEMPLATE_NAME = "metrics";

    //////////////////////////////////////////////////////////
    // Synthetics components (for matching synthetics-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "synthetics-mappings";
    public static final String SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "synthetics-settings";
    public static final String SYNTHETICS_INDEX_TEMPLATE_NAME = "synthetics";

    // General mappings conventions for any data that ends up in a data stream
    public static final String DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "data-streams-mappings";

    private static final Map<String, ComponentTemplate> COMPONENT_TEMPLATE_CONFIGS;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/data-streams-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/logs-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/logs-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/metrics-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/metrics-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/synthetics-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/synthetics-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
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

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(LOGS_INDEX_TEMPLATE_NAME, "/logs-template.json", REGISTRY_VERSION, TEMPLATE_VERSION_VARIABLE),
        new IndexTemplateConfig(METRICS_INDEX_TEMPLATE_NAME, "/metrics-template.json", REGISTRY_VERSION, TEMPLATE_VERSION_VARIABLE),
        new IndexTemplateConfig(SYNTHETICS_INDEX_TEMPLATE_NAME, "/synthetics-template.json", REGISTRY_VERSION, TEMPLATE_VERSION_VARIABLE)
    );

    private final Settings settings;

    public StackPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(StackTemplateRegistry.STACK_TEMPLATES_ENABLED);
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
        StackTemplateRegistry templateRegistry = new StackTemplateRegistry(settings, clusterService, threadPool, client, xContentRegistry);
        templateRegistry.initialize();
        return Collections.singleton(templateRegistry);
    }

    @Override
    public Map<String, ComponentTemplate> getComponentTemplates() {
        // TODO: if enabled
        return COMPONENT_TEMPLATE_CONFIGS;
    }

    @Override
    public Map<String, ComposableIndexTemplate> getComposableIndexTemplates() {
        // TODO: if enabled
        return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
    }

    @Override
    public String getOrigin() {
        return ClientHelper.STACK_ORIGIN;
    }
}
