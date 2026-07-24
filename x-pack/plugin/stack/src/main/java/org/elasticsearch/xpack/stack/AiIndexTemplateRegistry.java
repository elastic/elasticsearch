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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;

import java.util.Map;

/**
 * Installs the built-in component and composable index templates that back AI indices.
 *
 * <p>Two backing store types are supported, distinguished by their index name prefix: standard indices
 * ({@code ai-index-idx-*}) for persistent data requiring in-place updates, and data streams
 * ({@code ai-index-ds-*}) for time series data. Both share a common set of field mappings and compose it
 * with a type-specific settings component.
 */
public class AiIndexTemplateRegistry extends IndexTemplateRegistry {

    // This number must be incremented when we make changes to built-in templates.
    static final int REGISTRY_VERSION = 1;

    // The computed checksum of all templates and components that are registered in this registry.
    static final String COMPUTED_CHECKSUM = "27635e19";

    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.stack.ai-index.template.version";

    public static final String AI_INDEX_IDX_PREFIX = "ai-index-idx-";
    public static final String AI_INDEX_DS_PREFIX = "ai-index-ds-";

    public static final String AI_INDEX_IDX_PATTERN = AI_INDEX_IDX_PREFIX + "*";
    public static final String AI_INDEX_DS_PATTERN = AI_INDEX_DS_PREFIX + "*";

    public static final String AI_INDEX_MAPPINGS_COMPONENT_NAME = "ai-index@mappings";
    public static final String AI_INDEX_DS_SETTINGS_COMPONENT_NAME = "ai-index@ds-settings";

    public static final String AI_INDEX_IDX_TEMPLATE_NAME = "ai-index-idx";
    public static final String AI_INDEX_DS_TEMPLATE_NAME = "ai-index-ds";

    private static final String ROOT_RESOURCE_PATH = "/ai-index/";
    private static final String JSON_EXTENSION = ".json";

    /**
     * Setting for tests to disable the registry.
     */
    public static final Setting<Boolean> AI_INDEX_REGISTRY_ENABLED = Setting.boolSetting(
        "xpack.stack.ai-index.registry.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean aiIndexRegistryEnabled;

    private final Map<String, ComponentTemplate> componentTemplates = parseComponentTemplates(componentTemplateConfigs());

    static IndexTemplateConfig[] componentTemplateConfigs() {
        return new IndexTemplateConfig[] {
            new IndexTemplateConfig(
                AI_INDEX_MAPPINGS_COMPONENT_NAME,
                ROOT_RESOURCE_PATH + AI_INDEX_MAPPINGS_COMPONENT_NAME + JSON_EXTENSION,
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                AI_INDEX_DS_SETTINGS_COMPONENT_NAME,
                ROOT_RESOURCE_PATH + AI_INDEX_DS_SETTINGS_COMPONENT_NAME + JSON_EXTENSION,
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ) };
    }

    private final Map<String, ComposableIndexTemplate> composableIndexTemplates = parseComposableTemplates(composableTemplateConfigs());

    static IndexTemplateConfig[] composableTemplateConfigs() {
        return new IndexTemplateConfig[] {
            new IndexTemplateConfig(
                AI_INDEX_IDX_TEMPLATE_NAME,
                ROOT_RESOURCE_PATH + AI_INDEX_IDX_TEMPLATE_NAME + JSON_EXTENSION,
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                Map.of("ai-index.idx.index_pattern", AI_INDEX_IDX_PATTERN)
            ),
            new IndexTemplateConfig(
                AI_INDEX_DS_TEMPLATE_NAME,
                ROOT_RESOURCE_PATH + AI_INDEX_DS_TEMPLATE_NAME + JSON_EXTENSION,
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                Map.of("ai-index.ds.index_pattern", AI_INDEX_DS_PATTERN)
            ) };
    }

    public AiIndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        FeatureService featureService
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, featureService);
        this.aiIndexRegistryEnabled = AI_INDEX_REGISTRY_ENABLED.get(nodeSettings);
    }

    @Override
    public void initialize() {
        super.initialize();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AI_INDEX_REGISTRY_ENABLED, this::updateEnabledSetting);
    }

    private void updateEnabledSetting(boolean newValue) {
        this.aiIndexRegistryEnabled = newValue;
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.STACK_ORIGIN;
    }

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        return aiIndexRegistryEnabled ? componentTemplates : Map.of();
    }

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return aiIndexRegistryEnabled ? composableIndexTemplates : Map.of();
    }
}
