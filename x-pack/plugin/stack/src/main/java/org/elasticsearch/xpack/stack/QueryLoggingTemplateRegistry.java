/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;

import java.util.Map;

/**
 * Manages the index template for query logging.
 */
public class QueryLoggingTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(QueryLoggingTemplateRegistry.class);

    // history (please add a comment why you increased the version here)
    // version 1: initial template
    // version 2: limit query to 32k
    public static final int INDEX_TEMPLATE_VERSION = 2;

    public static final String QUERY_LOGGING_TEMPLATE_VERSION_VARIABLE = "xpack.stack.querylog.template.version";

    public static final String QUERY_LOGGING_MAPPINGS_NAME = "logs-elasticsearch.querylog@mappings";
    public static final String QUERY_LOGGING_SETTINGS_NAME = "logs-elasticsearch.querylog@settings";
    public static final String QUERY_LOGGING_TEMPLATE_NAME = "logs-elasticsearch.querylog@template";

    /**
     * Setting for tests to disable the registry.
     */
    public static final Setting<Boolean> QUERY_LOGGING_REGISTRY_ENABLED = Setting.boolSetting(
        "xpack.stack.querylog.registry.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean queryLoggingRegistryEnabled;

    public QueryLoggingTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        this.queryLoggingRegistryEnabled = QUERY_LOGGING_REGISTRY_ENABLED.get(nodeSettings);
    }

    @Override
    public void initialize() {
        super.initialize();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(QUERY_LOGGING_REGISTRY_ENABLED, this::updateEnabledSetting);
    }

    private void updateEnabledSetting(boolean newValue) {
        this.queryLoggingRegistryEnabled = newValue;
    }

    private final Map<String, ComponentTemplate> componentTemplates = parseComponentTemplates(
        new IndexTemplateConfig(
            QUERY_LOGGING_MAPPINGS_NAME,
            "/activitylog/logs-elasticsearch.querylog@mappings.json",
            INDEX_TEMPLATE_VERSION,
            QUERY_LOGGING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            QUERY_LOGGING_SETTINGS_NAME,
            "/activitylog/logs-elasticsearch.querylog@settings.json",
            INDEX_TEMPLATE_VERSION,
            QUERY_LOGGING_TEMPLATE_VERSION_VARIABLE
        )
    );

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        if (queryLoggingRegistryEnabled) {
            return componentTemplates;
        }
        return Map.of();
    }

    private final Map<String, ComposableIndexTemplate> composableIndexTemplateConfigs = parseComposableTemplates(
        new IndexTemplateConfig(
            QUERY_LOGGING_TEMPLATE_NAME,
            "/activitylog/logs-elasticsearch.querylog@template.json",
            INDEX_TEMPLATE_VERSION,
            QUERY_LOGGING_TEMPLATE_VERSION_VARIABLE,
            Map.of(
                "xpack.stack.querylog.template.deprecated",
                "false",
                "xpack.stack.querylog.template.pattern",
                "logs-elasticsearch.querylog-*"
            )
        )
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        if (queryLoggingRegistryEnabled) {
            return composableIndexTemplateConfigs;
        }
        return Map.of();
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.STACK_ORIGIN;
    }
}
