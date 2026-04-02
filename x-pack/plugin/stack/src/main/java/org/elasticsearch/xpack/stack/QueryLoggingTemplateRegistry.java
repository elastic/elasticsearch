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
    // history (please add a comment why you increased the version here)
    // version 1: initial placeholder
    public static final int INDEX_TEMPLATE_VERSION = 1;

    public static final String QUERY_LOGGING_TEMPLATE_VERSION_VARIABLE = "xpack.stack.querylog.template.version";

    public static final String QUERY_LOGGING_MAPPINGS_NAME = ".query-logging-mappings";
    public static final String QUERY_LOGGING_SETTINGS_NAME = ".query-logging-settings";
    public static final String QUERY_LOGGING_TEMPLATE_NAME = ".query-logging-template";
    public static final String QUERY_LOGGING_POLICY_NAME = ".query-logging-ilm-policy";

    public QueryLoggingTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    private final Map<String, ComponentTemplate> componentTemplates = parseComponentTemplates(
        new IndexTemplateConfig(
            QUERY_LOGGING_MAPPINGS_NAME,
            "/logging/querylog-mappings.json",
            INDEX_TEMPLATE_VERSION,
            QUERY_LOGGING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            QUERY_LOGGING_SETTINGS_NAME,
            "/logging/querylog-settings.json",
            INDEX_TEMPLATE_VERSION,
            QUERY_LOGGING_TEMPLATE_VERSION_VARIABLE
        )
    );

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        return componentTemplates;
    }

    private final Map<String, ComposableIndexTemplate> composableIndexTemplateConfigs = parseComposableTemplates(
        new IndexTemplateConfig(
            QUERY_LOGGING_TEMPLATE_NAME,
            "/logging/querylog-template.json",
            INDEX_TEMPLATE_VERSION,
            QUERY_LOGGING_TEMPLATE_VERSION_VARIABLE,
            Map.of(
                "xpack.stack.querylog.template.deprecated",
                "false",
                "xpack.stack.querylog.template.pattern",
                ".logs-elasticsearch.querylog-*"
            )
        )
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return composableIndexTemplateConfigs;
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.STACK_ORIGIN;
    }
}
