/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.stack;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

public class StackPlugin extends Plugin implements ActionPlugin {
    private final Settings settings;

    public StackPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            StackTemplateRegistry.STACK_TEMPLATES_ENABLED,
            QueryLoggingTemplateRegistry.QUERY_LOGGING_REGISTRY_ENABLED,
            AiIndexTemplateRegistry.AI_INDEX_REGISTRY_ENABLED
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        LegacyStackTemplateRegistry legacyStackTemplateRegistry = new LegacyStackTemplateRegistry(
            settings,
            services.clusterService(),
            services.threadPool(),
            services.client(),
            services.xContentRegistry(),
            services.featureService()
        );
        legacyStackTemplateRegistry.initialize();
        StackTemplateRegistry stackTemplateRegistry = new StackTemplateRegistry(
            settings,
            services.clusterService(),
            services.threadPool(),
            services.client(),
            services.xContentRegistry(),
            services.featureService()
        );
        stackTemplateRegistry.initialize();
        QueryLoggingTemplateRegistry queryLoggingTemplateRegistry = new QueryLoggingTemplateRegistry(
            settings,
            services.clusterService(),
            services.threadPool(),
            services.client(),
            services.xContentRegistry(),
            services.featureService()
        );
        queryLoggingTemplateRegistry.initialize();
        AiIndexTemplateRegistry aiIndexTemplateRegistry = new AiIndexTemplateRegistry(
            settings,
            services.clusterService(),
            services.threadPool(),
            services.client(),
            services.xContentRegistry(),
            services.featureService()
        );
        aiIndexTemplateRegistry.initialize();
        return List.of(legacyStackTemplateRegistry, stackTemplateRegistry, queryLoggingTemplateRegistry, aiIndexTemplateRegistry);
    }
}
