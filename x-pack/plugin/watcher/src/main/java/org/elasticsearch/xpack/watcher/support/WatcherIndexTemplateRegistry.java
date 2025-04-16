/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;
import org.elasticsearch.xpack.watcher.Watcher;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;

public class WatcherIndexTemplateRegistry extends IndexTemplateRegistry {

    public static final String WATCHER_TEMPLATE_VERSION_VARIABLE = "xpack.watcher.template.version";

    private final boolean ilmManagementEnabled;

    public WatcherIndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ProjectResolver projectResolver
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, projectResolver);
        ilmManagementEnabled = Watcher.USE_ILM_INDEX_MANAGEMENT.get(nodeSettings);
    }

    private static final Map<String, ComposableIndexTemplate> TEMPLATES_WATCH_HISTORY = parseComposableTemplates(
        new IndexTemplateConfig(
            WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME,
            "/watch-history.json",
            WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
            WATCHER_TEMPLATE_VERSION_VARIABLE
        )
    );

    private static final Map<String, ComposableIndexTemplate> TEMPLATES_WATCH_HISTORY_NO_ILM = parseComposableTemplates(
        new IndexTemplateConfig(
            WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME_NO_ILM,
            "/watch-history-no-ilm.json",
            WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
            WATCHER_TEMPLATE_VERSION_VARIABLE
        )
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return ilmManagementEnabled ? TEMPLATES_WATCH_HISTORY : TEMPLATES_WATCH_HISTORY_NO_ILM;
    }

    private static final LifecyclePolicyConfig LIFECYCLE_POLICIES = new LifecyclePolicyConfig(
        "watch-history-ilm-policy-16",
        "/watch-history-ilm-policy.json"
    );

    @Override
    protected List<LifecyclePolicyConfig> getLifecycleConfigs() {
        return List.of(LIFECYCLE_POLICIES);
    }

    /**
     * If Watcher is configured not to use ILM, we don't return a policy.
     */
    @Override
    protected List<LifecyclePolicy> getLifecyclePolicies() {
        return Watcher.USE_ILM_INDEX_MANAGEMENT.get(settings) == false ? Collections.emptyList() : lifecyclePolicies;
    }

    @Override
    protected String getOrigin() {
        return WATCHER_ORIGIN;
    }

    public static boolean validate(ClusterState state) {
        return state.getMetadata()
            .getProject()
            .templatesV2()
            .keySet()
            .stream()
            .filter(s -> s.startsWith(".watch-history-"))
            .map(s -> Integer.valueOf(s.substring(s.lastIndexOf('-') + 1)))
            .anyMatch(version -> version >= 12);
    }
}
