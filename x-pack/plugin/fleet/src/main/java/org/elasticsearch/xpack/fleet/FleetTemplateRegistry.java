/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.List;
import java.util.Map;

public class FleetTemplateRegistry extends IndexTemplateRegistry {

    public static final int INDEX_TEMPLATE_VERSION = 1;

    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.fleet.template.version";

    private static final List<LifecyclePolicyConfig> LIFECYCLE_POLICIES_CONFIG = List.of(
        new LifecyclePolicyConfig(".fleet-actions-results-ilm-policy", "/fleet-actions-results-ilm-policy.json"),
        new LifecyclePolicyConfig(".fleet-file-tohost-data-ilm-policy", "/fleet-file-tohost-data-ilm-policy.json"),
        new LifecyclePolicyConfig(".fleet-file-tohost-meta-ilm-policy", "/fleet-file-tohost-meta-ilm-policy.json"),
        new LifecyclePolicyConfig(".fleet-file-fromhost-data-ilm-policy", "/fleet-file-fromhost-data-ilm-policy.json"),
        new LifecyclePolicyConfig(".fleet-file-fromhost-meta-ilm-policy", "/fleet-file-fromhost-meta-ilm-policy.json")
    );

    public static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(
            ".fleet-fileds-fromhost-meta",
            "/fleet-file-fromhost-meta.json",
            INDEX_TEMPLATE_VERSION,
            TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            ".fleet-fileds-fromhost-data",
            "/fleet-file-fromhost-data.json",
            INDEX_TEMPLATE_VERSION,
            TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            ".fleet-fileds-tohost-meta",
            "/fleet-file-tohost-meta.json",
            INDEX_TEMPLATE_VERSION,
            TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            ".fleet-fileds-tohost-data",
            "/fleet-file-tohost-data.json",
            INDEX_TEMPLATE_VERSION,
            TEMPLATE_VERSION_VARIABLE
        )
    );

    public FleetTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ProjectResolver projectResolver
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, projectResolver);
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.FLEET_ORIGIN;
    }

    @Override
    protected List<LifecyclePolicyConfig> getLifecycleConfigs() {
        return LIFECYCLE_POLICIES_CONFIG;
    }

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
    }
}
