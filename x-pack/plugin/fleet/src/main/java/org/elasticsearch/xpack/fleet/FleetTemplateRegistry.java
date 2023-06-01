/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.List;
import java.util.Map;

public class FleetTemplateRegistry extends IndexTemplateRegistry {

    public static final int INDEX_TEMPLATE_VERSION = 1;

    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.fleet.template.version";

    private static final List<LifecyclePolicy> LIFECYCLE_POLICIES = List.of(
        new LifecyclePolicyConfig(".fleet-actions-results-ilm-policy", "/fleet-actions-results-ilm-policy.json").load(
            LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY
        ),
        new LifecyclePolicyConfig(".fleet-file-data-ilm-policy", "/fleet-file-data-ilm-policy.json").load(
            LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY
        ),
        new LifecyclePolicyConfig(".fleet-files-ilm-policy", "/fleet-files-ilm-policy.json").load(
            LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY
        )
    );

    public static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(".fleet-files", "/fleet-files.json", INDEX_TEMPLATE_VERSION, TEMPLATE_VERSION_VARIABLE),
        new IndexTemplateConfig(".fleet-file-data", "/fleet-file-data.json", INDEX_TEMPLATE_VERSION, TEMPLATE_VERSION_VARIABLE),
        new IndexTemplateConfig(".fleet-filedelivery-meta", "/fleet-filedelivery-meta.json", INDEX_TEMPLATE_VERSION, TEMPLATE_VERSION_VARIABLE),
        new IndexTemplateConfig(".fleet-filedelivery-data", "/fleet-filedelivery-data.json", INDEX_TEMPLATE_VERSION, TEMPLATE_VERSION_VARIABLE)
    );

    public FleetTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.FLEET_ORIGIN;
    }

    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        return LIFECYCLE_POLICIES;
    }

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
    }
}
