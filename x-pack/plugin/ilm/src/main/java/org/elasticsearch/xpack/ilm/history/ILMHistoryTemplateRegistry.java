/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.history;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.List;
import java.util.Map;

/**
 * The {@link ILMHistoryTemplateRegistry} class sets up and configures an ILM policy and index
 * template for the ILM history indices (ilm-history-N-00000M).
 */
public class ILMHistoryTemplateRegistry extends IndexTemplateRegistry {
    // history (please add a comment why you increased the version here)
    // version 1: initial
    // version 2: convert to hidden index
    // version 3: templates moved to composable templates
    // version 4: add `allow_auto_create` setting
    // version 5: convert to data stream
    // version 6: manage by data stream lifecycle
    // version 7: version the index template name so we can upgrade existing deployments
    public static final int INDEX_TEMPLATE_VERSION = 7;

    public static final String ILM_TEMPLATE_VERSION_VARIABLE = "xpack.ilm_history.template.version";
    public static final String ILM_TEMPLATE_NAME = "ilm-history-" + INDEX_TEMPLATE_VERSION;

    public static final String ILM_POLICY_NAME = "ilm-history-ilm-policy";

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    private final boolean ilmHistoryEnabled;

    public ILMHistoryTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        this.ilmHistoryEnabled = LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.get(nodeSettings);
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(ILM_TEMPLATE_NAME, "/ilm-history.json", INDEX_TEMPLATE_VERSION, ILM_TEMPLATE_VERSION_VARIABLE)
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        if (this.ilmHistoryEnabled) {
            return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
        } else {
            return Map.of();
        }
    }

    private static final LifecyclePolicyConfig LIFECYCLE_POLICY_CONFIG = new LifecyclePolicyConfig(
        ILM_POLICY_NAME,
        "/ilm-history-ilm-policy.json"
    );

    @Override
    protected List<LifecyclePolicyConfig> getLifecycleConfigs() {
        return List.of(LIFECYCLE_POLICY_CONFIG);
    }

    @Override
    protected List<LifecyclePolicy> getLifecyclePolicies() {
        if (ilmHistoryEnabled) {
            return lifecyclePolicies;
        } else {
            return List.of();
        }
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.INDEX_LIFECYCLE_ORIGIN;
    }
}
