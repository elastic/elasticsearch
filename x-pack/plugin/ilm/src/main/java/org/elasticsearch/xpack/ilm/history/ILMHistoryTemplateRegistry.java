/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.history;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.util.Collections;
import java.util.List;

/**
 * The {@link ILMHistoryTemplateRegistry} class sets up and configures an ILM policy and index
 * template for the ILM history indices (ilm-history-N-00000M).
 */
public class ILMHistoryTemplateRegistry extends IndexTemplateRegistry {

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

    private static final List<LifecyclePolicy> LIFECYCLE_POLICIES = List.of(
        new LifecyclePolicyConfig(ILM_POLICY_NAME, "/ilm-history-ilm-policy.json").load(
            new NamedXContentRegistry(IndexLifecycle.NAMED_X_CONTENT_ENTRIES)
        )
    );

    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        return this.ilmHistoryEnabled ? LIFECYCLE_POLICIES : Collections.emptyList();
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.INDEX_LIFECYCLE_ORIGIN;
    }
}
