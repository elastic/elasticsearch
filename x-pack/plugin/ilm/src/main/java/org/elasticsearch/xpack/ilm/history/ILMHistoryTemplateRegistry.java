/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.history;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.Collections;
import java.util.List;

/**
 * The {@link ILMHistoryTemplateRegistry} class sets up and configures an ILM policy and index
 * template for the ILM history indices (ilm-history-N-00000M).
 */
public class ILMHistoryTemplateRegistry extends IndexTemplateRegistry {
    // history (please add a comment why you increased the version here)
    // version 1: initial
    // version 2: convert to hidden index
    public static final int INDEX_TEMPLATE_VERSION = 2;

    public static final String ILM_TEMPLATE_VERSION_VARIABLE = "xpack.ilm_history.template.version";
    public static final String ILM_TEMPLATE_NAME = "ilm-history";

    public static final String ILM_POLICY_NAME = "ilm-history-ilm-policy";

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    public static final IndexTemplateConfig TEMPLATE_ILM_HISTORY = new IndexTemplateConfig(
        ILM_TEMPLATE_NAME,
        "/ilm-history.json",
        INDEX_TEMPLATE_VERSION,
        ILM_TEMPLATE_VERSION_VARIABLE
    );

    public static final LifecyclePolicyConfig ILM_HISTORY_POLICY = new LifecyclePolicyConfig(
        ILM_POLICY_NAME,
        "/ilm-history-ilm-policy.json"
    );

    private final boolean ilmHistoryEnabled;

    public ILMHistoryTemplateRegistry(Settings nodeSettings, ClusterService clusterService,
                                      ThreadPool threadPool, Client client,
                                      NamedXContentRegistry xContentRegistry) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        this.ilmHistoryEnabled = LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.get(nodeSettings);
    }

    @Override
    protected List<IndexTemplateConfig> getTemplateConfigs() {
        if (this.ilmHistoryEnabled) {
            return Collections.singletonList(TEMPLATE_ILM_HISTORY);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        if (this.ilmHistoryEnabled) {
            return Collections.singletonList(ILM_HISTORY_POLICY);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.INDEX_LIFECYCLE_ORIGIN;
    }
}
