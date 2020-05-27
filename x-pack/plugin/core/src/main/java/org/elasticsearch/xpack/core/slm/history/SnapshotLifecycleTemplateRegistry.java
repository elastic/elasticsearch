/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm.history;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;

/**
 * Manages the index template and associated ILM policy for the Snapshot
 * Lifecycle Management history index.
 */
public class SnapshotLifecycleTemplateRegistry extends IndexTemplateRegistry {
    // history (please add a comment why you increased the version here)
    // version 1: initial
    // version 2: converted to hidden index
    public static final int INDEX_TEMPLATE_VERSION = 2;

    public static final String SLM_TEMPLATE_VERSION_VARIABLE = "xpack.slm.template.version";
    public static final String SLM_TEMPLATE_NAME = ".slm-history";

    public static final String SLM_POLICY_NAME = "slm-history-ilm-policy";

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    public static final IndexTemplateConfig TEMPLATE_SLM_HISTORY = new IndexTemplateConfig(
        SLM_TEMPLATE_NAME,
        "/slm-history.json",
        INDEX_TEMPLATE_VERSION,
        SLM_TEMPLATE_VERSION_VARIABLE
    );

    public static final LifecyclePolicyConfig SLM_HISTORY_POLICY = new LifecyclePolicyConfig(
        SLM_POLICY_NAME,
        "/slm-history-ilm-policy.json"
    );

    private final boolean slmHistoryEnabled;

    public SnapshotLifecycleTemplateRegistry(Settings nodeSettings, ClusterService clusterService, ThreadPool threadPool, Client client,
                                             NamedXContentRegistry xContentRegistry) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        slmHistoryEnabled = SLM_HISTORY_INDEX_ENABLED_SETTING.get(nodeSettings);
    }

    @Override
    protected List<IndexTemplateConfig> getTemplateConfigs() {
        if (slmHistoryEnabled == false) {
            return Collections.emptyList();
        }
        return Collections.singletonList(TEMPLATE_SLM_HISTORY);
    }

    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        if (slmHistoryEnabled == false) {
            return Collections.emptyList();
        }
        return Collections.singletonList(SLM_HISTORY_POLICY);
    }

    @Override
    protected String getOrigin() {
        return INDEX_LIFECYCLE_ORIGIN; // TODO use separate SLM origin?
    }

    public boolean validate(ClusterState state) {
        boolean allTemplatesPresent = getTemplateConfigs().stream()
            .map(IndexTemplateConfig::getTemplateName)
            .allMatch(name -> state.metadata().getTemplates().containsKey(name));

        Optional<Map<String, LifecyclePolicy>> maybePolicies = Optional
            .<IndexLifecycleMetadata>ofNullable(state.metadata().custom(IndexLifecycleMetadata.TYPE))
            .map(IndexLifecycleMetadata::getPolicies);
        Set<String> policyNames = getPolicyConfigs().stream()
            .map(LifecyclePolicyConfig::getPolicyName)
            .collect(Collectors.toSet());

        boolean allPoliciesPresent = maybePolicies
            .map(policies -> policies.keySet()
                .containsAll(policyNames))
            .orElse(false);
        return allTemplatesPresent && allPoliciesPresent;
    }
}
