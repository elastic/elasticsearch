/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

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

/**
 * Manage the index template and associated ILM policy for the async search history index.
 */
public class AsyncSearchHistoryTemplateRegistry extends IndexTemplateRegistry {
    // history (please add a comment why you increased the version here)
    // version 1: initial
    public static final String INDEX_TEMPLATE_VERSION = "1";
    public static final String ASYNC_SEARCH_HISTORY_TEMPLATE_VERSION_VARIABLE = "xpack.async-search-history.template.version";
    public static final String ASYNC_SEARCH_HISTORY_TEMPLATE_NAME = ".async-search-history";

    public static final String ASYNC_SEARCH_POLICY_NAME = "async-search-history-ilm-policy";

    public static final IndexTemplateConfig TEMPLATE_ASYNC_SEARCH = new IndexTemplateConfig(
        ASYNC_SEARCH_HISTORY_TEMPLATE_NAME,
        "/async-search-history.json",
        INDEX_TEMPLATE_VERSION,
        ASYNC_SEARCH_HISTORY_TEMPLATE_VERSION_VARIABLE
    );

    public static final LifecyclePolicyConfig ASYNC_SEARCH_HISTORY_POLICY = new LifecyclePolicyConfig(
        ASYNC_SEARCH_POLICY_NAME,
        "/async-search-history-ilm-policy.json"
    );

    public AsyncSearchHistoryTemplateRegistry(Settings nodeSettings,
                                              ClusterService clusterService,
                                              ThreadPool threadPool,
                                              Client client,
                                              NamedXContentRegistry xContentRegistry) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    protected List<IndexTemplateConfig> getTemplateConfigs() {
        return Collections.singletonList(TEMPLATE_ASYNC_SEARCH);
    }

    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        return Collections.singletonList(ASYNC_SEARCH_HISTORY_POLICY);
    }

    @Override
    protected String getOrigin() {
        return INDEX_LIFECYCLE_ORIGIN;
    }

    public boolean validate(ClusterState state) {
        boolean allTemplatesPresent = getTemplateConfigs().stream()
            .map(IndexTemplateConfig::getTemplateName)
            .allMatch(name -> state.metaData().getTemplates().containsKey(name));

        Optional<Map<String, LifecyclePolicy>> maybePolicies = Optional
            .<IndexLifecycleMetadata>ofNullable(state.metaData().custom(IndexLifecycleMetadata.TYPE))
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
