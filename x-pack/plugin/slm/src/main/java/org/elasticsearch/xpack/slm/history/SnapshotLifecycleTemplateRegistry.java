/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.history;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
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
    // version 3: templates moved to composable templates
    // version 4:converted data stream
    // version 5: add `allow_auto_create` setting
    // version 6: manage by data stream lifecycle
    // version 7: version the index template name so we can upgrade existing deployments
    public static final int INDEX_TEMPLATE_VERSION = 7;

    public static final String SLM_TEMPLATE_VERSION_VARIABLE = "xpack.slm.template.version";
    public static final String SLM_TEMPLATE_NAME = ".slm-history-" + INDEX_TEMPLATE_VERSION;

    public static final String SLM_POLICY_NAME = "slm-history-ilm-policy";

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    private final boolean slmHistoryEnabled;

    public SnapshotLifecycleTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        slmHistoryEnabled = SLM_HISTORY_INDEX_ENABLED_SETTING.get(nodeSettings);
    }

    public static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(SLM_TEMPLATE_NAME, "/slm-history.json", INDEX_TEMPLATE_VERSION, SLM_TEMPLATE_VERSION_VARIABLE)
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        if (slmHistoryEnabled == false) {
            return Map.of();
        }
        return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
    }

    private static final List<LifecyclePolicyConfig> LIFECYCLE_POLICY_CONFIGS = List.of(
        new LifecyclePolicyConfig(SLM_POLICY_NAME, "/slm-history-ilm-policy.json")
    );

    @Override
    protected List<LifecyclePolicy> getLifecyclePolicies() {
        if (slmHistoryEnabled == false) {
            return Collections.emptyList();
        }
        return lifecyclePolicies;
    }

    @Override
    protected List<LifecyclePolicyConfig> getLifecycleConfigs() {
        return LIFECYCLE_POLICY_CONFIGS;
    }

    @Override
    protected String getOrigin() {
        return INDEX_LIFECYCLE_ORIGIN; // TODO use separate SLM origin?
    }

    public boolean validate(ClusterState state) {
        boolean allTemplatesPresent = getComposableTemplateConfigs().keySet()
            .stream()
            .allMatch(name -> state.metadata().templatesV2().containsKey(name));

        Optional<Map<String, LifecyclePolicy>> maybePolicies = Optional.<IndexLifecycleMetadata>ofNullable(
            state.metadata().custom(IndexLifecycleMetadata.TYPE)
        ).map(IndexLifecycleMetadata::getPolicies);
        Set<String> policyNames = getLifecyclePolicies().stream().map(LifecyclePolicy::getName).collect(Collectors.toSet());

        boolean allPoliciesPresent = maybePolicies.map(policies -> policies.keySet().containsAll(policyNames)).orElse(false);
        return allTemplatesPresent && allPoliciesPresent;
    }
}
