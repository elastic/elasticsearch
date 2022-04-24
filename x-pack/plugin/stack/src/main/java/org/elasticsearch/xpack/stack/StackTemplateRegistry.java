/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.stack.StackTemplateBundle.STACK_TEMPLATES_ENABLED;

public class StackTemplateRegistry extends IndexTemplateRegistry {

    private final ClusterService clusterService;
    private volatile boolean stackTemplateEnabled;

    //////////////////////////////////////////////////////////
    // Built in ILM policies for users to use
    //////////////////////////////////////////////////////////
    public static final String ILM_7_DAYS_POLICY_NAME = "7-days-default";
    public static final String ILM_30_DAYS_POLICY_NAME = "30-days-default";
    public static final String ILM_90_DAYS_POLICY_NAME = "90-days-default";
    public static final String ILM_180_DAYS_POLICY_NAME = "180-days-default";
    public static final String ILM_365_DAYS_POLICY_NAME = "365-days-default";

    //////////////////////////////////////////////////////////
    // Logs components (for matching logs-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String LOGS_ILM_POLICY_NAME = "logs";

    //////////////////////////////////////////////////////////
    // Metrics components (for matching metric-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String METRICS_ILM_POLICY_NAME = "metrics";

    //////////////////////////////////////////////////////////
    // Synthetics components (for matching synthetics-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String SYNTHETICS_ILM_POLICY_NAME = "synthetics";

    public StackTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        this.clusterService = clusterService;
        this.stackTemplateEnabled = STACK_TEMPLATES_ENABLED.get(nodeSettings);
    }

    @Override
    public void initialize() {
        super.initialize();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(STACK_TEMPLATES_ENABLED, this::updateEnabledSetting);
    }

    private void updateEnabledSetting(boolean newValue) {
        if (newValue) {
            this.stackTemplateEnabled = true;
        } else {
            this.stackTemplateEnabled = false;
        }
    }

    private static final List<LifecyclePolicy> LIFECYCLE_POLICY_CONFIGS = Stream.of(
        new LifecyclePolicyConfig(LOGS_ILM_POLICY_NAME, "/logs-policy.json"),
        new LifecyclePolicyConfig(METRICS_ILM_POLICY_NAME, "/metrics-policy.json"),
        new LifecyclePolicyConfig(SYNTHETICS_ILM_POLICY_NAME, "/synthetics-policy.json"),
        new LifecyclePolicyConfig(ILM_7_DAYS_POLICY_NAME, "/" + ILM_7_DAYS_POLICY_NAME + ".json"),
        new LifecyclePolicyConfig(ILM_30_DAYS_POLICY_NAME, "/" + ILM_30_DAYS_POLICY_NAME + ".json"),
        new LifecyclePolicyConfig(ILM_90_DAYS_POLICY_NAME, "/" + ILM_90_DAYS_POLICY_NAME + ".json"),
        new LifecyclePolicyConfig(ILM_180_DAYS_POLICY_NAME, "/" + ILM_180_DAYS_POLICY_NAME + ".json"),
        new LifecyclePolicyConfig(ILM_365_DAYS_POLICY_NAME, "/" + ILM_365_DAYS_POLICY_NAME + ".json")
    ).map(config -> config.load(LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY)).toList();

    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        if (stackTemplateEnabled) {
            return LIFECYCLE_POLICY_CONFIGS;
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.STACK_ORIGIN;
    }

    @Override
    protected boolean requiresMasterNode() {
        // Stack templates use the composable index template and component APIs,
        // these APIs aren't supported in 7.7 and earlier and in mixed cluster
        // environments this can cause a lot of ActionNotFoundTransportException
        // errors in the logs during rolling upgrades. If these templates
        // are only installed via elected master node then the APIs are always
        // there and the ActionNotFoundTransportException errors are then prevented.
        return true;
    }
}
