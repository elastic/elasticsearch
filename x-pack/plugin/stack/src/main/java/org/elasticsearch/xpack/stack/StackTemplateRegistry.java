/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.stack;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class StackTemplateRegistry extends IndexTemplateRegistry {
    // The stack template registry should remain at version 0. This is because templates and
    // policies will be changed by the ingest manager once they exist, and ES should only ever put
    // the template in place if it does not exist. If this were incremented we could accidentally
    // overwrite a template or policy changed by the ingest manager.
    public static final int REGISTRY_VERSION = 0;

    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.stack.template.version";

    private final boolean stackTemplateEnabled;

    //////////////////////////////////////////////////////////
    // Logs components (for matching logs-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "logs-mappings";
    public static final String LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME = "logs-settings";
    public static final String LOGS_ILM_POLICY_NAME = "logs";
    public static final String LOGS_INDEX_TEMPLATE_NAME = "logs";

    public static final IndexTemplateConfig LOGS_MAPPINGS_COMPONENT_TEMPLATE = new IndexTemplateConfig(
        LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
        "/logs-mappings.json",
        REGISTRY_VERSION,
        TEMPLATE_VERSION_VARIABLE
    );
    public static final IndexTemplateConfig LOGS_SETTINGS_COMPONENT_TEMPLATE = new IndexTemplateConfig(
        LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME,
        "/logs-settings.json",
        REGISTRY_VERSION,
        TEMPLATE_VERSION_VARIABLE
    );
    public static final LifecyclePolicyConfig LOGS_ILM_POLICY = new LifecyclePolicyConfig(LOGS_ILM_POLICY_NAME, "/logs-policy.json");
    public static final IndexTemplateConfig LOGS_INDEX_TEMPLATE = new IndexTemplateConfig(
        LOGS_INDEX_TEMPLATE_NAME,
        "/logs-template.json",
        REGISTRY_VERSION,
        TEMPLATE_VERSION_VARIABLE
    );

    //////////////////////////////////////////////////////////
    // Metrics components (for matching metric-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "metrics-mappings";
    public static final String METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "metrics-settings";
    public static final String METRICS_ILM_POLICY_NAME = "metrics";
    public static final String METRICS_INDEX_TEMPLATE_NAME = "metrics";

    public static final IndexTemplateConfig METRICS_MAPPINGS_COMPONENT_TEMPLATE = new IndexTemplateConfig(
        METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
        "/metrics-mappings.json",
        REGISTRY_VERSION,
        TEMPLATE_VERSION_VARIABLE
    );
    public static final IndexTemplateConfig METRICS_SETTINGS_COMPONENT_TEMPLATE = new IndexTemplateConfig(
        METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
        "/metrics-settings.json",
        REGISTRY_VERSION,
        TEMPLATE_VERSION_VARIABLE
    );
    public static final LifecyclePolicyConfig METRICS_ILM_POLICY = new LifecyclePolicyConfig(
        METRICS_ILM_POLICY_NAME,
        "/metrics-policy.json"
    );
    public static final IndexTemplateConfig METRICS_INDEX_TEMPLATE = new IndexTemplateConfig(
        METRICS_INDEX_TEMPLATE_NAME,
        "/metrics-template.json",
        REGISTRY_VERSION,
        TEMPLATE_VERSION_VARIABLE
    );

    public StackTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        this.stackTemplateEnabled = StackPlugin.STACK_TEMPLATES_ENABLED.get(nodeSettings);
    }

    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        if (stackTemplateEnabled) {
            return Arrays.asList(LOGS_ILM_POLICY, METRICS_ILM_POLICY);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected List<IndexTemplateConfig> getComponentTemplateConfigs() {
        if (stackTemplateEnabled) {
            return Arrays.asList(
                LOGS_MAPPINGS_COMPONENT_TEMPLATE,
                LOGS_SETTINGS_COMPONENT_TEMPLATE,
                METRICS_MAPPINGS_COMPONENT_TEMPLATE,
                METRICS_SETTINGS_COMPONENT_TEMPLATE
            );
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected List<IndexTemplateConfig> getComposableTemplateConfigs() {
        if (stackTemplateEnabled) {
            return Arrays.asList(LOGS_INDEX_TEMPLATE, METRICS_INDEX_TEMPLATE);
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
