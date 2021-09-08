/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
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
import java.util.stream.Collectors;

public class StackTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(StackTemplateRegistry.class);

    // The stack template registry version. This number must be incremented when we make changes
    // to built-in templates.
    public static final int REGISTRY_VERSION = 1;

    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.stack.template.version";
    public static final Setting<Boolean> STACK_TEMPLATES_ENABLED = Setting.boolSetting(
        "stack.templates.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ClusterService clusterService;
    private volatile boolean stackTemplateEnabled;

    // General mappings conventions for any data that ends up in a data stream
    public static final String DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "data-streams-mappings";

    public static final IndexTemplateConfig DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE = new IndexTemplateConfig(
        DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
        "/data-streams-mappings.json",
        REGISTRY_VERSION,
        TEMPLATE_VERSION_VARIABLE
    );

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

    //////////////////////////////////////////////////////////
    // Synthetics components (for matching synthetics-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "synthetics-mappings";
    public static final String SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "synthetics-settings";
    public static final String SYNTHETICS_ILM_POLICY_NAME = "synthetics";
    public static final String SYNTHETICS_INDEX_TEMPLATE_NAME = "synthetics";

    public static final IndexTemplateConfig SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE = new IndexTemplateConfig(
        SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
        "/synthetics-mappings.json",
        REGISTRY_VERSION,
        TEMPLATE_VERSION_VARIABLE
    );
    public static final IndexTemplateConfig SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE = new IndexTemplateConfig(
        SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
        "/synthetics-settings.json",
        REGISTRY_VERSION,
        TEMPLATE_VERSION_VARIABLE
    );
    public static final LifecyclePolicyConfig SYNTHETICS_ILM_POLICY = new LifecyclePolicyConfig(
        SYNTHETICS_ILM_POLICY_NAME,
        "/synthetics-policy.json"
    );
    public static final IndexTemplateConfig SYNTHETICS_INDEX_TEMPLATE = new IndexTemplateConfig(
        SYNTHETICS_INDEX_TEMPLATE_NAME,
        "/synthetics-template.json",
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
            logger.info(
                "stack composable templates [{}] and component templates [{}] will not be installed or reinstalled",
                getComposableTemplateConfigs().stream().map(IndexTemplateConfig::getTemplateName).collect(Collectors.joining(",")),
                getComponentTemplateConfigs().stream().map(IndexTemplateConfig::getTemplateName).collect(Collectors.joining(","))
            );
            this.stackTemplateEnabled = false;
        }
    }

    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        if (stackTemplateEnabled) {
            return Arrays.asList(LOGS_ILM_POLICY, METRICS_ILM_POLICY, SYNTHETICS_ILM_POLICY);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected List<IndexTemplateConfig> getComponentTemplateConfigs() {
        if (stackTemplateEnabled) {
            return Arrays.asList(
                DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE,
                LOGS_MAPPINGS_COMPONENT_TEMPLATE,
                LOGS_SETTINGS_COMPONENT_TEMPLATE,
                METRICS_MAPPINGS_COMPONENT_TEMPLATE,
                METRICS_SETTINGS_COMPONENT_TEMPLATE,
                SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE,
                SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE
            );
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected List<IndexTemplateConfig> getComposableTemplateConfigs() {
        if (stackTemplateEnabled) {
            return Arrays.asList(LOGS_INDEX_TEMPLATE, METRICS_INDEX_TEMPLATE, SYNTHETICS_INDEX_TEMPLATE);
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
