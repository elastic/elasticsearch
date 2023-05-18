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
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class StackTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(StackTemplateRegistry.class);

    // The stack template registry version. This number must be incremented when we make changes
    // to built-in templates.
    public static final int REGISTRY_VERSION = 3;

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
    public static final String LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "logs-mappings";
    public static final String LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME = "logs-settings";
    public static final String LOGS_ILM_POLICY_NAME = "logs";
    public static final String LOGS_INDEX_TEMPLATE_NAME = "logs";

    //////////////////////////////////////////////////////////
    // Metrics components (for matching metric-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "metrics-mappings";
    public static final String METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "metrics-settings";
    public static final String METRICS_ILM_POLICY_NAME = "metrics";
    public static final String METRICS_INDEX_TEMPLATE_NAME = "metrics";

    //////////////////////////////////////////////////////////
    // Synthetics components (for matching synthetics-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "synthetics-mappings";
    public static final String SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "synthetics-settings";
    public static final String SYNTHETICS_ILM_POLICY_NAME = "synthetics";
    public static final String SYNTHETICS_INDEX_TEMPLATE_NAME = "synthetics";

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
                String.join(",", getComposableTemplateConfigs().keySet()),
                String.join(",", getComponentTemplateConfigs().keySet())
            );
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

    private static final Map<String, ComponentTemplate> COMPONENT_TEMPLATE_CONFIGS;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/data-streams-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/logs-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/logs-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/metrics-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/metrics-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/synthetics-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/synthetics-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            )
        )) {
            try {
                componentTemplates.put(
                    config.getTemplateName(),
                    ComponentTemplate.parse(JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes()))
                );
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        COMPONENT_TEMPLATE_CONFIGS = Map.copyOf(componentTemplates);
    }

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        if (stackTemplateEnabled) {
            return COMPONENT_TEMPLATE_CONFIGS;
        } else {
            return Map.of();
        }
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(LOGS_INDEX_TEMPLATE_NAME, "/logs-template.json", REGISTRY_VERSION, TEMPLATE_VERSION_VARIABLE),
        new IndexTemplateConfig(METRICS_INDEX_TEMPLATE_NAME, "/metrics-template.json", REGISTRY_VERSION, TEMPLATE_VERSION_VARIABLE),
        new IndexTemplateConfig(SYNTHETICS_INDEX_TEMPLATE_NAME, "/synthetics-template.json", REGISTRY_VERSION, TEMPLATE_VERSION_VARIABLE)
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        if (stackTemplateEnabled) {
            return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
        } else {
            return Map.of();
        }
    }

    private static final List<IngestPipelineConfig> INGEST_PIPELINE_CONFIGS = List.of(
        new IngestPipelineConfig("logs-default-pipeline", "/logs-default-pipeline.json", REGISTRY_VERSION, TEMPLATE_VERSION_VARIABLE)
    );

    @Override
    protected List<IngestPipelineConfig> getIngestPipelines() {
        return INGEST_PIPELINE_CONFIGS;
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
