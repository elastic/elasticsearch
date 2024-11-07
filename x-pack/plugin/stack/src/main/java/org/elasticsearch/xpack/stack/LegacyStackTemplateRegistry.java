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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;
import org.elasticsearch.xpack.core.template.JsonIngestPipelineConfig;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.stack.StackTemplateRegistry.STACK_TEMPLATES_ENABLED;
import static org.elasticsearch.xpack.stack.StackTemplateRegistry.STACK_TEMPLATES_FEATURE;

@Deprecated(since = "8.12.0", forRemoval = true)
public class LegacyStackTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(LegacyStackTemplateRegistry.class);

    // The stack template registry version. This number must be incremented when we make changes
    // to built-in templates.
    public static final int REGISTRY_VERSION = 5;

    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.stack.template.version";

    private final ClusterService clusterService;
    private final FeatureService featureService;
    private volatile boolean stackTemplateEnabled;

    private static final Map<String, String> ADDITIONAL_TEMPLATE_VARIABLES = Map.of("xpack.stack.template.deprecated", "true");

    // General mappings conventions for any data that ends up in a data stream
    public static final String DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "data-streams-mappings";

    // ECS dynamic mappings
    public static final String ECS_DYNAMIC_MAPPINGS_COMPONENT_TEMPLATE_NAME = "ecs@dynamic_templates";

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

    //////////////////////////////////////////////////////////
    // Metrics components (for matching metric-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "metrics-mappings";
    public static final String METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "metrics-settings";
    public static final String METRICS_TSDB_SETTINGS_COMPONENT_TEMPLATE_NAME = "metrics-tsdb-settings";
    public static final String METRICS_ILM_POLICY_NAME = "metrics";

    //////////////////////////////////////////////////////////
    // Synthetics components (for matching synthetics-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "synthetics-mappings";
    public static final String SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "synthetics-settings";
    public static final String SYNTHETICS_ILM_POLICY_NAME = "synthetics";

    public LegacyStackTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        FeatureService featureService
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        this.clusterService = clusterService;
        this.featureService = featureService;
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

    private static final List<LifecyclePolicyConfig> LIFECYCLE_POLICY_CONFIGS = List.of(
        new LifecyclePolicyConfig(LOGS_ILM_POLICY_NAME, "/logs@lifecycle.json", ADDITIONAL_TEMPLATE_VARIABLES),
        new LifecyclePolicyConfig(METRICS_ILM_POLICY_NAME, "/metrics@lifecycle.json", ADDITIONAL_TEMPLATE_VARIABLES),
        new LifecyclePolicyConfig(SYNTHETICS_ILM_POLICY_NAME, "/synthetics@lifecycle.json", ADDITIONAL_TEMPLATE_VARIABLES),
        new LifecyclePolicyConfig(ILM_7_DAYS_POLICY_NAME, "/7-days@lifecycle.json", ADDITIONAL_TEMPLATE_VARIABLES),
        new LifecyclePolicyConfig(ILM_30_DAYS_POLICY_NAME, "/30-days@lifecycle.json", ADDITIONAL_TEMPLATE_VARIABLES),
        new LifecyclePolicyConfig(ILM_90_DAYS_POLICY_NAME, "/90-days@lifecycle.json", ADDITIONAL_TEMPLATE_VARIABLES),
        new LifecyclePolicyConfig(ILM_180_DAYS_POLICY_NAME, "/180-days@lifecycle.json", ADDITIONAL_TEMPLATE_VARIABLES),
        new LifecyclePolicyConfig(ILM_365_DAYS_POLICY_NAME, "/365-days@lifecycle.json", ADDITIONAL_TEMPLATE_VARIABLES)
    );

    @Override
    protected List<LifecyclePolicyConfig> getLifecycleConfigs() {
        return LIFECYCLE_POLICY_CONFIGS;
    }

    @Override
    protected List<LifecyclePolicy> getLifecyclePolicies() {
        if (stackTemplateEnabled) {
            return lifecyclePolicies;
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
                "/data-streams@mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                ADDITIONAL_TEMPLATE_VARIABLES
            ),
            new IndexTemplateConfig(
                LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/logs@mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                ADDITIONAL_TEMPLATE_VARIABLES
            ),
            new IndexTemplateConfig(
                ECS_DYNAMIC_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/ecs@mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                ADDITIONAL_TEMPLATE_VARIABLES
            ),
            new IndexTemplateConfig(
                LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/logs@settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                ADDITIONAL_TEMPLATE_VARIABLES
            ),
            new IndexTemplateConfig(
                METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/metrics@mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                ADDITIONAL_TEMPLATE_VARIABLES
            ),
            new IndexTemplateConfig(
                METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/metrics@settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                ADDITIONAL_TEMPLATE_VARIABLES
            ),
            new IndexTemplateConfig(
                METRICS_TSDB_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/metrics@tsdb-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                ADDITIONAL_TEMPLATE_VARIABLES
            ),
            new IndexTemplateConfig(
                SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/synthetics@mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                ADDITIONAL_TEMPLATE_VARIABLES
            ),
            new IndexTemplateConfig(
                SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/synthetics@settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                ADDITIONAL_TEMPLATE_VARIABLES
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

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return Map.of();
    }

    private static final List<IngestPipelineConfig> INGEST_PIPELINE_CONFIGS = List.of(
        new JsonIngestPipelineConfig(
            "logs@json-message",
            "/logs@json-pipeline.json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            List.of(),
            ADDITIONAL_TEMPLATE_VARIABLES
        ),
        new JsonIngestPipelineConfig(
            "logs-default-pipeline",
            "/logs@default-pipeline.json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            List.of(),
            ADDITIONAL_TEMPLATE_VARIABLES
        )
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

    @Override
    protected boolean isClusterReady(ClusterChangedEvent event) {
        // Ensure current version of the components are installed only once all nodes are updated to 8.9.0.
        // This is necessary to prevent an error caused nby the usage of the ignore_missing_pipeline property
        // in the pipeline processor, which has been introduced only in 8.9.0
        return featureService.clusterHasFeature(event.state(), STACK_TEMPLATES_FEATURE);
    }
}
