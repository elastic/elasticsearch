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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.cluster.settings.ClusterSettings.CLUSTER_LOGSDB_ENABLED;

public class StackTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(StackTemplateRegistry.class);

    // Historical node feature kept here as LegacyStackTemplateRegistry is deprecated
    public static final NodeFeature STACK_TEMPLATES_FEATURE = new NodeFeature("stack.templates_supported");

    // this node feature is a redefinition of {@link DataStreamFeatures#DATA_STREAM_LIFECYCLE} and it's meant to avoid adding a
    // dependency to the data-streams module just for this
    public static final NodeFeature DATA_STREAM_LIFECYCLE = new NodeFeature("data_stream.lifecycle");

    // The stack template registry version. This number must be incremented when we make changes
    // to built-in templates.
    public static final int REGISTRY_VERSION = 14;

    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.stack.template.version";
    public static final Setting<Boolean> STACK_TEMPLATES_ENABLED = Setting.boolSetting(
        "stack.templates.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final Map<String, ComponentTemplate> componentTemplateConfigs;
    private volatile boolean stackTemplateEnabled;

    public static final Map<String, String> ADDITIONAL_TEMPLATE_VARIABLES = Map.of("xpack.stack.template.deprecated", "false");

    // General mappings conventions for any data that ends up in a data stream
    public static final String DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "data-streams@mappings";

    // ECS dynamic mappings
    public static final String ECS_DYNAMIC_MAPPINGS_COMPONENT_TEMPLATE_NAME = "ecs@mappings";

    //////////////////////////////////////////////////////////
    // Built in ILM policies for users to use
    //////////////////////////////////////////////////////////
    public static final String ILM_7_DAYS_POLICY_NAME = "7-days@lifecycle";
    public static final String ILM_30_DAYS_POLICY_NAME = "30-days@lifecycle";
    public static final String ILM_90_DAYS_POLICY_NAME = "90-days@lifecycle";
    public static final String ILM_180_DAYS_POLICY_NAME = "180-days@lifecycle";
    public static final String ILM_365_DAYS_POLICY_NAME = "365-days@lifecycle";

    //////////////////////////////////////////////////////////
    // Logs components (for matching logs-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "logs@mappings";
    public static final String LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME = "logs@settings";
    public static final String LOGS_ILM_POLICY_NAME = "logs@lifecycle";
    public static final String LOGS_INDEX_TEMPLATE_NAME = "logs";

    //////////////////////////////////////////////////////////
    // Metrics components (for matching metric-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "metrics@mappings";
    public static final String METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "metrics@settings";
    public static final String METRICS_TSDB_SETTINGS_COMPONENT_TEMPLATE_NAME = "metrics@tsdb-settings";
    public static final String METRICS_ILM_POLICY_NAME = "metrics@lifecycle";
    public static final String METRICS_INDEX_TEMPLATE_NAME = "metrics";

    //////////////////////////////////////////////////////////
    // Base traces components
    //////////////////////////////////////////////////////////
    public static final String TRACES_MAPPINGS_COMPONENT_TEMPLATE_NAME = "traces@mappings";
    public static final String TRACES_SETTINGS_COMPONENT_TEMPLATE_NAME = "traces@settings";

    //////////////////////////////////////////////////////////
    // Synthetics components (for matching synthetics-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "synthetics@mappings";
    public static final String SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "synthetics@settings";
    public static final String SYNTHETICS_ILM_POLICY_NAME = "synthetics@lifecycle";
    public static final String SYNTHETICS_INDEX_TEMPLATE_NAME = "synthetics";

    ///////////////////////////////////
    // Kibana reporting template
    ///////////////////////////////////
    public static final String KIBANA_REPORTING_INDEX_TEMPLATE_NAME = ".kibana-reporting";
    public static final String KIBANA_REPORTING_COMPONENT_TEMPLATE_NAME = "kibana-reporting@settings";

    public StackTemplateRegistry(
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
        this.componentTemplateConfigs = loadComponentTemplateConfigs(CLUSTER_LOGSDB_ENABLED.get(nodeSettings));
    }

    private Map<String, ComponentTemplate> loadComponentTemplateConfigs(boolean logsDbEnabled) {
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
                logsDbEnabled ? "/logs@settings-logsdb.json" : "/logs@settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                Map.of("xpack.stack.template.deprecated", "false")
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
                TRACES_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/traces@settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                ADDITIONAL_TEMPLATE_VARIABLES
            ),
            new IndexTemplateConfig(
                TRACES_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/traces@mappings.json",
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
            ),
            new IndexTemplateConfig(
                KIBANA_REPORTING_COMPONENT_TEMPLATE_NAME,
                "/kibana-reporting@settings.json",
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
        return Map.copyOf(componentTemplates);
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
        return lifecyclePolicies;
    }

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        return componentTemplateConfigs;
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(
            LOGS_INDEX_TEMPLATE_NAME,
            "/logs@template.json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            ADDITIONAL_TEMPLATE_VARIABLES
        ),
        new IndexTemplateConfig(
            METRICS_INDEX_TEMPLATE_NAME,
            "/metrics@template.json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            ADDITIONAL_TEMPLATE_VARIABLES
        ),
        new IndexTemplateConfig(
            SYNTHETICS_INDEX_TEMPLATE_NAME,
            "/synthetics@template.json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            ADDITIONAL_TEMPLATE_VARIABLES
        ),
        new IndexTemplateConfig(
            KIBANA_REPORTING_INDEX_TEMPLATE_NAME,
            "/kibana-reporting@template.json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            ADDITIONAL_TEMPLATE_VARIABLES
        )
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
        new JsonIngestPipelineConfig(
            "logs@json-pipeline",
            "/logs@json-pipeline.json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            List.of(),
            ADDITIONAL_TEMPLATE_VARIABLES
        ),
        new JsonIngestPipelineConfig(
            "logs@default-pipeline",
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
        // Ensure current version of the components are installed only after versions that support data stream lifecycle
        // due to .kibana-reporting making use of the feature
        return featureService.clusterHasFeature(event.state(), DATA_STREAM_LIFECYCLE);
    }
}
