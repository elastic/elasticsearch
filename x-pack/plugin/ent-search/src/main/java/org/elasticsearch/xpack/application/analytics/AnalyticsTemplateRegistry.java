/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_INDEX_PATTERN;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_INDEX_PREFIX;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.ROOT_RESOURCE_PATH;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.STATELESS_ROOT_RESOURCE_PATH;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.TEMPLATE_VERSION_VARIABLE;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class AnalyticsTemplateRegistry extends IndexTemplateRegistry {

    // This registry requires all nodes to be at least 8.8.0
    static final Version MIN_NODE_VERSION = Version.V_8_8_0;

    // This number must be incremented when we make changes to built-in templates.
    static final int REGISTRY_VERSION = 2;

    // ILM Policies configuration
    static final String EVENT_DATA_STREAM_ILM_POLICY_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "default_policy";

    static final List<LifecyclePolicy> LIFECYCLE_POLICIES = Stream.of(
        new LifecyclePolicyConfig(EVENT_DATA_STREAM_ILM_POLICY_NAME, ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_ILM_POLICY_NAME + ".json")
    ).map(config -> config.load(LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY)).toList();

    // Index template components configuration
    static final String EVENT_DATA_STREAM_SETTINGS_COMPONENT_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "settings";
    static final String EVENT_DATA_STREAM_MAPPINGS_COMPONENT_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "mappings";

    static final String EVENT_DATA_STREAM_INGEST_PIPELINE_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "final_pipeline";

    final Map<String, ComponentTemplate> COMPONENT_TEMPLATES;
    final Map<String, ComponentTemplate> COMPONENT_TEMPLATES_STATELESS;

    @Override
    protected List<IngestPipelineConfig> getIngestPipelines() {
        return List.of(
            new IngestPipelineConfig(
                EVENT_DATA_STREAM_INGEST_PIPELINE_NAME,
                ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_INGEST_PIPELINE_NAME + ".json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            )
        );
    }

    // Composable index templates configuration.
    static final String EVENT_DATA_STREAM_TEMPLATE_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "default";
    static final String EVENT_DATA_STREAM_TEMPLATE_FILENAME = EVENT_DATA_STREAM_INDEX_PREFIX + "template";

    final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATES_WITH_ILM;

    final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATES_STATELESS;

    public AnalyticsTemplateRegistry(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(clusterService.getSettings(), clusterService, threadPool, client, xContentRegistry);
        if (DiscoveryNode.isStateless(clusterService.getSettings())) {
            final Map<String, ComponentTemplate> statelessComponentTemplates = new HashMap<>();
            for (IndexTemplateConfig config : List.of(
                new IndexTemplateConfig(
                    EVENT_DATA_STREAM_SETTINGS_COMPONENT_NAME,
                    STATELESS_ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_SETTINGS_COMPONENT_NAME + ".json",
                    REGISTRY_VERSION,
                    TEMPLATE_VERSION_VARIABLE
                ),
                new IndexTemplateConfig(
                    EVENT_DATA_STREAM_MAPPINGS_COMPONENT_NAME,
                    STATELESS_ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_MAPPINGS_COMPONENT_NAME + ".json",
                    REGISTRY_VERSION,
                    TEMPLATE_VERSION_VARIABLE
                )
            )) {
                try {
                    statelessComponentTemplates.put(
                        config.getTemplateName(),
                        ComponentTemplate.parse(
                            JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes())
                        )
                    );
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            COMPONENT_TEMPLATES_STATELESS = Map.copyOf(statelessComponentTemplates);

            COMPOSABLE_INDEX_TEMPLATES_STATELESS = parseComposableTemplates(
                new IndexTemplateConfig(
                    EVENT_DATA_STREAM_TEMPLATE_NAME,
                    STATELESS_ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_TEMPLATE_FILENAME + ".json",
                    REGISTRY_VERSION,
                    TEMPLATE_VERSION_VARIABLE,
                    Map.of("event_data_stream.index_pattern", EVENT_DATA_STREAM_INDEX_PATTERN)
                )
            );

            // stateful resources are not parsed
            COMPONENT_TEMPLATES = Map.of();
            COMPOSABLE_INDEX_TEMPLATES_WITH_ILM = Map.of();
        } else {
            final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
            for (IndexTemplateConfig config : List.of(
                new IndexTemplateConfig(
                    EVENT_DATA_STREAM_SETTINGS_COMPONENT_NAME,
                    ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_SETTINGS_COMPONENT_NAME + ".json",
                    REGISTRY_VERSION,
                    TEMPLATE_VERSION_VARIABLE
                ),
                new IndexTemplateConfig(
                    EVENT_DATA_STREAM_MAPPINGS_COMPONENT_NAME,
                    ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_MAPPINGS_COMPONENT_NAME + ".json",
                    REGISTRY_VERSION,
                    TEMPLATE_VERSION_VARIABLE
                )
            )) {
                try {
                    componentTemplates.put(
                        config.getTemplateName(),
                        ComponentTemplate.parse(
                            JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes())
                        )
                    );
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            COMPONENT_TEMPLATES = Map.copyOf(componentTemplates);
            COMPOSABLE_INDEX_TEMPLATES_WITH_ILM = parseComposableTemplates(
                new IndexTemplateConfig(
                    EVENT_DATA_STREAM_TEMPLATE_NAME,
                    ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_TEMPLATE_FILENAME + ".json",
                    REGISTRY_VERSION,
                    TEMPLATE_VERSION_VARIABLE,
                    Map.of("event_data_stream.index_pattern", EVENT_DATA_STREAM_INDEX_PATTERN)
                )
            );

            // stateless resources are not parsed
            COMPONENT_TEMPLATES_STATELESS = Map.of();
            COMPOSABLE_INDEX_TEMPLATES_STATELESS = Map.of();
        }
    }

    @Override
    protected String getOrigin() {
        return ENT_SEARCH_ORIGIN;
    }

    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        return LIFECYCLE_POLICIES;
    }

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        return COMPONENT_TEMPLATES;
    }

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return COMPOSABLE_INDEX_TEMPLATES_WITH_ILM;
    }

    @Override
    protected Map<String, ComponentTemplate> getStatelessComponentTemplateConfigs() {
        return COMPONENT_TEMPLATES_STATELESS;
    }

    @Override
    protected Map<String, ComposableIndexTemplate> getStatelessIndexTemplateConfigs() {
        return COMPOSABLE_INDEX_TEMPLATES_STATELESS;
    }

    @Override
    protected boolean requiresMasterNode() {
        // We are using the composable index template and component APIs,
        // these APIs aren't supported in 7.7 and earlier and in mixed cluster
        // environments this can cause a lot of ActionNotFoundTransportException
        // errors in the logs during rolling upgrades. If these templates
        // are only installed via elected master node then the APIs are always
        // there and the ActionNotFoundTransportException errors are then prevented.
        return true;
    }

    @Override
    protected boolean isClusterReady(ClusterChangedEvent event) {
        // Ensure templates are installed only once all nodes are updated to 8.8.0.
        Version minNodeVersion = event.state().nodes().getMinNodeVersion();
        return minNodeVersion.onOrAfter(MIN_NODE_VERSION);
    }
}
