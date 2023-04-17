/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class AnalyticsTemplateRegistry extends IndexTemplateRegistry {

    // Data stream related constants.
    public static final String EVENT_DATA_STREAM_DATASET = "events";
    public static final String EVENT_DATA_STREAM_TYPE = "behavioral_analytics";
    public static final String EVENT_DATA_STREAM_INDEX_PREFIX = EVENT_DATA_STREAM_TYPE + "-" + EVENT_DATA_STREAM_DATASET + "-";
    public static final String EVENT_DATA_STREAM_INDEX_PATTERN = EVENT_DATA_STREAM_INDEX_PREFIX + "*";

    // Resource config.
    protected static final String ROOT_RESOURCE_PATH = "/org/elasticsearch/xpack/entsearch/analytics/";

    // This number must be incremented when we make changes to built-in templates.
    protected static final int REGISTRY_VERSION = 1;

    // The variable to be replaced with the template version number
    protected static final String TEMPLATE_VERSION_VARIABLE = "xpack.entsearch.analytics.template.version";

    // ILM Policies configuration
    protected static final String EVENT_DATA_STREAM_ILM_POLICY_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "default_policy";

    protected static final List<LifecyclePolicy> LIFECYCLE_POLICIES = Stream.of(
        new LifecyclePolicyConfig(EVENT_DATA_STREAM_ILM_POLICY_NAME, ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_ILM_POLICY_NAME + ".json")
    ).map(config -> config.load(LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY)).toList();

    // Index template components configuration
    protected static final String EVENT_DATA_STREAM_SETTINGS_COMPONENT_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "settings";
    protected static final String EVENT_DATA_STREAM_MAPPINGS_COMPONENT_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "mappings";

    protected static final Map<String, ComponentTemplate> COMPONENT_TEMPLATES;

    static {
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
                    ComponentTemplate.parse(JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes()))
                );
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        COMPONENT_TEMPLATES = Map.copyOf(componentTemplates);
    }

    // Composable index templates configuration.

    protected static final String EVENT_DATA_STREAM_TEMPLATE_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "default";
    protected static final String EVENT_DATA_STREAM_TEMPLATE_FILENAME = EVENT_DATA_STREAM_INDEX_PREFIX + "template";

    protected static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATES = parseComposableTemplates(
        new IndexTemplateConfig(
            EVENT_DATA_STREAM_TEMPLATE_NAME,
            ROOT_RESOURCE_PATH + EVENT_DATA_STREAM_TEMPLATE_FILENAME + ".json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            Map.of("event_data_stream.index_pattern", EVENT_DATA_STREAM_INDEX_PATTERN)
        )
    );

    public AnalyticsTemplateRegistry(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(Settings.EMPTY, clusterService, threadPool, client, xContentRegistry);
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
        return COMPOSABLE_INDEX_TEMPLATES;
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
}
