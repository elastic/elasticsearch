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
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;
import org.elasticsearch.xpack.core.template.JsonIngestPipelineConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_INDEX_PATTERN;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_INDEX_PREFIX;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.ROOT_RESOURCE_PATH;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.TEMPLATE_VERSION_VARIABLE;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

/**
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class AnalyticsTemplateRegistry extends IndexTemplateRegistry {

    // This number must be incremented when we make changes to built-in templates.
    static final int REGISTRY_VERSION = 3;

    // Index template components configuration
    static final String EVENT_DATA_STREAM_SETTINGS_COMPONENT_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "settings";
    static final String EVENT_DATA_STREAM_MAPPINGS_COMPONENT_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "mappings";

    static final String EVENT_DATA_STREAM_INGEST_PIPELINE_NAME = EVENT_DATA_STREAM_INDEX_PREFIX + "final_pipeline";

    static final Map<String, ComponentTemplate> COMPONENT_TEMPLATES;

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
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes())) {
                componentTemplates.put(config.getTemplateName(), ComponentTemplate.parse(parser));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        COMPONENT_TEMPLATES = Map.copyOf(componentTemplates);
    }

    @Override
    protected List<IngestPipelineConfig> getIngestPipelines() {
        return List.of(
            new JsonIngestPipelineConfig(
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

    static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATES = parseComposableTemplates(
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
        NamedXContentRegistry xContentRegistry,
        ProjectResolver projectResolver
    ) {
        super(Settings.EMPTY, clusterService, threadPool, client, xContentRegistry, projectResolver);
    }

    @Override
    protected String getOrigin() {
        return ENT_SEARCH_ORIGIN;
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
