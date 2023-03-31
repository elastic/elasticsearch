/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.connector.ConnectorConstants.ACCESS_CONTROL_INDEX_NAME_PATTERN;
import static org.elasticsearch.xpack.application.connector.ConnectorConstants.ACCESS_CONTROL_TEMPLATE_NAME;
import static org.elasticsearch.xpack.application.connector.ConnectorConstants.CONNECTOR_INDEX_NAME_PATTERN;
import static org.elasticsearch.xpack.application.connector.ConnectorConstants.CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN;
import static org.elasticsearch.xpack.application.connector.ConnectorConstants.CONNECTOR_TEMPLATE_NAME;
import static org.elasticsearch.xpack.application.connector.ConnectorConstants.CONNECTOR_SYNC_JOBS_TEMPLATE_NAME;
import static org.elasticsearch.xpack.application.connector.ConnectorConstants.ENT_SEARCH_GENERIC_PIPELINE_FILE;
import static org.elasticsearch.xpack.application.connector.ConnectorConstants.ENT_SEARCH_GENERIC_PIPELINE_NAME;
import static org.elasticsearch.xpack.application.connector.ConnectorConstants.ROOT_RESOURCE_PATH;
import static org.elasticsearch.xpack.application.connector.ConnectorConstants.ROOT_TEMPLATE_RESOURCE_PATH;
import static org.elasticsearch.xpack.application.connector.ConnectorConstants.TEMPLATE_VERSION_VARIABLE;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class ConnectorTemplateRegistry extends IndexTemplateRegistry {

    static final Version MIN_NODE_VERSION = Version.V_8_10_0;

    // This number must be incremented when we make changes to built-in templates.
    static final int REGISTRY_VERSION = 1;

    static final Map<String, ComponentTemplate> COMPONENT_TEMPLATES;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                CONNECTOR_TEMPLATE_NAME + "-mappings",
                ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_TEMPLATE_NAME + "-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                CONNECTOR_TEMPLATE_NAME + "-settings",
                ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_TEMPLATE_NAME + "-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + "-mappings",
                ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + "-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + "-settings",
                ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_TEMPLATE_NAME + "-settings.json",
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

    @Override
    protected List<IngestPipelineConfig> getIngestPipelines() {
        return List.of(
            new IngestPipelineConfig(
                ENT_SEARCH_GENERIC_PIPELINE_NAME,
                ROOT_RESOURCE_PATH + ENT_SEARCH_GENERIC_PIPELINE_FILE + ".json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            )
        );
    }

    static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATES = parseComposableTemplates(
        new IndexTemplateConfig(
            CONNECTOR_TEMPLATE_NAME,
            ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_TEMPLATE_NAME + ".json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            Map.of("connector.index_pattern", CONNECTOR_INDEX_NAME_PATTERN)
        ),
        new IndexTemplateConfig(
            CONNECTOR_SYNC_JOBS_TEMPLATE_NAME,
            ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + ".json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            Map.of("connector-sync-jobs.index_pattern", CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN)
        ),
        new IndexTemplateConfig(
            ACCESS_CONTROL_TEMPLATE_NAME,
            ROOT_TEMPLATE_RESOURCE_PATH + ACCESS_CONTROL_TEMPLATE_NAME + ".json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            Map.of("access-control.index_pattern", ACCESS_CONTROL_INDEX_NAME_PATTERN)
        )
    );

    public ConnectorTemplateRegistry(
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

    @Override
    protected boolean isClusterReady(ClusterChangedEvent event) {
        // Ensure templates are installed only once all nodes are updated to 8.8.0.
        Version minNodeVersion = event.state().nodes().getMinNodeVersion();
        return minNodeVersion.onOrAfter(MIN_NODE_VERSION);
    }
}
