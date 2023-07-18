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

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class ConnectorTemplateRegistry extends IndexTemplateRegistry {

    static final Version MIN_NODE_VERSION = Version.V_8_10_0;

    // This number must be incremented when we make changes to built-in templates.
    static final int REGISTRY_VERSION = 1;

    // Connector indices constants

    public static final String CONNECTOR_INDEX_NAME_PATTERN = ".elastic-connectors-v" + REGISTRY_VERSION;
    public static final String CONNECTOR_TEMPLATE_NAME = "elastic-connectors";

    public static final String CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN = ".elastic-connectors-sync-jobs-v" + REGISTRY_VERSION;
    public static final String CONNECTOR_SYNC_JOBS_TEMPLATE_NAME = "elastic-connectors-sync-jobs";

    public static final String ACCESS_CONTROL_INDEX_NAME_PATTERN = ".search-acl-filter-*";
    public static final String ACCESS_CONTROL_TEMPLATE_NAME = "search-acl-filter";

    // Pipeline constants

    public static final String ENT_SEARCH_GENERIC_PIPELINE_NAME = "ent-search-generic-ingestion";
    public static final String ENT_SEARCH_GENERIC_PIPELINE_FILE = "generic_ingestion_pipeline";

    // Resource config
    public static final String ROOT_RESOURCE_PATH = "/org/elasticsearch/xpack/entsearch/";
    public static final String ROOT_TEMPLATE_RESOURCE_PATH = ROOT_RESOURCE_PATH + "connector/";

    // Variable used to replace template version in index templates
    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.application.connector.template.version";

    private static final String MAPPINGS_SUFFIX = "-mappings";

    private static final String SETTINGS_SUFFIX = "-settings";

    private static final String JSON_EXTENSION = ".json";

    static final Map<String, ComponentTemplate> COMPONENT_TEMPLATES;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                CONNECTOR_TEMPLATE_NAME + MAPPINGS_SUFFIX,
                ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_TEMPLATE_NAME + MAPPINGS_SUFFIX + JSON_EXTENSION,
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                CONNECTOR_TEMPLATE_NAME + SETTINGS_SUFFIX,
                ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_TEMPLATE_NAME + SETTINGS_SUFFIX + JSON_EXTENSION,
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + MAPPINGS_SUFFIX,
                ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + MAPPINGS_SUFFIX + JSON_EXTENSION,
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + SETTINGS_SUFFIX,
                ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_TEMPLATE_NAME + SETTINGS_SUFFIX + JSON_EXTENSION,
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
            Map.of("connectors.index_pattern", CONNECTOR_INDEX_NAME_PATTERN)
        ),
        new IndexTemplateConfig(
            CONNECTOR_SYNC_JOBS_TEMPLATE_NAME,
            ROOT_TEMPLATE_RESOURCE_PATH + CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + ".json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE,
            Map.of("connectors-sync-jobs.index_pattern", CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN)
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
        // Necessary to prevent conflicts in some mixed-cluster environments with pre-7.7 nodes
        return true;
    }

    @Override
    protected boolean isClusterReady(ClusterChangedEvent event) {
        // Ensure templates are installed only once all nodes are updated to 8.10.0.
        Version minNodeVersion = event.state().nodes().getMinNodeVersion();
        return minNodeVersion.onOrAfter(MIN_NODE_VERSION);
    }
}
