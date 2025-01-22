/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;
import org.elasticsearch.xpack.core.template.JsonIngestPipelineConfig;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.CLOUD_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.CONNECTORS_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.KIBANA_ORIGIN;

public class ConnectorTemplateRegistry extends IndexTemplateRegistry {

    // This number must be incremented when we make changes to built-in templates.
    static final int REGISTRY_VERSION = 3;

    // Connector indices constants
    public static final String ACCESS_CONTROL_INDEX_PREFIX = ".search-acl-filter-";
    public static final String ACCESS_CONTROL_INDEX_NAME_PATTERN = ".search-acl-filter-*";
    public static final String ACCESS_CONTROL_TEMPLATE_NAME = "search-acl-filter";

    public static final String MANAGED_CONNECTOR_INDEX_PREFIX = "content-";

    // Pipeline constants
    public static final String SEARCH_DEFAULT_PIPELINE_NAME = "search-default-ingestion";
    public static final String SEARCH_DEFAULT_PIPELINE_FILE = "search_default_pipeline";

    // Resource config
    public static final String ROOT_RESOURCE_PATH = "/entsearch/";
    public static final String ROOT_TEMPLATE_RESOURCE_PATH = ROOT_RESOURCE_PATH + "connector/";

    // Variable used to replace template version in index templates
    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.application.connector.template.version";

    // Sources allowed to access system indices using X-elastic-product-origin header
    public static final List<String> CONNECTORS_ALLOWED_PRODUCT_ORIGINS = List.of(KIBANA_ORIGIN, CONNECTORS_ORIGIN, CLOUD_ORIGIN);

    @Override
    protected List<IngestPipelineConfig> getIngestPipelines() {
        return List.of(
            new JsonIngestPipelineConfig(
                SEARCH_DEFAULT_PIPELINE_NAME,
                ROOT_RESOURCE_PATH + SEARCH_DEFAULT_PIPELINE_FILE + ".json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            )
        );
    }

    static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATES = parseComposableTemplates(
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
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return COMPOSABLE_INDEX_TEMPLATES;
    }

    @Override
    protected boolean requiresMasterNode() {
        // Necessary to prevent conflicts in some mixed-cluster environments with pre-7.7 nodes
        return true;
    }
}
