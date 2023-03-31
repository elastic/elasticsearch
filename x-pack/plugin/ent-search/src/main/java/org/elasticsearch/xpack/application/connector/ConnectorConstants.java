/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

public class ConnectorConstants {
    // Connector indices constants
    public static final String CONNECTOR_INDEX_NAME_PATTERN = ".elastic-connectors-*";
    public static final String CONNECTOR_TEMPLATE_NAME = "elastic-connectors";

    public static final String CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN = ".elastic-connectors-sync-jobs-*";
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
}
