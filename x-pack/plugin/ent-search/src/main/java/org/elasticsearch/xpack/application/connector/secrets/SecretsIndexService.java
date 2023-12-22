/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.*;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

/**
 * A service that manages persistent Search Secrets.
 */
public class SecretsIndexService {

    public static final String CONNECTOR_SECRETS_INDEX_NAME = ".connector-secrets";

    private static final int CURRENT_INDEX_VERSION = 1;
    private static final String MAPPING_VERSION_VARIABLE = "search-secrets.version";
    private static final String MAPPING_MANAGED_VERSION_VARIABLE = "search-secrets.managed.index.version";

    /**
     * Returns the {@link SystemIndexDescriptor} for the Search Secrets system index.
     *
     * @return The {@link SystemIndexDescriptor} for the Search Secrets system index.
     */
    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();

        String templateSource = TemplateUtils.loadTemplate(
            "/connector-secrets.json",
            Version.CURRENT.toString(),
            MAPPING_VERSION_VARIABLE,
            Map.of(MAPPING_MANAGED_VERSION_VARIABLE, Integer.toString(CURRENT_INDEX_VERSION))
        );
        request.source(templateSource, XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setIndexPattern(CONNECTOR_SECRETS_INDEX_NAME + "*")
            .setPrimaryIndex(CONNECTOR_SECRETS_INDEX_NAME + "-" + CURRENT_INDEX_VERSION)
            .setDescription("Secret values managed by Connectors")
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setAliasName(CONNECTOR_SECRETS_INDEX_NAME)
            .setVersionMetaKey("version")
            .setOrigin(ENT_SEARCH_ORIGIN)
            .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
            .build();
    }
}
