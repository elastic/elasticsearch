/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.repositories.metering.azure;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.repositories.metering.AbstractRepositoriesMeteringAPIRestTestCase;

import java.util.List;
import java.util.Map;

public class AzureRepositoriesMeteringIT extends AbstractRepositoriesMeteringAPIRestTestCase {

    @Override
    protected String repositoryType() {
        return "azure";
    }

    @Override
    protected Map<String, String> repositoryLocation() {
        return Map.of("container", getProperty("test.azure.container"), "base_path", getProperty("test.azure.base_path"));
    }

    @Override
    protected Settings repositorySettings() {
        final String container = getProperty("test.azure.container");

        final String basePath = getProperty("test.azure.base_path");

        return Settings.builder().put("client", "repositories_metering").put("container", container).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        return Settings.builder().put(repositorySettings()).put("azure.client.repositories_metering.max_retries", 5).build();
    }

    @Override
    protected List<String> readCounterKeys() {
        return List.of("GetBlob", "GetBlobProperties", "ListBlobs");
    }

    @Override
    protected List<String> writeCounterKeys() {
        return List.of("PutBlob");
    }
}
