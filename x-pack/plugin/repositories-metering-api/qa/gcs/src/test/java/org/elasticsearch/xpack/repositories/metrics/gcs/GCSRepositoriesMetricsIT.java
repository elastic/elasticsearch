/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.repositories.metrics.gcs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.repositories.metrics.AbstractRepositoriesMetricsAPIRestTestCase;

import java.util.List;

public class GCSRepositoriesMetricsIT extends AbstractRepositoriesMetricsAPIRestTestCase {

    @Override
    protected String repositoryType() {
        return "gcs";
    }

    @Override
    protected String repositoryLocation() {
        return getProperty("test.gcs.bucket") + "/" + getProperty("test.gcs.base_path") + "/";
    }

    @Override
    protected Settings repositorySettings() {
        final String bucket = getProperty("test.gcs.bucket");
        final String basePath = getProperty("test.gcs.base_path");

        return Settings.builder().put("client", "repositories_metering").put("bucket", bucket).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        return Settings.builder().put(repositorySettings()).put("gcs.client.repositories_metering.application_name", "updated").build();
    }

    @Override
    protected List<String> readCounterKeys() {
        return List.of("GET", "LIST");
    }

    @Override
    protected List<String> writeCounterKeys() {
        return List.of("POST");
    }
}
