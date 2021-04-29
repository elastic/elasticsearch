/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.repositories.metering.s3;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.repositories.metering.AbstractRepositoriesMeteringAPIRestTestCase;

import java.util.List;
import java.util.Map;

public class S3RepositoriesMeteringIT extends AbstractRepositoriesMeteringAPIRestTestCase {

    @Override
    protected String repositoryType() {
        return "s3";
    }

    @Override
    protected Map<String, String> repositoryLocation() {
        return Map.of("bucket", getProperty("test.s3.bucket"), "base_path", getProperty("test.s3.base_path"));
    }

    @Override
    protected Settings repositorySettings() {
        final String bucket = getProperty("test.s3.bucket");
        final String basePath = getProperty("test.s3.base_path");

        return Settings.builder().put("client", "repositories_metering").put("bucket", bucket).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        Settings settings = repositorySettings();
        return Settings.builder().put(settings).put("s3.client.max_retries", 4).build();
    }

    @Override
    protected List<String> readCounterKeys() {
        return List.of("GetObject", "ListObjects");
    }

    @Override
    protected List<String> writeCounterKeys() {
        return List.of("PutObject");
    }
}
