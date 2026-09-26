/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.DataSourcesAzureHttpFixture;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.ACCOUNT;
import static org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.CONTAINER;
import static org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.KEY;

/**
 * {@link BackendFixture} adapter for {@link DataSourcesAzureHttpFixture}. Encapsulates the
 * {@code azure} data-source registration shape (endpoint + account + shared key) and the
 * path-style {@code wasbs://<account>.blob.core.windows.net/<container>/<key>} resource URI form.
 */
public final class AzureBackendFixture implements BackendFixture {

    private final DataSourcesAzureHttpFixture fixture;

    public AzureBackendFixture(DataSourcesAzureHttpFixture fixture) {
        this.fixture = fixture;
    }

    @Override
    public String name() {
        return "azure";
    }

    @Override
    public String dataSourceType() {
        return "azure";
    }

    @Override
    public Map<String, Object> dataSourceSettings() {
        return Map.of("endpoint", fixture.getAddress(), "account", ACCOUNT, "key", KEY);
    }

    @Override
    public String resourceUri(String blobKey) {
        // Path-style Azure URI: wasbs://<account>.blob.core.windows.net/<container>/<key>.
        // The container slot is reused from S3FixtureUtils.BUCKET via AzureFixtureUtils.CONTAINER.
        return "wasbs://" + ACCOUNT + ".blob.core.windows.net/" + CONTAINER + "/" + blobKey;
    }

    @Override
    public void uploadBlob(String key, byte[] content) {
        AzureFixtureUtils.addBlobToFixture(fixture.getAddress(), key, content);
    }
}
