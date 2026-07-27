/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;

/**
 * {@link BackendFixture} adapter for {@link DataSourcesS3HttpFixture}. Encapsulates the
 * {@code s3} data-source registration shape (endpoint + access/secret keys) and the
 * {@code s3://<bucket>/<key>} resource URI form so a parameterized REST IT can drive the S3
 * code path without inlining any S3-specific plumbing.
 */
public final class S3BackendFixture implements BackendFixture {

    private final DataSourcesS3HttpFixture fixture;

    public S3BackendFixture(DataSourcesS3HttpFixture fixture) {
        this.fixture = fixture;
    }

    @Override
    public String name() {
        return "s3";
    }

    @Override
    public String dataSourceType() {
        return "s3";
    }

    @Override
    public Map<String, Object> dataSourceSettings() {
        return Map.of("endpoint", fixture.getAddress(), "access_key", ACCESS_KEY, "secret_key", SECRET_KEY);
    }

    @Override
    public String resourceUri(String blobKey) {
        return "s3://" + BUCKET + "/" + blobKey;
    }

    @Override
    public void uploadBlob(String key, byte[] content) {
        S3FixtureUtils.addBlobToFixture(fixture.getHandler(), key, content);
    }
}
