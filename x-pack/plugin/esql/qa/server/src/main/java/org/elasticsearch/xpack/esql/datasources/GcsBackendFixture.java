/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.xpack.esql.datasources.GcsFixtureUtils.DataSourcesGcsHttpFixture;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.GcsFixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.GcsFixtureUtils.TOKEN;

/**
 * {@link BackendFixture} adapter for {@link DataSourcesGcsHttpFixture}. Encapsulates the
 * {@code gcs} data-source registration shape (endpoint + service-account JSON + project + token URI)
 * and the {@code gs://<bucket>/<key>} resource URI form.
 *
 * <p>The constructor force-initializes the fixture's service-account JSON so callers can pull it
 * out via {@link #dataSourceSettings()} without remembering to call
 * {@link DataSourcesGcsHttpFixture#ensureServiceAccountInitialized()} first.
 */
public final class GcsBackendFixture implements BackendFixture {

    private final DataSourcesGcsHttpFixture fixture;

    public GcsBackendFixture(DataSourcesGcsHttpFixture fixture) {
        this.fixture = fixture;
        // Idempotent; safe even when the fixture was already initialized (e.g. by another test method
        // in the same parameterized class).
        fixture.ensureServiceAccountInitialized();
    }

    @Override
    public String name() {
        return "gcs";
    }

    @Override
    public String dataSourceType() {
        return "gcs";
    }

    @Override
    public Map<String, Object> dataSourceSettings() {
        // LinkedHashMap to keep insertion order stable in the JSON payload (eases debugging on failure).
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("endpoint", fixture.getAddress());
        settings.put("credentials", fixture.gcsServiceAccountJson());
        settings.put("project_id", "test");
        settings.put("token_uri", fixture.getAddress() + "/" + TOKEN);
        return settings;
    }

    @Override
    public String resourceUri(String blobKey) {
        return "gs://" + BUCKET + "/" + blobKey;
    }

    @Override
    public void uploadBlob(String key, byte[] content) {
        fixture.getHandler().putBlob(key, new BytesArray(content));
    }
}
