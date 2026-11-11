/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.io.IOException;
import java.util.Map;

/**
 * Backend-specific knobs every {@code FROM <dataset>} REST IT needs to drive the production
 * {@code PUT /_query/data_source/...} + {@code PUT /_query/dataset/...} flow against a particular
 * cloud-storage fixture. Implementations are thin wrappers over the in-process {@code *HttpFixture}
 * for S3, GCS, or Azure that paper over the per-backend differences (auth shape, URI scheme,
 * how to push a blob into the fixture) so a single parameterized test body can run across all
 * three backends.
 *
 * <p>Instances are created lazily inside test methods — after the underlying {@code @ClassRule}
 * fixtures have started — so constructors are free to capture live fixture state
 * (addresses, generated credentials, etc.).
 *
 * <p><strong>Iceberg note:</strong> Iceberg deliberately is <em>not</em> a {@link BackendFixture}.
 * {@code IcebergDataSourcePlugin} registers only a {@code TableCatalogFactory} (no
 * {@code DataSourceValidator}), so {@code PUT /_query/data_source/<name>} with
 * {@code "type": "iceberg"} is unsupported in production today. Iceberg is reached exclusively via
 * the {@code EXTERNAL "s3://..." WITH { "format": "iceberg" }} command shape and so does not fit
 * the dataset-registration contract this interface encodes. Adding an iceberg {@code BackendFixture}
 * requires first adding a core {@code DataSourceValidator} for {@code "iceberg"}.
 */
public interface BackendFixture {

    /** Short label used in parameterized test display names ({@code "s3"}, {@code "gcs"}, {@code "azure"}). */
    String name();

    /**
     * {@code DataSourceValidator} type string registered by the corresponding production plugin
     * ({@code "s3"} / {@code "gcs"} / {@code "azure"}).
     */
    String dataSourceType();

    /**
     * Settings to pass at {@code PUT /_query/data_source/<name>} so the registered data source can
     * authenticate against this backend's in-process fixture. Each implementation owns the
     * backend-specific keys (e.g. {@code access_key} vs {@code credentials} vs {@code account}).
     */
    Map<String, Object> dataSourceSettings();

    /**
     * Build the {@code resource} URI to pass at {@code PUT /_query/dataset/<name>} for the supplied
     * blob key. The URI scheme is backend-specific ({@code s3://}, {@code gs://}, {@code wasbs://}).
     */
    String resourceUri(String blobKey);

    /**
     * Push the supplied bytes into the underlying fixture at the supplied key, so a subsequent
     * {@code FROM <dataset>} query can read them back. Each implementation chooses the cheapest
     * direct-put path its fixture exposes (handler poking, fixture address upload, etc.).
     */
    void uploadBlob(String key, byte[] content) throws IOException;
}
