/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Data source plugin providing Google Cloud Storage support for ESQL.
 * Supports the gs:// URI scheme.
 * <p>
 * Usage in ESQL: register a dataset over a {@code gs://} resource, optionally with the
 * {@code credentials} (service account JSON) and {@code project_id} settings, then query it with
 * {@code FROM <dataset>}.
 * <p>
 * GCS is not in the released ship set yet (S3 is the released cloud provider), so registration is
 * gated on {@link #ESQL_EXTERNAL_GCS_FEATURE_FLAG}: available in snapshot/development builds, disabled
 * in release. When the gate is off the {@code gs} scheme is not registered, so a {@code gs://} source
 * resolves to the generic "Unsupported storage scheme" rejection.
 */
public class GcsDataSourcePlugin extends Plugin implements DataSourcePlugin {

    /**
     * Gates the GCS storage provider. Snapshot-on, release-off; override in release with
     * {@code -Des.esql_external_gcs_feature_flag_enabled=true}.
     */
    public static final FeatureFlag ESQL_EXTERNAL_GCS_FEATURE_FLAG = new FeatureFlag("esql_external_gcs");

    private static boolean enabled() {
        return ESQL_EXTERNAL_GCS_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Set<String> supportedSchemes() {
        if (enabled() == false) {
            return Set.of();
        }
        return Set.of("gs");
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings, ExecutorService executor) {
        if (enabled() == false) {
            return Map.of();
        }
        StorageProviderFactory gcsFactory = StorageProviderFactory.of(
            () -> new GcsStorageProvider((GcsConfiguration) null),
            GcsConfiguration::fromQueryConfig,
            GcsStorageProvider::new
        );
        return Map.of("gs", gcsFactory);
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        DataSourceValidator v = new FileDataSourceValidator("gcs", GcsConfiguration::fromMap, supportedSchemes());
        return Map.of(v.type(), v);
    }
}
