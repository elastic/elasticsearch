/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.TableCatalogFactory;

import java.util.Map;
import java.util.Set;

/**
 * Data source plugin that provides Iceberg table catalog support for ESQL external data sources.
 *
 * <p>This plugin provides:
 * <ul>
 *   <li>Iceberg table catalog for reading Iceberg tables from S3</li>
 *   <li>Schema discovery from Iceberg metadata</li>
 *   <li>Predicate pushdown for efficient filtering</li>
 *   <li>Vectorized reading using Arrow format</li>
 * </ul>
 *
 * <p>The Iceberg implementation uses:
 * <ul>
 *   <li>Iceberg's StaticTableOperations for metadata access</li>
 *   <li>S3FileIO for S3 storage access</li>
 *   <li>ArrowReader for efficient vectorized columnar data reading</li>
 * </ul>
 *
 * <p>Heavy dependencies (Iceberg, Arrow, Parquet, AWS SDK) are isolated in this module
 * to avoid jar hell issues in the core ESQL plugin.
 *
 * <p>Iceberg is not in the released ship set yet, so catalog registration is gated on
 * {@link #ESQL_EXTERNAL_ICEBERG_FEATURE_FLAG}: the catalog is available in snapshot/development
 * builds and disabled in release. When the gate is off neither {@code supportedCatalogs()} nor
 * {@code tableCatalogs()} return any entry, so no extensionless S3 object path is claimed by the
 * Iceberg catalog and the resolver applies the standard file-format check instead.
 */
public class IcebergDataSourcePlugin extends Plugin implements DataSourcePlugin {

    /**
     * Gates the Iceberg table catalog. Snapshot-on, release-off; override in release with
     * {@code -Des.esql_external_iceberg_feature_flag_enabled=true}.
     */
    public static final FeatureFlag ESQL_EXTERNAL_ICEBERG_FEATURE_FLAG = new FeatureFlag("esql_external_iceberg");

    private static boolean enabled() {
        return ESQL_EXTERNAL_ICEBERG_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Set<String> supportedCatalogs() {
        if (enabled() == false) {
            return Set.of();
        }
        return Set.of("iceberg");
    }

    @Override
    public Map<String, TableCatalogFactory> tableCatalogs(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        return Map.of("iceberg", s -> new IcebergTableCatalog());
    }
}
