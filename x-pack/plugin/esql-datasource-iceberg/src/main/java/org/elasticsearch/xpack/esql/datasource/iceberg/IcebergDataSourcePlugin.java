/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.TableCatalogFactory;

import java.util.Map;

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
 */
public class IcebergDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Map<String, TableCatalogFactory> tableCatalogs(Settings settings) {
        return Map.of("iceberg", s -> new IcebergTableCatalog());
    }
}
