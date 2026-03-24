/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.compress;

import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;

/**
 * Shared compression libraries plugin for ESQL external data sources.
 *
 * <p>This plugin acts as a classloader anchor — it bundles shared compression libraries
 * (aircompressor, zstd-jni) so that other plugins can access them through the
 * {@code extendedPlugins} mechanism without each bundling their own copy.
 *
 * <p>Plugins that need compression libraries (format readers like ORC/Parquet/Iceberg
 * for internal column compression, and codec plugins like Snappy/Zstd for file-level
 * decompression) declare {@code extendedPlugins = ['x-pack-esql', 'esql-datasource-compression-libs']}
 * to inherit these libraries through parent-first classloader delegation.
 */
public class CompressionLibsPlugin extends Plugin implements DataSourcePlugin, ExtensiblePlugin {
    // Classloader anchor only — no codecs registered, no functionality.
    // The value is in the libraries this plugin bundles on its classloader.
}
