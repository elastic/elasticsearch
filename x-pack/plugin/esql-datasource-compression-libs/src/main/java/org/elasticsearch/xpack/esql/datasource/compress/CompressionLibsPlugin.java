/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.compress;

import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;

/**
 * Shared compression libraries plugin for ESQL external data sources.
 *
 * <p>This plugin acts as a classloader anchor — it bundles shared compression libraries
 * (aircompressor, zstd-jni, snappy-java) so that other plugins can access them through the
 * {@code extendedPlugins} mechanism without each bundling their own copy.
 *
 * <p>Plugins that need compression libraries (format readers like ORC/Parquet/Iceberg
 * for internal column compression, and codec plugins like Snappy/Zstd for file-level
 * decompression) declare {@code extendedPlugins = ['x-pack-esql', 'esql-datasource-compression-libs']}
 * to inherit these libraries through parent-first classloader delegation.
 *
 * <p>This module also hosts {@link PanamaZstd}, the shared Panama FFI binding to libzstd
 * used by the direct-buffer page decompression hot path. Format readers should prefer it
 * over zstd-jni for direct→direct decompression: it avoids JNI's
 * {@code GetPrimitiveArrayCritical} G1GC region pinning. Parquet uses it today; ORC and
 * Iceberg should adopt it on their direct paths.
 *
 * <p><b>Why this is a named Java module.</b> Hosting {@code PanamaZstd} requires access to
 * the {@code org.elasticsearch.nativeaccess} package, which {@code libs/native} qualifies
 * to a curated list of named modules so that the surrounding native-access surface (process
 * limits, mlock, exec sandbox, systemd hooks, raw memory mapping) stays invisible to plugins
 * in general. Declaring this plugin as a named module ({@code org.elasticsearch.xpack.esql.datasource.compress})
 * makes it addressable as a single new entry in that qualified-export list.
 *
 * <p>This class deliberately does <em>not</em> implement {@code DataSourcePlugin}. It registers
 * no schemes, formats, codecs, or factories — every default method on that interface would
 * return an empty map — so the SPI iteration over data source plugins would only ever see
 * a no-op contributor. Removing the interface keeps the named module's {@code requires} graph
 * minimal (no dependency on the unnamed {@code x-pack-esql} plugin module) and lets ES's
 * standard plugin loader pick it up via {@code plugin-descriptor.properties}.
 */
public class CompressionLibsPlugin extends Plugin implements ExtensiblePlugin {
    // Classloader anchor only — no codecs registered, no functionality.
    // The value is in the libraries this plugin bundles on its classloader.
}
