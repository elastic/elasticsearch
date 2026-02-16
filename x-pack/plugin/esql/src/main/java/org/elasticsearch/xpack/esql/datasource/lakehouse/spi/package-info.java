/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Lakehouse SPI contracts for data lake and file-based external storage.
 *
 * <p>This package contains all interfaces and abstract classes for lakehouse data
 * source implementations. Internal helpers (registries, operators, glob expansion)
 * live in the {@link org.elasticsearch.xpack.esql.datasource.lakehouse parent package}.
 *
 * <h2>Data Source and Plan</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.LakehouseDataSource} —
 *       concrete, registry-driven data source (looks up storage/format from LakehouseRegistry)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.LakehousePlan} —
 *       concrete plan node with expression, formatName, nativeFilter, limit</li>
 * </ul>
 *
 * <h2>Storage Access</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StoragePath} —
 *       URI-like path (scheme://host[:port]/path)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageProvider} —
 *       access objects in a storage system (S3, GCS, HDFS, local)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageObject} —
 *       read handle for a single object</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageEntry} —
 *       metadata from directory listing (path, length, lastModified)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageIterator} —
 *       closeable iterator over storage entries</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageProviderFactory} —
 *       factory for creating StorageProvider instances</li>
 * </ul>
 *
 * <h2>Format Reading</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatReader} —
 *       read file formats (Parquet, ORC, CSV, Avro)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatReaderFactory} —
 *       factory for creating FormatReader instances</li>
 * </ul>
 *
 * <h2>Metadata</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceMetadata} —
 *       schema, location, and optional statistics</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SimpleSourceMetadata} —
 *       immutable SourceMetadata implementation with builder</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceStatistics} —
 *       row count, size, per-column statistics</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FileSet} —
 *       resolved set of files from glob/path</li>
 * </ul>
 *
 * <h2>Filter Pushdown</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FilterPushdownSupport} —
 *       push filter expressions to the data source</li>
 * </ul>
 *
 * <h2>Table Catalog</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.TableCatalog} —
 *       integration with Iceberg, Delta Lake, Hudi</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.TableCatalogFactory} —
 *       factory for creating TableCatalog instances</li>
 * </ul>
 *
 * <h2>Operator Creation</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceOperatorFactoryProvider} —
 *       extension point for custom source operator creation</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceOperatorContext} —
 *       context record for operator factory creation</li>
 * </ul>
 *
 * <h2>Plugin Discovery</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StoragePlugin} —
 *       extension point for storage providers, keyed by URI scheme</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatPlugin} —
 *       extension point for format readers, keyed by format name</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.CatalogPlugin} —
 *       extension point for table catalogs, keyed by catalog type</li>
 * </ul>
 */
package org.elasticsearch.xpack.esql.datasource.lakehouse.spi;
