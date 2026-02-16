/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Data lake SPI types for reading from file-based and table-based external storage.
 *
 * <p>This package provides production-ready abstractions for storage access,
 * format reading, table catalog integration, and filter pushdown. These types
 * are used by {@link org.elasticsearch.xpack.esql.datasource.lakehouse.LakehouseDataSource}
 * to implement data lake data sources.
 *
 * <h2>Storage Access</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.StoragePath} —
 *       URI-like path for addressing objects in storage systems (scheme://host[:port]/path)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.StorageProvider} —
 *       Access files in a storage system (S3, GCS, HDFS, local filesystem)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.StorageObject} —
 *       Read handle for a single object with sync and async capabilities</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.StorageEntry} —
 *       Metadata about an object from directory listing (path, length, lastModified)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.StorageIterator} —
 *       Closeable iterator over storage entries for lazy directory listing</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.StorageProviderFactory} —
 *       Factory for creating StorageProvider instances</li>
 * </ul>
 *
 * <h2>Format Reading</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.FormatReader} —
 *       Read file formats (Parquet, ORC, CSV, Avro) with schema inference and data reading</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.FormatReaderFactory} —
 *       Factory for creating FormatReader instances</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.CloseableIterator} —
 *       Generic closeable iterator used for streaming data pages</li>
 * </ul>
 *
 * <h2>Metadata</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.SourceMetadata} —
 *       Schema, location, and optional statistics for a data source</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.SimpleSourceMetadata} —
 *       Immutable implementation of SourceMetadata with builder</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.SourceStatistics} —
 *       Row count, size, and per-column statistics for query planning</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.FileSet} —
 *       Resolved set of files from glob/path with sentinel states (UNRESOLVED, EMPTY)</li>
 * </ul>
 *
 * <h2>Filter Pushdown</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.FilterPushdownSupport} —
 *       Push filter expressions to the data source for efficient evaluation</li>
 * </ul>
 *
 * <h2>Table Catalog</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.TableCatalog} —
 *       Integration with table formats (Iceberg, Delta Lake, Hudi)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.TableCatalogFactory} —
 *       Factory for creating TableCatalog instances</li>
 * </ul>
 *
 * <h2>Operator Creation</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.SourceOperatorFactoryProvider} —
 *       Extension point for custom source operator creation</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.SourceOperatorContext} —
 *       Context record with all information needed for operator factory creation</li>
 * </ul>
 *
 * <h2>Relationship to LakehouseDataSource</h2>
 *
 * <p>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.LakehouseDataSource} uses these types
 * to implement its storage/format separation architecture. Subclasses provide concrete
 * implementations of {@link org.elasticsearch.xpack.esql.datasource.lakehouse.StorageProvider}
 * and {@link org.elasticsearch.xpack.esql.datasource.lakehouse.FormatReader}, optionally with
 * a {@link org.elasticsearch.xpack.esql.datasource.lakehouse.TableCatalog} for table-based sources.
 */
package org.elasticsearch.xpack.esql.datasource.lakehouse;
