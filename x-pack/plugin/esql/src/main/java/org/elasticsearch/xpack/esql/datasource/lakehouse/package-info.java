/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Data lake infrastructure for reading from file-based and table-based external storage.
 *
 * <p>SPI contracts (interfaces and abstract classes) live in the
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi spi} sub-package.
 * This package contains internal helpers: registries, async operators, and glob expansion.
 * Both are used by {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.LakehouseDataSource}
 * to implement data lake data sources.
 *
 * <h2>Storage Access</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StoragePath} —
 *       URI-like path for addressing objects in storage systems (scheme://host[:port]/path)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageProvider} —
 *       Access files in a storage system (S3, GCS, HDFS, local filesystem)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageObject} —
 *       Read handle for a single object with sync and async capabilities</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageEntry} —
 *       Metadata about an object from directory listing (path, length, lastModified)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageIterator} —
 *       Closeable iterator over storage entries for lazy directory listing</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageProviderFactory} —
 *       Factory for creating StorageProvider instances</li>
 * </ul>
 *
 * <h2>Format Reading</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatReader} —
 *       Read file formats (Parquet, ORC, CSV, Avro) with schema inference and data reading</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatReaderFactory} —
 *       Factory for creating FormatReader instances</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.CloseableIterator} —
 *       Generic closeable iterator used for streaming data pages (in parent package)</li>
 * </ul>
 *
 * <h2>Metadata</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceMetadata} —
 *       Schema, location, and optional statistics for a data source</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SimpleSourceMetadata} —
 *       Immutable implementation of SourceMetadata with builder</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceStatistics} —
 *       Row count, size, and per-column statistics for query planning</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FileSet} —
 *       Resolved set of files from glob/path with sentinel states (UNRESOLVED, EMPTY)</li>
 * </ul>
 *
 * <h2>Filter Pushdown</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FilterPushdownSupport} —
 *       Push filter expressions to the data source for efficient evaluation</li>
 * </ul>
 *
 * <h2>Table Catalog</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.TableCatalog} —
 *       Integration with table formats (Iceberg, Delta Lake, Hudi)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.TableCatalogFactory} —
 *       Factory for creating TableCatalog instances</li>
 * </ul>
 *
 * <h2>Operator Creation</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceOperatorFactoryProvider} —
 *       Extension point for custom source operator creation</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceOperatorContext} —
 *       Context record with all information needed for operator factory creation</li>
 * </ul>
 *
 * <h2>Plugin Discovery</h2>
 *
 * <p>Lakehouse-specific plugin interfaces, discovered by
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.LakehouseRegistry} at startup:
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StoragePlugin} —
 *       Extension point for storage providers, keyed by URI scheme</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatPlugin} —
 *       Extension point for format readers, keyed by format name</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.CatalogPlugin} —
 *       Extension point for table catalogs, keyed by catalog type</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.LakehouseRegistry} —
 *       Discovers lakehouse plugins, populates registries</li>
 * </ul>
 *
 * <h2>Registries</h2>
 *
 * <p>Both registries use lazy factory-based creation — heavy dependencies (S3 client,
 * Parquet reader) are only loaded when a query first targets that backend.
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.StorageProviderRegistry} —
 *       Scheme-keyed lazy lookup: stores factories, creates providers on first access</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.FormatReaderRegistry} —
 *       Name-keyed lazy lookup: stores factories, creates readers on first access</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.StorageManager} —
 *       Facade over StorageProviderRegistry for creating StorageObject from paths</li>
 * </ul>
 *
 * <h2>Async Operators</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.AsyncExternalSourceOperatorFactory} —
 *       Dual-mode factory: auto-selects sync wrapper vs native async</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.AsyncExternalSourceOperator} —
 *       SourceOperator that polls from AsyncExternalSourceBuffer</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.AsyncExternalSourceBuffer} —
 *       Thread-safe buffer with backpressure for cross-thread page transfer</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.ExternalSourceOperatorFactory} —
 *       Synchronous factory for simple format readers</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.ExternalSourceDrainUtils} —
 *       Backpressure-aware page draining using PlainActionFuture blocking</li>
 * </ul>
 *
 * <h2>Relationship to LakehouseDataSource</h2>
 *
 * <p>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.LakehouseDataSource} is a concrete,
 * registry-driven data source that looks up
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageProvider} and
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatReader} from the
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.LakehouseRegistry} at runtime.
 * No subclassing needed — all variation is handled by registered
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StoragePlugin},
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatPlugin}, and
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.CatalogPlugin} implementations.
 *
 * <h2>Related Packages</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.connector connector} —
 *       Connection-oriented data sources (Flight, JDBC). The connector package
 *       reuses async infrastructure from this package
 *       ({@link org.elasticsearch.xpack.esql.datasource.lakehouse.AsyncExternalSourceBuffer},
 *       {@link org.elasticsearch.xpack.esql.datasource.lakehouse.ExternalSourceDrainUtils}).</li>
 * </ul>
 */
package org.elasticsearch.xpack.esql.datasource.lakehouse;
