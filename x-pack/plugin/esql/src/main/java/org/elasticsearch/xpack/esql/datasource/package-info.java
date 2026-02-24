/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * DataSource SPI for external data sources in ES|QL.
 *
 * <h2>Invocation Flow</h2>
 *
 * <p>DataSource methods are called by specific ES|QL components at defined phases:
 *
 * <ol>
 *   <li><b>Resolution</b> - {@code PreAnalyzer} calls
 *       {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource#resolve} to discover schema
 *       and create the data source's logical plan node</li>
 *   <li><b>Logical Optimization</b> -
 *       {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource#applyOptimizationRules DataSource.applyOptimizationRules()}
 *       collects {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource#optimizationRules() rules}
 *       from all registered data sources and runs them as a separate pass after the main optimizer</li>
 *   <li><b>Physical Planning</b> - {@code Mapper} calls
 *       {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource#createPhysicalPlan}</li>
 *   <li><b>Work Distribution</b> - Physical planner calls
 *       {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource#planPartitions}</li>
 *   <li><b>Execution</b> - {@code LocalExecutionPlanner} calls
 *       {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource#createSourceOperator}</li>
 * </ol>
 *
 * <h2>Core Abstractions</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSource} - Main SPI interface (includes
 *       {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource#capabilities() capabilities()})</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlan} - Abstract base class for data source
 *       plan leaves (extends LeafPlan)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourcePartition} - Interface for units of work in distributed execution</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourceDescriptor} - Parsed data source reference</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourceCapabilities} - Execution mode flag
 *       (returned by {@code DataSource.capabilities()})</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.UnresolvedDataSourceRelation} - Unresolved plan
 *       leaf created by parser, resolved into DataSourcePlan</li>
 * </ul>
 *
 * <h2>Plugin Discovery</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlugin} - Extension point:
 *       {@code dataSources(Settings)} returns {@code Map<String, DataSourceFactory>}</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourceFactory} - Factory for creating
 *       DataSource instances from configuration</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.DataSourceRegistry} - Collects DataSourcePlugin
 *       factories, provides lookup by type</li>
 * </ul>
 *
 * <h2>Helpers</h2>
 *
 * <p>Convenience classes built on top of the core abstractions. Data sources can use these or
 * implement the core abstractions directly.
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.CloseableIterator} - Generic closeable iterator
 *       (Iterator + Closeable), used by FormatReader and other streaming APIs</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourcePushdownRule} - Convenience base for
 *       optimization rules that push operations into data source plan leaves</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSource#applyOptimizationRules
 *       DataSource.applyOptimizationRules()} - Collects and runs data source-provided optimization rules</li>
 * </ul>
 *
 * <h2>Sub-Packages</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi spi} - Core SPI contracts
 *       (DataSource, DataSourcePlan, DataSourcePartition, etc.)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse lakehouse} - Storage + format
 *       separation for data lake sources (Iceberg, Delta Lake, Parquet).
 *       SPI contracts in {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi lakehouse/spi}
 *       (including {@link org.elasticsearch.xpack.esql.datasource.spi.partitioning partitioning}),
 *       helpers (registries, operators, glob) at package root.</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.connector connector} - Connection-oriented
 *       data sources (Flight, JDBC, gRPC). SPI contracts in
 *       {@link org.elasticsearch.xpack.esql.datasource.connector.spi connector/spi},
 *       async operator infrastructure at package root.</li>
 * </ul>
 *
 * <h2>Design Principles</h2>
 *
 * <p><b>DataSource-specific plan nodes:</b> Each data source defines its own
 * {@link org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlan}
 * implementation that extends {@link org.elasticsearch.xpack.esql.plan.logical.LeafPlan}. This allows data sources to store
 * type-safe data source-specific state (e.g., Iceberg manifests, SQL fragments) without opaque state objects.
 *
 * <p><b>No enumerated pushdown types:</b> Instead of declaring "I support filter pushdown",
 * data sources provide their own {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource#optimizationRules() optimization rules}
 * that pattern-match on standard ES|QL plan nodes and fold operations into the data source plan leaf.
 * This allows data sources to make nuanced, case-by-case decisions without coupling the SPI
 * to specific optimization types.
 *
 * <h2>Base Classes</h2>
 *
 * <p>Sub-packages provide abstract base classes for common patterns:
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.LakehouseDataSource} -
 *       For Iceberg, Delta Lake, Hudi, raw Parquet (composes
 *       {@link org.elasticsearch.xpack.esql.datasource.spi.partitioning.SplitPartitioner},
 *       {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageProvider}
 *       + {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatReader})</li>
 * </ul>
 *
 * <h2>Lakehouse SPI</h2>
 *
 * <p>The {@link org.elasticsearch.xpack.esql.datasource.lakehouse lakehouse} sub-package provides
 * production-ready abstractions for data lake access:
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageProvider} /
 *       {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageObject} — storage access</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatReader} — format reading</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.TableCatalog} — catalog integration</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FilterPushdownSupport} — filter pushdown</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceMetadata} — schema and statistics</li>
 * </ul>
 *
 * <h2>Example Usage</h2>
 *
 * <pre>{@code
 * // 1. Define data source-specific plan node
 * public class IcebergPlan extends LakehousePlan {
 *     private final IcebergDataSource dataSource;
 *     private final Expression filter;
 *     private final Integer limit;
 *
 *     @Override
 *     public DataSource dataSource() { return dataSource; }
 *
 *     @Override
 *     public LakehousePlan withFilter(Expression filter) {
 *         return new IcebergPlan(..., filter, this.limit);
 *     }
 * }
 *
 * // 2. Implement the data source (composes storage + format)
 * public class IcebergDataSource extends LakehouseDataSource {
 *
 *     @Override
 *     public String type() { return "iceberg"; }
 *
 *     @Override
 *     protected StorageProvider getStorageProvider() { return s3Storage; }
 *
 *     @Override
 *     protected FormatReader getFormatReader() { return parquetReader; }
 *
 *     // Override resolve() to use Iceberg catalog metadata
 *     @Override
 *     public DataSourcePlan resolve(DataSourceDescriptor source, ResolutionContext context) {
 *         // Use Iceberg catalog to get schema instead of inferring from files
 *     }
 * }
 * }</pre>
 */
package org.elasticsearch.xpack.esql.datasource;
