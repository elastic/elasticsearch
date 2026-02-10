/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Connector SPI for external data sources in ES|QL.
 *
 * <h2>Invocation Flow</h2>
 *
 * <p>Connector methods are called by specific ES|QL components at defined phases:
 *
 * <ol>
 *   <li><b>Resolution</b> - {@code PreAnalyzer} calls
 *       {@link org.elasticsearch.xpack.esql.connector.Connector#resolve} to discover schema
 *       and create the connector's logical plan node</li>
 *   <li><b>Logical Optimization</b> -
 *       {@link org.elasticsearch.xpack.esql.connector.Connector#applyOptimizationRules Connector.applyOptimizationRules()}
 *       collects {@link org.elasticsearch.xpack.esql.connector.Connector#optimizationRules() rules}
 *       from connectors and runs them as a separate pass after the main optimizer</li>
 *   <li><b>Physical Planning</b> - {@code Mapper} calls
 *       {@link org.elasticsearch.xpack.esql.connector.Connector#createPhysicalPlan}</li>
 *   <li><b>Work Distribution</b> - Physical planner calls
 *       {@link org.elasticsearch.xpack.esql.connector.Connector#planPartitions}</li>
 *   <li><b>Execution</b> - {@code LocalExecutionPlanner} calls
 *       {@link org.elasticsearch.xpack.esql.connector.Connector#createSourceOperator}</li>
 * </ol>
 *
 * <h2>Key Interfaces</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.connector.Connector} - Main SPI interface with all lifecycle hooks</li>
 *   <li>{@link org.elasticsearch.xpack.esql.connector.ConnectorPlan} - Interface for connector-specific logical plan nodes</li>
 *   <li>{@link org.elasticsearch.xpack.esql.connector.Connector#applyOptimizationRules
 *       Connector.applyOptimizationRules()} - Runs connector-provided optimization rules</li>
 *   <li>{@link org.elasticsearch.xpack.esql.connector.ConnectorPartition} - Unit of work for distributed execution</li>
 * </ul>
 *
 * <h2>Design Principles</h2>
 *
 * <p><b>Connector-specific plan nodes:</b> Each connector defines its own {@link org.elasticsearch.xpack.esql.connector.ConnectorPlan}
 * implementation that extends {@link org.elasticsearch.xpack.esql.plan.logical.LeafPlan}. This allows connectors to store
 * type-safe connector-specific state (e.g., Iceberg manifests, SQL fragments) without opaque state objects.
 *
 * <p><b>No enumerated pushdown types:</b> Instead of declaring "I support filter pushdown",
 * connectors provide their own {@link org.elasticsearch.xpack.esql.connector.Connector#optimizationRules() optimization rules}
 * that pattern-match on standard ES|QL plan nodes and fold operations into the connector plan leaf.
 * This allows connectors to make nuanced, case-by-case decisions without coupling the SPI
 * to specific optimization types.
 *
 * <h2>Base Classes</h2>
 *
 * <p>The {@code base} sub-package provides abstract base classes for common patterns:
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.connector.base.DataLakeConnector} -
 *       For Iceberg, Delta Lake, Hudi, raw Parquet (composes {@link org.elasticsearch.xpack.esql.connector.base.StorageProvider}
 *       + {@link org.elasticsearch.xpack.esql.connector.base.FormatReader})</li>
 *   <li>{@link org.elasticsearch.xpack.esql.connector.base.SqlConnector} -
 *       For PostgreSQL, MySQL, Oracle, etc.</li>
 * </ul>
 *
 * <h2>Example Usage</h2>
 *
 * <pre>{@code
 * // 1. Define connector-specific plan node
 * public class IcebergPlan extends LeafPlan implements DataLakePlan {
 *     private final IcebergConnector connector;
 *     private final Expression filter;
 *     private final Integer limit;
 *
 *     @Override
 *     public Connector connector() { return connector; }
 *
 *     @Override
 *     public DataLakePlan withFilter(Expression filter) {
 *         return new IcebergPlan(..., filter, this.limit);
 *     }
 * }
 *
 * // 2. Implement the connector (composes storage + format)
 * public class IcebergConnector extends DataLakeConnector {
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
 *     public ConnectorPlan resolve(ConnectorSourceDescriptor source, ResolutionContext context) {
 *         // Use Iceberg catalog to get schema instead of inferring from files
 *     }
 * }
 * }</pre>
 */
package org.elasticsearch.xpack.esql.connector;
