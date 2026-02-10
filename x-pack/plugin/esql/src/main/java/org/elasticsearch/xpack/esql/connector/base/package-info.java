/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Abstract base classes for common connector patterns.
 *
 * <h2>DataLakeConnector</h2>
 *
 * <p>{@link org.elasticsearch.xpack.esql.connector.base.DataLakeConnector} provides
 * a pluggable architecture for reading from data lakes with separated storage and
 * format concerns:
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.connector.base.StorageProvider} -
 *       Access files in a storage system (S3, GCS, HDFS, local filesystem)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.connector.base.FormatReader} -
 *       Read file formats (Parquet, ORC, CSV, Avro)</li>
 * </ul>
 *
 * <p>This separation allows any storage to be combined with any format:
 * <ul>
 *   <li>S3 + Parquet</li>
 *   <li>GCS + ORC</li>
 *   <li>Local filesystem + CSV</li>
 *   <li>S3 + Parquet + Iceberg catalog (override resolve)</li>
 * </ul>
 *
 * <p>Connectors extending this class should implement
 * {@link org.elasticsearch.xpack.esql.connector.base.DataLakePlan} for their plan nodes.
 *
 * <p>Data lake connectors typically support:
 * <ul>
 *   <li>Filter pushdown (partition pruning, file-level filtering)</li>
 *   <li>Limit pushdown</li>
 *   <li>Column projection</li>
 *   <li>Partitioned parallel execution across data nodes</li>
 * </ul>
 *
 * <p>And typically do NOT support:
 * <ul>
 *   <li>ORDER BY pushdown (no sorted output guarantee)</li>
 *   <li>Aggregation pushdown (no compute capability)</li>
 * </ul>
 *
 * <h2>SqlConnector</h2>
 *
 * <p>{@link org.elasticsearch.xpack.esql.connector.base.SqlConnector} provides
 * a foundation for connectors that query relational databases via SQL:
 *
 * <ul>
 *   <li>PostgreSQL</li>
 *   <li>MySQL</li>
 *   <li>Oracle</li>
 *   <li>SQL Server</li>
 * </ul>
 *
 * <p>Connectors extending this class should implement
 * {@link org.elasticsearch.xpack.esql.connector.base.SqlPlan} for their plan nodes.
 *
 * <p>SQL connectors typically support full pushdown:
 * <ul>
 *   <li>Filter to SQL WHERE</li>
 *   <li>ORDER BY to SQL ORDER BY</li>
 *   <li>LIMIT to SQL LIMIT</li>
 *   <li>Aggregations to SQL SELECT with GROUP BY</li>
 * </ul>
 *
 * <p>The base class accumulates operations into a SQL query that the database
 * executes, avoiding the need to stream all data and process locally.
 *
 * <h2>Implementing a Connector</h2>
 *
 * <p>For data lake connectors, subclasses must:
 * <ol>
 *   <li>Implement {@link org.elasticsearch.xpack.esql.connector.base.DataLakeConnector#getStorageProvider()}
 *       and {@link org.elasticsearch.xpack.esql.connector.base.DataLakeConnector#getFormatReader()}</li>
 *   <li>Define a connector-specific plan class extending
 *       {@link org.elasticsearch.xpack.esql.connector.base.DataLakePlan}</li>
 *   <li>Implement {@link org.elasticsearch.xpack.esql.connector.base.DataLakeConnector#createPlan}
 *       to create the plan node</li>
 *   <li>Implement the abstract operation methods (applyFilter, applyLimit, etc.)</li>
 *   <li>Implement {@link org.elasticsearch.xpack.esql.connector.Connector#createSourceOperator}
 *       to create the actual data reader</li>
 * </ol>
 *
 * <p>For SQL connectors, subclasses must:
 * <ol>
 *   <li>Define a connector-specific plan class extending
 *       {@link org.elasticsearch.xpack.esql.connector.base.SqlPlan}</li>
 *   <li>Implement {@link org.elasticsearch.xpack.esql.connector.Connector#resolve} to create
 *       the plan node with schema</li>
 *   <li>Implement the abstract translation methods for filter/aggregation/etc.</li>
 *   <li>Implement {@link org.elasticsearch.xpack.esql.connector.Connector#createSourceOperator}
 *       to create the actual data reader</li>
 * </ol>
 */
package org.elasticsearch.xpack.esql.connector.base;
