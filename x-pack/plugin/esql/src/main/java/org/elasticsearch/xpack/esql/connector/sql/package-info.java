/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Base classes for SQL-style connectors.
 *
 * <h2>SqlConnector</h2>
 *
 * <p>{@link org.elasticsearch.xpack.esql.connector.sql.SqlConnector} provides
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
 * {@link org.elasticsearch.xpack.esql.connector.sql.SqlPlan} for their plan nodes.
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
 * <h2>Implementing a SQL Connector</h2>
 *
 * <p>Subclasses must:
 * <ol>
 *   <li>Define a connector-specific plan class extending
 *       {@link org.elasticsearch.xpack.esql.connector.sql.SqlPlan}</li>
 *   <li>Implement {@link org.elasticsearch.xpack.esql.connector.Connector#resolve} to create
 *       the plan node with schema</li>
 *   <li>Implement the abstract translation methods for filter/aggregation/etc.</li>
 *   <li>Implement {@link org.elasticsearch.xpack.esql.connector.Connector#createSourceOperator}
 *       to create the actual data reader</li>
 * </ol>
 *
 * @see org.elasticsearch.xpack.esql.connector.sql.SqlConnector
 * @see org.elasticsearch.xpack.esql.connector.sql.SqlPlan
 * @see org.elasticsearch.xpack.esql.connector.lakehouse.LakehouseConnector
 */
package org.elasticsearch.xpack.esql.connector.sql;
