/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Base classes for SQL-style data sources.
 *
 * <h2>SqlDataSource</h2>
 *
 * <p>{@link org.elasticsearch.xpack.esql.datasource.sql.SqlDataSource} provides
 * a foundation for data sources that query relational databases via SQL:
 *
 * <ul>
 *   <li>PostgreSQL</li>
 *   <li>MySQL</li>
 *   <li>Oracle</li>
 *   <li>SQL Server</li>
 * </ul>
 *
 * <p>Data sources extending this class should implement
 * {@link org.elasticsearch.xpack.esql.datasource.sql.SqlPlan} for their plan nodes.
 *
 * <p>SQL data sources typically support full pushdown:
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
 * <h2>Implementing a SQL DataSource</h2>
 *
 * <p>Subclasses must:
 * <ol>
 *   <li>Define a data source-specific plan class extending
 *       {@link org.elasticsearch.xpack.esql.datasource.sql.SqlPlan}</li>
 *   <li>Implement {@link org.elasticsearch.xpack.esql.datasource.DataSource#resolve} to create
 *       the plan node with schema</li>
 *   <li>Implement the abstract translation methods for filter/aggregation/etc.</li>
 *   <li>Implement {@link org.elasticsearch.xpack.esql.datasource.DataSource#createSourceOperator}
 *       to create the actual data reader</li>
 * </ol>
 *
 * @see org.elasticsearch.xpack.esql.datasource.sql.SqlDataSource
 * @see org.elasticsearch.xpack.esql.datasource.sql.SqlPlan
 * @see org.elasticsearch.xpack.esql.datasource.lakehouse.LakehouseDataSource
 */
package org.elasticsearch.xpack.esql.datasource.sql;
