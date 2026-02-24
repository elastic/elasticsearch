/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Connector SPI for connection-oriented external data sources.
 *
 * <p>While the {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi lakehouse SPI}
 * handles file-based storage (S3 + Parquet, GCS + ORC, etc.), the connector SPI handles
 * protocol-based sources that require live connections (Flight, JDBC, gRPC).
 *
 * <h2>Core Types</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.connector.spi.ConnectorFactory} —
 *       Factory for creating connectors; handles schema resolution and connection creation</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.connector.spi.Connector} —
 *       Live connection to an external data source; discovers splits and executes queries</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.connector.spi.QueryRequest} —
 *       Immutable query descriptor with target, projected columns, config, and batch size</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.connector.spi.ResultCursor} —
 *       Streaming cursor over query results (extends
 *       {@link org.elasticsearch.xpack.esql.datasource.spi.CloseableIterator})</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.connector.spi.Split} —
 *       Unit of parallel work; simple connectors use {@link
 *       org.elasticsearch.xpack.esql.datasource.connector.spi.Split#SINGLE}</li>
 * </ul>
 *
 * <h2>Plugin Discovery</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.connector.spi.ConnectorPlugin} —
 *       Extension point for connector implementations; returns
 *       {@code Map<String, ConnectorFactory>} keyed by connector type</li>
 * </ul>
 *
 * <h2>Relationship to Core SPI</h2>
 *
 * <p>The connector SPI is a lower-level abstraction that provides the raw connection
 * and query execution mechanism. A future {@code ConnectorDataSource} will implement
 * {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource} by delegating to
 * connector SPI types, mapping:
 * <ul>
 *   <li>{@code DataSource.resolve()} → {@code ConnectorFactory.resolveMetadata()}</li>
 *   <li>{@code DataSource.planPartitions()} → {@code Connector.discoverSplits()}</li>
 *   <li>{@code DataSource.createSourceOperator()} → {@code Connector.execute()}</li>
 * </ul>
 *
 * @see org.elasticsearch.xpack.esql.datasource.connector connector infrastructure
 * @see org.elasticsearch.xpack.esql.datasource.spi core DataSource SPI
 */
package org.elasticsearch.xpack.esql.datasource.connector.spi;
