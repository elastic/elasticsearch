/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Core SPI contracts for ES|QL external data sources.
 *
 * <p>This package contains all interfaces and abstract classes that data source
 * implementations must implement or extend. See the
 * {@link org.elasticsearch.xpack.esql.datasource parent package} for an overview
 * of the invocation flow and design principles.
 *
 * <h2>Main Interface</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSource} — lifecycle hooks:
 *       resolve (async via ActionListener), capabilities, optimizationRules, createPhysicalPlan,
 *       planPartitions, createSourceOperator</li>
 * </ul>
 *
 * <h2>Plan Nodes</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlan} — abstract base
 *       for data source logical plan leaves</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourceExec} — physical plan
 *       node wrapping a DataSourcePlan</li>
 * </ul>
 *
 * <h2>Supporting Types</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourceDescriptor} — parsed
 *       data source reference (type, config, settings)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourcePartition} — unit of
 *       work for distributed execution</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourceCapabilities} — execution
 *       mode flag (distributed vs coordinator-only)</li>
 * </ul>
 *
 * <h2>Optimization</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourceOptimizer} — separate
 *       rule executor for data source-provided optimization rules</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourcePushdownRule} — convenience
 *       base for filter/limit pushdown rules</li>
 * </ul>
 *
 * <h2>Plugin Discovery</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlugin} — extension point:
 *       {@code dataSources(Settings)} returns {@code Map<String, DataSourceFactory>}</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.DataSourceFactory} — factory for
 *       creating DataSource instances</li>
 * </ul>
 *
 * <h2>Utilities</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.spi.CloseableIterator} — generic
 *       closeable iterator (Iterator + Closeable)</li>
 * </ul>
 */
package org.elasticsearch.xpack.esql.datasource.spi;
