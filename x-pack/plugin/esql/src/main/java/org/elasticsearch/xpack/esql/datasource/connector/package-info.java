/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Connector infrastructure for connection-oriented external data sources.
 *
 * <p>This package contains internal helpers for executing connector queries.
 * SPI contracts (interfaces) live in the
 * {@link org.elasticsearch.xpack.esql.datasource.connector.spi spi} sub-package.
 *
 * <h2>Infrastructure</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.connector.AsyncConnectorSourceOperatorFactory} —
 *       Single-split source operator factory that executes a connector query on a background
 *       thread and feeds pages into an
 *       {@link org.elasticsearch.xpack.esql.datasource.lakehouse.AsyncExternalSourceBuffer}
 *       for the driver to consume. Uses shared async infrastructure from the
 *       {@link org.elasticsearch.xpack.esql.datasource.lakehouse lakehouse} package.</li>
 * </ul>
 *
 * @see org.elasticsearch.xpack.esql.datasource.connector.spi connector SPI
 * @see org.elasticsearch.xpack.esql.datasource.lakehouse lakehouse infrastructure
 */
package org.elasticsearch.xpack.esql.datasource.connector;
