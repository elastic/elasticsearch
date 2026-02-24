/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.connector.spi;

import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.SourceMetadata;

import java.util.Map;

/**
 * Factory for creating connectors to external data sources.
 * Each factory handles a specific protocol or source type (e.g. "flight", "jdbc").
 * Schema resolution lives here to avoid creating throwaway {@link Connector} instances.
 *
 * <p>Unlike the main branch's {@code ConnectorFactory} which extends {@code ExternalSourceFactory},
 * this is a standalone interface. In our architecture, {@code DataSource} handles the lifecycle
 * concerns (filter pushdown via optimization rules, operator creation via
 * {@code createSourceOperator}), so ConnectorFactory only needs connector-relevant methods.
 */
public interface ConnectorFactory {

    /**
     * Returns the connector type identifier (e.g. "flight", "jdbc").
     */
    String type();

    /**
     * Returns whether this factory can handle the given location.
     */
    boolean canHandle(String location);

    /**
     * Resolves metadata (schema, statistics) for the given location without opening a full connection.
     */
    SourceMetadata resolveMetadata(String location, Map<String, Object> config);

    /**
     * Opens a live connection to the data source.
     */
    Connector open(Map<String, Object> config);
}
