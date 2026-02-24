/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.connector.spi;

import org.elasticsearch.common.settings.Settings;

import java.util.Map;
import java.util.Set;

/**
 * Extension point for connector implementations.
 *
 * <p>Plugins implementing this interface provide connection-oriented data source
 * access for protocol-based sources (e.g. Flight, JDBC). Each connector factory
 * is keyed by connector type (e.g. "flight", "jdbc").
 *
 * <p>Follows the same pattern as
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StoragePlugin},
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatPlugin}, and
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.CatalogPlugin}.
 *
 * @see ConnectorFactory
 * @see Connector
 */
public interface ConnectorPlugin {

    /**
     * Returns the set of URI schemes this plugin's connectors can handle.
     * Used for capability queries without loading connector instances.
     *
     * @return set of supported URI schemes (e.g. "flight", "jdbc")
     */
    default Set<String> supportedConnectorSchemes() {
        return Set.of();
    }

    /**
     * Returns factories for creating {@link ConnectorFactory} instances, keyed
     * by connector type identifier.
     *
     * @param settings the node settings
     * @return map of connector type to connector factory
     */
    Map<String, ConnectorFactory> connectors(Settings settings);
}
