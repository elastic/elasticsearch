/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;

import java.util.Map;
import java.util.Set;

/**
 * Registers the Arrow Flight connector for ESQL.
 * Handles {@code flight://} and {@code grpc://} URIs for columnar data streaming via gRPC.
 */
public class GrpcDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<String> supportedConnectorSchemes() {
        return Set.of("flight", "grpc");
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        return Map.of("flight", new FlightConnectorFactory());
    }
}
