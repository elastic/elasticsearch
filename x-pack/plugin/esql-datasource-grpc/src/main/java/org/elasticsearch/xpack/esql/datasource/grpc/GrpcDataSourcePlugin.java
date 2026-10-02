/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Registers the Arrow Flight connector and storage SPI for ESQL.
 * Handles {@code flight://} and {@code grpc://} URIs for columnar data streaming via gRPC.
 *
 * <p>gRPC/Flight is not in the released ship set yet, so registration of both schemes and the
 * Flight connector is gated on {@link #ESQL_EXTERNAL_DATASOURCES_GRPC_FEATURE_FLAG}: available in
 * snapshot/development builds, disabled in release. When the gate is off the {@code flight}/
 * {@code grpc} schemes and connector are not registered, so a source targeting either resolves to
 * the generic "unsupported storage scheme" rejection.
 */
public class GrpcDataSourcePlugin extends Plugin implements DataSourcePlugin {

    /**
     * Gates the gRPC/Flight storage providers and connector. Snapshot-on, release-off; override in
     * release with {@code -Des.esql_external_datasources_grpc_feature_flag_enabled=true}.
     */
    public static final FeatureFlag ESQL_EXTERNAL_DATASOURCES_GRPC_FEATURE_FLAG = new FeatureFlag("esql_external_datasources_grpc");

    private static boolean enabled() {
        return ESQL_EXTERNAL_DATASOURCES_GRPC_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Set<String> supportedSchemes() {
        if (enabled() == false) {
            return Set.of();
        }
        return Set.of("flight", "grpc");
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        StorageProviderFactory factory = StorageProviderFactory.noConfigKeys(FlightStorageProvider::new);
        return Map.of("flight", factory, "grpc", factory);
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        if (enabled() == false) {
            return Set.of();
        }
        return Set.of("flight", "grpc");
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        return Map.of("flight", new FlightConnectorFactory());
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(FlightSplit.ENTRY);
    }
}
