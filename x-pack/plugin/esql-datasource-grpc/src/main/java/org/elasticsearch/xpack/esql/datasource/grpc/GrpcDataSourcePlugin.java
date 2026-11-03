/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
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

    static final TimeValue DEFAULT_CALL_TIMEOUT = TimeValue.timeValueSeconds(60);

    /**
     * Deadline of the Flight calls that only plan a read: schema, capabilities, split discovery and the flight info of a fragment.
     * The result stream has no fixed deadline; it ends with the query, by completion, timeout or cancellation.
     */
    public static final Setting<TimeValue> FLIGHT_CALL_TIMEOUT = Setting.timeSetting(
        "esql.datasource.flight.call_timeout",
        DEFAULT_CALL_TIMEOUT,
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope
    );

    static final TimeValue DEFAULT_DECLARATION_TTL = TimeValue.timeValueMinutes(1);

    /**
     * How long a node keeps the capability declaration a satellite answered, so that queries within it do not repeat the capability
     * round trip. It bounds how long a satellite that lost a capability keeps failing the queries this node planned, and how long
     * one that gained a capability goes unused. Zero reads the declaration on every query.
     */
    public static final Setting<TimeValue> FLIGHT_DECLARATION_TTL = Setting.timeSetting(
        "esql.datasource.flight.declaration_ttl",
        DEFAULT_DECLARATION_TTL,
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * The hosts the nodes may open Flight connections to (see {@link FlightHostPolicy}). The default is this machine only: a dataset
     * names its host, so a wider default would let whoever can create a dataset reach every host a data node can.
     */
    public static final Setting<List<String>> FLIGHT_ALLOWED_HOSTS = Setting.listSetting(
        "esql.datasource.flight.allowed_hosts",
        FlightHostPolicy.DEFAULT_ALLOWED,
        entry -> {
            new FlightHostPolicy(List.of(entry)); // an entry that cannot be read is refused when the node starts
            return entry;
        },
        Setting.Property.NodeScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(FLIGHT_CALL_TIMEOUT, FLIGHT_DECLARATION_TTL, FLIGHT_ALLOWED_HOSTS);
    }

    private static FlightHostPolicy policy(Settings settings) {
        return new FlightHostPolicy(FLIGHT_ALLOWED_HOSTS.get(settings));
    }

    private static boolean enabled() {
        return ESQL_EXTERNAL_DATASOURCES_GRPC_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Set<String> supportedSchemes() {
        if (enabled() == false) {
            return Set.of();
        }
        return Set.of("flight", "grpc", "flight+tls", "grpc+tls");
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        FlightHostPolicy policy = policy(settings);
        // the TLS settings of the data source reach the provider as config keys, so that its existence check trusts the same
        // authority as the connector
        StorageProviderFactory factory = StorageProviderFactory.of(
            () -> new FlightStorageProvider(null, null, null, policy),
            config -> Configured.fromKnownSubset(
                new FlightClients.Transport(
                    true,
                    FlightClients.setting(config, FlightClients.CA_CERTIFICATE),
                    FlightClients.setting(config, FlightClients.SERVER_NAME),
                    FlightClients.setting(config, FlightClients.TOKEN)
                ),
                config,
                FlightClients.TLS_SETTINGS
            ),
            tls -> new FlightStorageProvider(tls.caCertificate(), tls.serverName(), tls.token(), policy)
        );
        return Map.of("flight", factory, "grpc", factory, "flight+tls", factory, "grpc+tls", factory);
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        if (enabled() == false) {
            return Set.of();
        }
        return Set.of("flight", "grpc", "flight+tls", "grpc+tls");
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        return Map.of(
            "flight",
            new FlightConnectorFactory(
                FLIGHT_CALL_TIMEOUT.get(settings),
                new SatelliteDeclarationCache(FLIGHT_DECLARATION_TTL.get(settings)),
                policy(settings)
            )
        );
    }

    @Override
    public Set<String> datasourceSecretSettingNames() {
        return FlightClients.SECRET_SETTINGS;
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        DataSourceValidator flight = new FlightDataSourceValidator(policy(settings));
        return Map.of(flight.type(), flight);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(FlightSplit.ENTRY);
    }
}
