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
import org.elasticsearch.common.unit.ByteSizeValue;
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

    /**
     * Gates the satellite protocol on top of the Flight connector: the capability handshake, plan fragments pushed to the server and the
     * dataset settings that only a satellite understands. Snapshot-on, release-off; override in release with
     * {@code -Des.esql_satellites_feature_flag_enabled=true}. When it is off a Flight server is read as a plain Level 0 server.
     */
    public static final FeatureFlag ESQL_SATELLITES_FEATURE_FLAG = new FeatureFlag("esql_satellites");

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
     * The hosts the nodes may open Flight connections to (see {@link FlightGuard}). The default is this machine only: a dataset
     * names its host, so a wider default would let whoever can create a dataset reach every host a data node can.
     */
    public static final Setting<List<String>> FLIGHT_ALLOWED_HOSTS = Setting.listSetting(
        "esql.datasource.flight.allowed_hosts",
        FlightGuard.DEFAULT_ALLOWED,
        entry -> {
            new FlightGuard(List.of(entry)); // an entry that cannot be read is refused when the node starts
            return entry;
        },
        Setting.Property.NodeScope
    );

    /** The largest inbound gRPC message a node accepts from a satellite: a record batch, a schema, a flight info, an action result. */
    public static final Setting<ByteSizeValue> FLIGHT_MAX_MESSAGE_SIZE = Setting.byteSizeSetting(
        "esql.datasource.flight.max_message_size",
        ByteSizeValue.ofMb(64),
        ByteSizeValue.ofMb(1),
        ByteSizeValue.ofBytes(Integer.MAX_VALUE),
        Setting.Property.NodeScope
    );

    /** The most endpoints a flight info may list; each becomes a split that is sent to every node. */
    public static final Setting<Integer> FLIGHT_MAX_ENDPOINTS = Setting.intSetting(
        "esql.datasource.flight.max_endpoints",
        FlightGuard.Limits.DEFAULT.maxEndpoints(),
        1,
        Setting.Property.NodeScope
    );

    /** The most columns a satellite's table may have. */
    public static final Setting<Integer> FLIGHT_MAX_COLUMNS = Setting.intSetting(
        "esql.datasource.flight.max_columns",
        FlightGuard.Limits.DEFAULT.maxColumns(),
        1,
        Setting.Property.NodeScope
    );

    /**
     * How long a result stream may send nothing before the read fails; zero for no limit. The wait for the first batch counts, and a
     * satellite that sorts or aggregates reads all its input before it, so the default is ten minutes.
     */
    public static final Setting<TimeValue> FLIGHT_STREAM_IDLE_TIMEOUT = Setting.timeSetting(
        "esql.datasource.flight.stream_idle_timeout",
        FlightGuard.Limits.DEFAULT.streamIdleTimeout(),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            FLIGHT_CALL_TIMEOUT,
            FLIGHT_DECLARATION_TTL,
            FLIGHT_ALLOWED_HOSTS,
            FLIGHT_MAX_MESSAGE_SIZE,
            FLIGHT_MAX_ENDPOINTS,
            FLIGHT_MAX_COLUMNS,
            FLIGHT_STREAM_IDLE_TIMEOUT
        );
    }

    private static FlightGuard guard(Settings settings) {
        return new FlightGuard(
            FLIGHT_ALLOWED_HOSTS.get(settings),
            new FlightGuard.Limits(
                FLIGHT_MAX_MESSAGE_SIZE.get(settings).getBytes(),
                FLIGHT_MAX_ENDPOINTS.get(settings),
                FLIGHT_MAX_COLUMNS.get(settings),
                FLIGHT_STREAM_IDLE_TIMEOUT.get(settings)
            )
        );
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
        FlightGuard guard = guard(settings);
        // the TLS settings of the data source reach the provider as config keys, so that its existence check trusts the same
        // authority as the connector
        StorageProviderFactory factory = StorageProviderFactory.of(
            () -> new FlightStorageProvider(null, null, null, guard),
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
            tls -> new FlightStorageProvider(tls.caCertificate(), tls.serverName(), tls.token(), guard)
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
                guard(settings),
                ESQL_SATELLITES_FEATURE_FLAG.isEnabled()
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
        DataSourceValidator flight = new FlightDataSourceValidator(guard(settings));
        return Map.of(flight.type(), flight);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(FlightSplit.ENTRY);
    }
}
