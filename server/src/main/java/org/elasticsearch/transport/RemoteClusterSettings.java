/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.enumSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;
import static org.elasticsearch.transport.RemoteConnectionStrategy.ConnectionStrategy;

public class RemoteClusterSettings {

    /**
     * The initial connect timeout for remote cluster connections
     */
    public static final Setting<TimeValue> REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "cluster.remote.initial_connect_timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    /**
     * The name of a node attribute to select nodes that should be connected to in the remote cluster.
     * For instance a node can be configured with {@code node.attr.gateway: true} in order to be eligible as a gateway node between
     * clusters. In that case {@code cluster.remote.node.attr: gateway} can be used to filter out other nodes in the remote cluster.
     * The value of the setting is expected to be a boolean, {@code true} for nodes that can become gateways, {@code false} otherwise.
     */
    public static final Setting<String> REMOTE_NODE_ATTRIBUTE = Setting.simpleString(
        "cluster.remote.node.attr",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<Boolean> REMOTE_CLUSTER_SKIP_UNAVAILABLE = Setting.affixKeySetting(
        "cluster.remote.",
        "skip_unavailable",
        (ns, key) -> boolSetting(key, true, new RemoteConnectionEnabled<>(ns, key), Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> REMOTE_CLUSTER_PING_SCHEDULE = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.ping_schedule",
        (ns, key) -> timeSetting(
            key,
            TransportSettings.PING_SCHEDULE,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    public static final Setting.AffixSetting<Compression.Enabled> REMOTE_CLUSTER_COMPRESS = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.compress",
        (ns, key) -> enumSetting(
            Compression.Enabled.class,
            key,
            TransportSettings.TRANSPORT_COMPRESS,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    public static final Setting.AffixSetting<Compression.Scheme> REMOTE_CLUSTER_COMPRESSION_SCHEME = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.compression_scheme",
        (ns, key) -> enumSetting(
            Compression.Scheme.class,
            key,
            TransportSettings.TRANSPORT_COMPRESSION_SCHEME,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    public static final Setting.AffixSetting<SecureString> REMOTE_CLUSTER_CREDENTIALS = Setting.affixKeySetting(
        "cluster.remote.",
        "credentials",
        key -> SecureSetting.secureString(key, null)
    );

    public static final Setting.AffixSetting<RemoteConnectionStrategy.ConnectionStrategy> REMOTE_CONNECTION_MODE = Setting.affixKeySetting(
        "cluster.remote.",
        "mode",
        key -> new Setting<>(
            key,
            RemoteConnectionStrategy.ConnectionStrategy.SNIFF.name(),
            value -> RemoteConnectionStrategy.ConnectionStrategy.valueOf(value.toUpperCase(Locale.ROOT)),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    );

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<Integer> REMOTE_MAX_PENDING_CONNECTION_LISTENERS = Setting.intSetting(
        "cluster.remote.max_pending_connection_listeners",
        1000,
        Setting.Property.NodeScope
    );

    public static class ProxyConnectionStrategySettings {
        /**
         * The remote address for the proxy. The connections will be opened to the configured address.
         */
        public static final Setting.AffixSetting<String> PROXY_ADDRESS = Setting.affixKeySetting(
            "cluster.remote.",
            "proxy_address",
            (ns, key) -> Setting.simpleString(
                key,
                new StrategyValidator<>(ns, key, RemoteConnectionStrategy.ConnectionStrategy.PROXY, s -> {
                    if (Strings.hasLength(s)) {
                        RemoteConnectionStrategy.parsePort(s);
                    }
                }),
                Setting.Property.Dynamic,
                Setting.Property.NodeScope
            )
        );

        /**
         * The maximum number of socket connections that will be established to a remote cluster. The default is 18.
         */
        public static final Setting.AffixSetting<Integer> REMOTE_SOCKET_CONNECTIONS = Setting.affixKeySetting(
            "cluster.remote.",
            "proxy_socket_connections",
            (ns, key) -> intSetting(
                key,
                18,
                1,
                new StrategyValidator<>(ns, key, RemoteConnectionStrategy.ConnectionStrategy.PROXY),
                Setting.Property.Dynamic,
                Setting.Property.NodeScope
            )
        );

        /**
         * A configurable server_name attribute
         */
        public static final Setting.AffixSetting<String> SERVER_NAME = Setting.affixKeySetting(
            "cluster.remote.",
            "server_name",
            (ns, key) -> Setting.simpleString(
                key,
                new StrategyValidator<>(ns, key, RemoteConnectionStrategy.ConnectionStrategy.PROXY),
                Setting.Property.Dynamic,
                Setting.Property.NodeScope
            )
        );

        static Stream<Setting.AffixSetting<?>> enablementSettings() {
            return Stream.of(PROXY_ADDRESS);
        }

        static void readSettings(String clusterAlias, Settings settings, LinkedProjectConfig.Builder builder) {
            builder.proxyAddress(PROXY_ADDRESS.getConcreteSettingForNamespace(clusterAlias).get(settings))
                .proxyNumSocketConnections(REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(settings))
                .proxyServerName(SERVER_NAME.getConcreteSettingForNamespace(clusterAlias).get(settings));
        }
    }

    public static class SniffConnectionStrategySettings {
        /**
         * A list of initial seed nodes to discover eligible nodes from the remote cluster
         */
        public static final Setting.AffixSetting<List<String>> REMOTE_CLUSTER_SEEDS = Setting.affixKeySetting(
            "cluster.remote.",
            "seeds",
            (ns, key) -> Setting.listSetting(key, Collections.emptyList(), s -> {
                // validate seed address
                RemoteConnectionStrategy.parsePort(s);
                return s;
            },
                new StrategyValidator<>(ns, key, RemoteConnectionStrategy.ConnectionStrategy.SNIFF),
                Setting.Property.Dynamic,
                Setting.Property.NodeScope
            )
        );

        /**
         * A proxy address for the remote cluster. By default this is not set, meaning that Elasticsearch will connect directly to the nodes
         * in the remote cluster using their publish addresses. If this setting is set to an IP address or hostname then Elasticsearch will
         * connect to the nodes in the remote cluster using this address instead. Use of this setting is not recommended and it is
         * deliberately undocumented as it does not work well with all proxies.
         */
        public static final Setting.AffixSetting<String> REMOTE_CLUSTERS_PROXY = Setting.affixKeySetting(
            "cluster.remote.",
            "proxy",
            (ns, key) -> Setting.simpleString(
                key,
                new StrategyValidator<>(ns, key, RemoteConnectionStrategy.ConnectionStrategy.SNIFF, s -> {
                    if (Strings.hasLength(s)) {
                        RemoteConnectionStrategy.parsePort(s);
                    }
                }),
                Setting.Property.Dynamic,
                Setting.Property.NodeScope
            ),
            () -> REMOTE_CLUSTER_SEEDS
        );

        /**
         * The maximum number of connections that will be established to a remote cluster. For instance if there is only a single
         * seed node, other nodes will be discovered up to the given number of nodes in this setting. The default is 3.
         */
        public static final Setting<Integer> REMOTE_CONNECTIONS_PER_CLUSTER = intSetting(
            "cluster.remote.connections_per_cluster",
            3,
            1,
            Setting.Property.NodeScope
        );

        /**
         * The maximum number of node connections that will be established to a remote cluster. For instance if there is only a single
         * seed node, other nodes will be discovered up to the given number of nodes in this setting. The default is 3.
         */
        public static final Setting.AffixSetting<Integer> REMOTE_NODE_CONNECTIONS = Setting.affixKeySetting(
            "cluster.remote.",
            "node_connections",
            (ns, key) -> intSetting(
                key,
                REMOTE_CONNECTIONS_PER_CLUSTER,
                1,
                new StrategyValidator<>(ns, key, RemoteConnectionStrategy.ConnectionStrategy.SNIFF),
                Setting.Property.Dynamic,
                Setting.Property.NodeScope
            )
        );

        static Stream<Setting.AffixSetting<?>> enablementSettings() {
            return Stream.of(REMOTE_CLUSTER_SEEDS);
        }

        static void readSettings(String clusterAlias, Settings settings, LinkedProjectConfig.Builder builder) {
            builder.sniffNodePredicate(getNodePredicate(settings))
                .sniffSeedNodes(REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).get(settings))
                .proxyAddress(REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(clusterAlias).get(settings))
                .sniffMaxNumConnections(REMOTE_NODE_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(settings));
        }

        static Predicate<DiscoveryNode> getNodePredicate(Settings settings) {
            if (REMOTE_NODE_ATTRIBUTE.exists(settings)) {
                // nodes can be tagged with node.attr.remote_gateway: true to allow a node to be a gateway node for cross cluster search
                String attribute = REMOTE_NODE_ATTRIBUTE.get(settings);
                return LinkedProjectConfig.DEFAULT_SNIFF_NODE_PREDICATE.and(
                    (node) -> Booleans.parseBoolean(node.getAttributes().getOrDefault(attribute, "false"))
                );
            }
            return LinkedProjectConfig.DEFAULT_SNIFF_NODE_PREDICATE;
        }
    }

    public static Set<String> getRemoteClusters(Settings settings) {
        return Arrays.stream(ConnectionStrategy.values())
            .flatMap(RemoteClusterSettings::getEnablementSettings)
            .flatMap(s -> getClusterAlias(settings, s))
            .collect(Collectors.toSet());
    }

    private static <T> Stream<String> getClusterAlias(Settings settings, Setting.AffixSetting<T> affixSetting) {
        Stream<Setting<T>> allConcreteSettings = affixSetting.getAllConcreteSettings(settings);
        return allConcreteSettings.map(affixSetting::getNamespace);
    }

    private static Stream<Setting.AffixSetting<?>> getEnablementSettings(ConnectionStrategy strategy) {
        return switch (strategy) {
            case SNIFF -> SniffConnectionStrategySettings.enablementSettings();
            case PROXY -> ProxyConnectionStrategySettings.enablementSettings();
        };
    }

    /**
     * Reads all settings values to create a fully populated {@link LinkedProjectConfig} instance.
     */
    public static LinkedProjectConfig toConfig(String clusterAlias, Settings settings) {
        return toConfigBuilder(clusterAlias, settings).build();
    }

    /**
     * Reads all settings values to create a fully populated {@link LinkedProjectConfig} instance for the given origin {@link ProjectId}.
     */
    public static LinkedProjectConfig toConfig(ProjectId originProjectId, String clusterAlias, Settings settings) {
        return toConfigBuilder(clusterAlias, settings).originProjectId(originProjectId).build();
    }

    public static LinkedProjectConfig.Builder toConfigBuilder(String clusterAlias, Settings settings) {
        final var builder = LinkedProjectConfig.buildForAlias(clusterAlias);
        readConnectionSettings(clusterAlias, settings, builder);
        readConnectionStrategySettings(clusterAlias, settings, builder);
        return builder;
    }

    public static void readConnectionSettings(String clusterAlias, Settings settings, LinkedProjectConfig.Builder builder) {
        builder.transportConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .connectionCompression(REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace(clusterAlias).get(settings))
            .connectionCompressionScheme(REMOTE_CLUSTER_COMPRESSION_SCHEME.getConcreteSettingForNamespace(clusterAlias).get(settings))
            .clusterPingSchedule(REMOTE_CLUSTER_PING_SCHEDULE.getConcreteSettingForNamespace(clusterAlias).get(settings))
            .initialConnectionTimeout(REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(settings))
            .maxPendingConnectionListeners(RemoteClusterSettings.REMOTE_MAX_PENDING_CONNECTION_LISTENERS.get(settings))
            .skipUnavailable(REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace(clusterAlias).get(settings));
    }

    private static void readConnectionStrategySettings(String clusterAlias, Settings settings, LinkedProjectConfig.Builder builder) {
        final var connectionStrategy = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).get(settings);
        builder.connectionStrategy(connectionStrategy);
        switch (connectionStrategy) {
            case SNIFF -> SniffConnectionStrategySettings.readSettings(clusterAlias, settings, builder);
            case PROXY -> ProxyConnectionStrategySettings.readSettings(clusterAlias, settings, builder);
        }
    }

    public static boolean isConnectionEnabled(String clusterAlias, Settings settings) {
        return toConfig(clusterAlias, settings).isConnectionEnabled();
    }

    private static class RemoteConnectionEnabled<T> implements Setting.Validator<T> {

        private final String clusterAlias;
        private final String key;

        private RemoteConnectionEnabled(String clusterAlias, String key) {
            this.clusterAlias = clusterAlias;
            this.key = key;
        }

        @Override
        public void validate(T value) {}

        @Override
        public void validate(T value, Map<Setting<?>, Object> settings, boolean isPresent) {
            if (isPresent && isConnectionEnabled(clusterAlias, settings) == false) {
                throw new IllegalArgumentException("Cannot configure setting [" + key + "] if remote cluster is not enabled.");
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return Stream.concat(Stream.of(REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias)), settingsStream())
                .iterator();
        }

        private Stream<Setting<?>> settingsStream() {
            return Arrays.stream(RemoteConnectionStrategy.ConnectionStrategy.values())
                .flatMap(RemoteClusterSettings::getEnablementSettings)
                .map(as -> as.getConcreteSettingForNamespace(clusterAlias));
        }

        @SuppressWarnings("unchecked")
        private static boolean isConnectionEnabled(String clusterAlias, Map<Setting<?>, Object> settings) {
            final var mode = (ConnectionStrategy) settings.get(REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias));
            final var builder = LinkedProjectConfig.buildForAlias(clusterAlias).connectionStrategy(mode);
            return switch (mode) {
                case SNIFF -> builder.sniffSeedNodes(
                    (List<String>) settings.get(
                        SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias)
                    )
                ).build().isConnectionEnabled();
                case PROXY -> builder.proxyAddress(
                    (String) settings.get(ProxyConnectionStrategySettings.PROXY_ADDRESS.getConcreteSettingForNamespace(clusterAlias))
                ).build().isConnectionEnabled();
            };
        }
    }

    private static class StrategyValidator<T> implements Setting.Validator<T> {

        private final String key;
        private final RemoteConnectionStrategy.ConnectionStrategy expectedStrategy;
        private final String namespace;
        private final Consumer<T> valueChecker;

        StrategyValidator(String namespace, String key, RemoteConnectionStrategy.ConnectionStrategy expectedStrategy) {
            this(namespace, key, expectedStrategy, (v) -> {});
        }

        StrategyValidator(
            String namespace,
            String key,
            RemoteConnectionStrategy.ConnectionStrategy expectedStrategy,
            Consumer<T> valueChecker
        ) {
            this.namespace = namespace;
            this.key = key;
            this.expectedStrategy = expectedStrategy;
            this.valueChecker = valueChecker;
        }

        @Override
        public void validate(T value) {
            valueChecker.accept(value);
        }

        @Override
        public void validate(T value, Map<Setting<?>, Object> settings, boolean isPresent) {
            Setting<RemoteConnectionStrategy.ConnectionStrategy> concrete = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(
                namespace
            );
            RemoteConnectionStrategy.ConnectionStrategy modeType = (RemoteConnectionStrategy.ConnectionStrategy) settings.get(concrete);
            if (isPresent && modeType.equals(expectedStrategy) == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Setting \"%s\" cannot be used with the configured \"%s\" [required=%s, configured=%s]",
                        key,
                        concrete.getKey(),
                        expectedStrategy.name(),
                        modeType.name()
                    )
                );
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            Setting<RemoteConnectionStrategy.ConnectionStrategy> concrete = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(
                namespace
            );
            Stream<Setting<?>> settingStream = Stream.of(concrete);
            return settingStream.iterator();
        }
    }
}
