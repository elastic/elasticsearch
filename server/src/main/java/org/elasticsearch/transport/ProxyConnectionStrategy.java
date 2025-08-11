/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.intSetting;

public class ProxyConnectionStrategy extends RemoteConnectionStrategy {

    /**
     * The remote address for the proxy. The connections will be opened to the configured address.
     */
    public static final Setting.AffixSetting<String> PROXY_ADDRESS = Setting.affixKeySetting(
        "cluster.remote.",
        "proxy_address",
        (ns, key) -> Setting.simpleString(key, new StrategyValidator<>(ns, key, ConnectionStrategy.PROXY, s -> {
            if (Strings.hasLength(s)) {
                parsePort(s);
            }
        }), Setting.Property.Dynamic, Setting.Property.NodeScope)
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
            new StrategyValidator<>(ns, key, ConnectionStrategy.PROXY),
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
            new StrategyValidator<>(ns, key, ConnectionStrategy.PROXY),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    static final int CHANNELS_PER_CONNECTION = 1;

    private static final int MAX_CONNECT_ATTEMPTS_PER_RUN = 3;

    private final int maxNumConnections;
    private final String configuredAddress;
    private final String configuredServerName;
    private final Supplier<TransportAddress> address;
    private final AtomicReference<ClusterName> remoteClusterName = new AtomicReference<>();
    private final ConnectionManager.ConnectionValidator clusterNameValidator;

    ProxyConnectionStrategy(
        String clusterAlias,
        TransportService transportService,
        RemoteConnectionManager connectionManager,
        Settings settings
    ) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            settings,
            REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(settings),
            PROXY_ADDRESS.getConcreteSettingForNamespace(clusterAlias).get(settings),
            SERVER_NAME.getConcreteSettingForNamespace(clusterAlias).get(settings)
        );
    }

    ProxyConnectionStrategy(
        String clusterAlias,
        TransportService transportService,
        RemoteConnectionManager connectionManager,
        Settings settings,
        int maxNumConnections,
        String configuredAddress
    ) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            settings,
            maxNumConnections,
            configuredAddress,
            () -> resolveAddress(configuredAddress),
            null
        );
    }

    ProxyConnectionStrategy(
        String clusterAlias,
        TransportService transportService,
        RemoteConnectionManager connectionManager,
        Settings settings,
        int maxNumConnections,
        String configuredAddress,
        String configuredServerName
    ) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            settings,
            maxNumConnections,
            configuredAddress,
            () -> resolveAddress(configuredAddress),
            configuredServerName
        );
    }

    ProxyConnectionStrategy(
        String clusterAlias,
        TransportService transportService,
        RemoteConnectionManager connectionManager,
        Settings settings,
        int maxNumConnections,
        String configuredAddress,
        Supplier<TransportAddress> address,
        String configuredServerName
    ) {
        super(clusterAlias, transportService, connectionManager, settings);
        this.maxNumConnections = maxNumConnections;
        this.configuredAddress = configuredAddress;
        this.configuredServerName = configuredServerName;
        assert Strings.isEmpty(configuredAddress) == false : "Cannot use proxy connection strategy with no configured addresses";
        this.address = address;
        this.clusterNameValidator = (newConnection, actualProfile, listener) -> transportService.handshake(
            newConnection,
            actualProfile.getHandshakeTimeout(),
            cn -> true,
            listener.map(resp -> {
                ClusterName remote = resp.getClusterName();
                if (remoteClusterName.compareAndSet(null, remote)) {
                    return null;
                } else {
                    if (remoteClusterName.get().equals(remote) == false) {
                        DiscoveryNode node = newConnection.getNode();
                        throw new ConnectTransportException(node, "handshake failed. unexpected remote cluster name " + remote);
                    }
                    return null;
                }
            })
        );
    }

    static Stream<Setting.AffixSetting<?>> enablementSettings() {
        return Stream.of(ProxyConnectionStrategy.PROXY_ADDRESS);
    }

    static Writeable.Reader<RemoteConnectionInfo.ModeInfo> infoReader() {
        return ProxyModeInfo::new;
    }

    @Override
    protected boolean shouldOpenMoreConnections() {
        return connectionManager.size() < maxNumConnections;
    }

    @Override
    protected boolean strategyMustBeRebuilt(Settings newSettings) {
        String address = PROXY_ADDRESS.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        int numOfSockets = REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        String serverName = SERVER_NAME.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        return numOfSockets != maxNumConnections
            || configuredAddress.equals(address) == false
            || Objects.equals(serverName, configuredServerName) == false;
    }

    @Override
    protected ConnectionStrategy strategyType() {
        return ConnectionStrategy.PROXY;
    }

    @Override
    protected void connectImpl(ActionListener<Void> listener) {
        performProxyConnectionProcess(listener);
    }

    @Override
    public RemoteConnectionInfo.ModeInfo getModeInfo() {
        return new ProxyModeInfo(configuredAddress, configuredServerName, maxNumConnections, connectionManager.size());
    }

    private void performProxyConnectionProcess(ActionListener<Void> listener) {
        openConnections(listener, 1);
    }

    private void openConnections(ActionListener<Void> finished, int attemptNumber) {
        if (attemptNumber <= MAX_CONNECT_ATTEMPTS_PER_RUN) {
            TransportAddress resolved = address.get();

            int remaining = maxNumConnections - connectionManager.size();
            ActionListener<Void> compositeListener = new ActionListener<Void>() {

                private final AtomicInteger successfulConnections = new AtomicInteger(0);
                private final CountDown countDown = new CountDown(remaining);

                @Override
                public void onResponse(Void v) {
                    successfulConnections.incrementAndGet();
                    if (countDown.countDown()) {
                        if (shouldOpenMoreConnections()) {
                            openConnections(finished, attemptNumber + 1);
                        } else {
                            finished.onResponse(v);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (countDown.countDown()) {
                        openConnections(finished, attemptNumber + 1);
                    }
                }
            };

            for (int i = 0; i < remaining; ++i) {
                String id = clusterAlias + "#" + resolved;
                Map<String, String> attributes;
                if (Strings.isNullOrEmpty(configuredServerName)) {
                    attributes = Collections.emptyMap();
                } else {
                    attributes = Collections.singletonMap("server_name", configuredServerName);
                }
                DiscoveryNode node = new DiscoveryNode(
                    id,
                    resolved,
                    attributes,
                    DiscoveryNodeRole.BUILT_IN_ROLES,
                    Version.CURRENT.minimumCompatibilityVersion()
                );

                connectionManager.connectToRemoteClusterNode(node, clusterNameValidator, compositeListener.delegateResponse((l, e) -> {
                    logger.debug(
                        new ParameterizedMessage(
                            "failed to open remote connection [remote cluster: {}, address: {}]",
                            clusterAlias,
                            resolved
                        ),
                        e
                    );
                    l.onFailure(e);
                }));
            }
        } else {
            int openConnections = connectionManager.size();
            if (openConnections == 0) {
                finished.onFailure(new NoSeedNodeLeftException(strategyType(), clusterAlias));
            } else {
                logger.debug(
                    "unable to open maximum number of connections [remote cluster: {}, opened: {}, maximum: {}]",
                    clusterAlias,
                    openConnections,
                    maxNumConnections
                );
                finished.onResponse(null);
            }
        }
    }

    private static TransportAddress resolveAddress(String address) {
        return new TransportAddress(parseConfiguredAddress(address));
    }

    public static class ProxyModeInfo implements RemoteConnectionInfo.ModeInfo {

        private final String address;
        private final String serverName;
        private final int maxSocketConnections;
        private final int numSocketsConnected;

        public ProxyModeInfo(String address, String serverName, int maxSocketConnections, int numSocketsConnected) {
            this.address = address;
            this.serverName = serverName;
            this.maxSocketConnections = maxSocketConnections;
            this.numSocketsConnected = numSocketsConnected;
        }

        private ProxyModeInfo(StreamInput input) throws IOException {
            address = input.readString();
            if (input.getVersion().onOrAfter(Version.V_7_7_0)) {
                serverName = input.readString();
            } else {
                serverName = null;
            }
            maxSocketConnections = input.readVInt();
            numSocketsConnected = input.readVInt();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("proxy_address", address);
            builder.field("server_name", serverName);
            builder.field("num_proxy_sockets_connected", numSocketsConnected);
            builder.field("max_proxy_socket_connections", maxSocketConnections);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(address);
            if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
                out.writeString(serverName);
            }
            out.writeVInt(maxSocketConnections);
            out.writeVInt(numSocketsConnected);
        }

        @Override
        public boolean isConnected() {
            return numSocketsConnected > 0;
        }

        @Override
        public String modeName() {
            return "proxy";
        }

        public String getAddress() {
            return address;
        }

        public String getServerName() {
            return serverName;
        }

        public int getMaxSocketConnections() {
            return maxSocketConnections;
        }

        public int getNumSocketsConnected() {
            return numSocketsConnected;
        }

        @Override
        public RemoteConnectionStrategy.ConnectionStrategy modeType() {
            return RemoteConnectionStrategy.ConnectionStrategy.PROXY;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProxyModeInfo otherProxy = (ProxyModeInfo) o;
            return maxSocketConnections == otherProxy.maxSocketConnections
                && numSocketsConnected == otherProxy.numSocketsConnected
                && Objects.equals(address, otherProxy.address)
                && Objects.equals(serverName, otherProxy.serverName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, serverName, maxSocketConnections, numSocketsConnected);
        }
    }
}
