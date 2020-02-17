/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.xcontent.XContentBuilder;

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
    public static final Setting.AffixSetting<String> REMOTE_CLUSTER_ADDRESSES = Setting.affixKeySetting(
        "cluster.remote.",
        "proxy_address",
        (ns, key) -> Setting.simpleString(key, new StrategyValidator<>(ns, key, ConnectionStrategy.PROXY, s -> {
                if (Strings.hasLength(s)) {
                    parsePort(s);
                }
            }), Setting.Property.Dynamic, Setting.Property.NodeScope));

    /**
     * The maximum number of socket connections that will be established to a remote cluster. The default is 18.
     */
    public static final Setting.AffixSetting<Integer> REMOTE_SOCKET_CONNECTIONS = Setting.affixKeySetting(
        "cluster.remote.",
        "proxy_socket_connections",
        (ns, key) -> intSetting(key, 18, 1, new StrategyValidator<>(ns, key, ConnectionStrategy.PROXY),
            Setting.Property.Dynamic, Setting.Property.NodeScope));

    /**
     * A configurable server_name attribute
     */
    public static final Setting.AffixSetting<String> SERVER_NAME = Setting.affixKeySetting(
        "cluster.remote.",
        "server_name",
        (ns, key) -> Setting.simpleString(key, new StrategyValidator<>(ns, key, ConnectionStrategy.PROXY),
            Setting.Property.Dynamic, Setting.Property.NodeScope));

    static final int CHANNELS_PER_CONNECTION = 1;

    private static final int MAX_CONNECT_ATTEMPTS_PER_RUN = 3;
    private static final Logger logger = LogManager.getLogger(ProxyConnectionStrategy.class);

    private final int maxNumConnections;
    private final String configuredAddress;
    private final String configuredServerName;
    private final Supplier<TransportAddress> address;
    private final AtomicReference<ClusterName> remoteClusterName = new AtomicReference<>();
    private final ConnectionManager.ConnectionValidator clusterNameValidator;

    ProxyConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                            Settings settings) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(settings),
            REMOTE_CLUSTER_ADDRESSES.getConcreteSettingForNamespace(clusterAlias).get(settings),
            SERVER_NAME.getConcreteSettingForNamespace(clusterAlias).get(settings));
    }

    ProxyConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                            int maxNumConnections, String configuredAddress) {
        this(clusterAlias, transportService, connectionManager, maxNumConnections, configuredAddress,
            () -> resolveAddress(configuredAddress), null);
    }

    ProxyConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                            int maxNumConnections, String configuredAddress, String configuredServerName) {
        this(clusterAlias, transportService, connectionManager, maxNumConnections, configuredAddress,
            () -> resolveAddress(configuredAddress), configuredServerName);
    }

    ProxyConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                            int maxNumConnections, String configuredAddress, Supplier<TransportAddress> address,
                            String configuredServerName) {
        super(clusterAlias, transportService, connectionManager);
        this.maxNumConnections = maxNumConnections;
        this.configuredAddress = configuredAddress;
        this.configuredServerName = configuredServerName;
        assert Strings.isEmpty(configuredAddress) == false : "Cannot use proxy connection strategy with no configured addresses";
        this.address = address;
        this.clusterNameValidator = (newConnection, actualProfile, listener) ->
            transportService.handshake(newConnection, actualProfile.getHandshakeTimeout().millis(), cn -> true,
                ActionListener.map(listener, resp -> {
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
                }));
    }

    static Stream<Setting.AffixSetting<?>> enablementSettings() {
        return Stream.of(ProxyConnectionStrategy.REMOTE_CLUSTER_ADDRESSES);
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
        String address = REMOTE_CLUSTER_ADDRESSES.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        int numOfSockets = REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        return numOfSockets != maxNumConnections || configuredAddress.equals(address) == false;
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
        return new ProxyModeInfo(configuredAddress, maxNumConnections, connectionManager.size());
    }

    private void performProxyConnectionProcess(ActionListener<Void> listener) {
        openConnections(listener, 1);
    }

    private void openConnections(ActionListener<Void> finished, int attemptNumber) {
        if (attemptNumber <= MAX_CONNECT_ATTEMPTS_PER_RUN) {
            TransportAddress resolved = address.get();

            int remaining = maxNumConnections - connectionManager.size();
            ActionListener<Void> compositeListener = new ActionListener<>() {

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
                DiscoveryNode node = new DiscoveryNode(id, resolved, attributes, DiscoveryNodeRole.BUILT_IN_ROLES,
                    Version.CURRENT.minimumCompatibilityVersion());

                connectionManager.connectToNode(node, null, clusterNameValidator, new ActionListener<>() {
                    @Override
                    public void onResponse(Void v) {
                        compositeListener.onResponse(v);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(new ParameterizedMessage("failed to open remote connection [remote cluster: {}, address: {}]",
                            clusterAlias, resolved), e);
                        compositeListener.onFailure(e);
                    }
                });
            }
        } else {
            int openConnections = connectionManager.size();
            if (openConnections == 0) {
                finished.onFailure(new IllegalStateException("Unable to open any proxy connections to remote cluster [" + clusterAlias
                    + "]"));
            } else {
                logger.debug("unable to open maximum number of connections [remote cluster: {}, opened: {}, maximum: {}]", clusterAlias,
                    openConnections, maxNumConnections);
                finished.onResponse(null);
            }
        }
    }

    private static TransportAddress resolveAddress(String address) {
        return new TransportAddress(parseConfiguredAddress(address));
    }

    public static class ProxyModeInfo implements RemoteConnectionInfo.ModeInfo {

        private final String address;
        private final int maxSocketConnections;
        private final int numSocketsConnected;

        public ProxyModeInfo(String address, int maxSocketConnections, int numSocketsConnected) {
            this.address = address;
            this.maxSocketConnections = maxSocketConnections;
            this.numSocketsConnected = numSocketsConnected;
        }

        private ProxyModeInfo(StreamInput input) throws IOException {
            address = input.readString();
            maxSocketConnections = input.readVInt();
            numSocketsConnected = input.readVInt();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("address", address);
            builder.field("num_sockets_connected", numSocketsConnected);
            builder.field("max_socket_connections", maxSocketConnections);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(address);
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
            return maxSocketConnections == otherProxy.maxSocketConnections &&
                numSocketsConnected == otherProxy.numSocketsConnected &&
                Objects.equals(address, otherProxy.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, maxSocketConnections, numSocketsConnected);
        }
    }
}
