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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.intSetting;

public class SimpleConnectionStrategy extends RemoteConnectionStrategy {

    /**
     * A list of addresses for remote cluster connections. The connections will be opened to the configured addresses in a round-robin
     * fashion.
     */
    public static final Setting.AffixSetting<List<String>> REMOTE_CLUSTER_ADDRESSES = Setting.affixKeySetting(
        "cluster.remote.",
        "addresses",
        key -> Setting.listSetting(key, Collections.emptyList(), s -> {
                // validate address
                parsePort(s);
                return s;
            }, Setting.Property.Dynamic, Setting.Property.NodeScope));

    /**
     * The maximum number of socket connections that will be established to a remote cluster. The default is 18.
     */
    public static final Setting.AffixSetting<Integer> REMOTE_SOCKET_CONNECTIONS = Setting.affixKeySetting(
        "cluster.remote.",
        "simple.socket_connections",
        key -> intSetting(key, 18, 1, Setting.Property.Dynamic, Setting.Property.NodeScope));

    static final int CHANNELS_PER_CONNECTION = 1;

    private static final int MAX_CONNECT_ATTEMPTS_PER_RUN = 3;
    private static final Logger logger = LogManager.getLogger(SimpleConnectionStrategy.class);

    private final int maxNumConnections;
    private final AtomicLong counter = new AtomicLong(0);
    private final List<Supplier<TransportAddress>> addresses;
    private final AtomicReference<ClusterName> remoteClusterName = new AtomicReference<>();
    private final ConnectionProfile profile;
    private final ConnectionManager.ConnectionValidator clusterNameValidator;

    SimpleConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                            Settings settings) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(settings),
            REMOTE_CLUSTER_ADDRESSES.getConcreteSettingForNamespace(clusterAlias).get(settings));
    }

    SimpleConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                             int maxNumConnections, List<String> configuredAddresses) {
        this(clusterAlias, transportService, connectionManager, maxNumConnections, configuredAddresses,
            configuredAddresses.stream().map(address ->
                (Supplier<TransportAddress>) () -> resolveAddress(address)).collect(Collectors.toList()));
    }

    SimpleConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                             int maxNumConnections, List<String> configuredAddresses, List<Supplier<TransportAddress>> addresses) {
        super(clusterAlias, transportService, connectionManager);
        this.maxNumConnections = maxNumConnections;
        assert addresses.isEmpty() == false : "Cannot use simple connection strategy with no configured addresses";
        this.addresses = addresses;
        // TODO: Move into the ConnectionManager
        this.profile = new ConnectionProfile.Builder()
            .addConnections(1, TransportRequestOptions.Type.REG, TransportRequestOptions.Type.PING)
            .addConnections(0, TransportRequestOptions.Type.BULK, TransportRequestOptions.Type.STATE, TransportRequestOptions.Type.RECOVERY)
            .build();
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
        return Stream.of(SimpleConnectionStrategy.REMOTE_CLUSTER_ADDRESSES);
    }

    @Override
    protected boolean shouldOpenMoreConnections() {
        return connectionManager.size() < maxNumConnections;
    }

    @Override
    protected boolean strategyMustBeRebuilt(Settings newSettings) {
        return false;
    }

    @Override
    protected ConnectionStrategy strategyType() {
        return ConnectionStrategy.SIMPLE;
    }

    @Override
    protected void connectImpl(ActionListener<Void> listener) {
        performSimpleConnectionProcess(listener);
    }

    private void performSimpleConnectionProcess(ActionListener<Void> listener) {
        openConnections(listener, 1);
    }

    private void openConnections(ActionListener<Void> finished, int attemptNumber) {
        if (attemptNumber <= MAX_CONNECT_ATTEMPTS_PER_RUN) {
            List<TransportAddress> resolved = addresses.stream().map(Supplier::get).collect(Collectors.toList());

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
                TransportAddress address = nextAddress(resolved);
                String id = clusterAlias + "#" + address;
                DiscoveryNode node = new DiscoveryNode(id, address, Version.CURRENT.minimumCompatibilityVersion());

                connectionManager.connectToNode(node, profile, clusterNameValidator, new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void v) {
                        compositeListener.onResponse(v);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(new ParameterizedMessage("failed to open remote connection [remote cluster: {}, address: {}]",
                            clusterAlias, address), e);
                        compositeListener.onFailure(e);
                    }
                });
            }
        } else {
            int openConnections = connectionManager.size();
            if (openConnections == 0) {
                finished.onFailure(new IllegalStateException("Unable to open any simple connections to remote cluster [" + clusterAlias
                    + "]"));
            } else {
                logger.debug("unable to open maximum number of connections [remote cluster: {}, opened: {}, maximum: {}]", clusterAlias,
                    openConnections, maxNumConnections);
                finished.onResponse(null);
            }
        }
    }

    private TransportAddress nextAddress(List<TransportAddress> resolvedAddresses) {
        long curr;
        while ((curr = counter.getAndIncrement()) == Long.MIN_VALUE) ;
        return resolvedAddresses.get(Math.toIntExact(Math.floorMod(curr, (long) resolvedAddresses.size())));
    }

    private static TransportAddress resolveAddress(String address) {
        return new TransportAddress(parseSeedAddress(address));
    }
}
