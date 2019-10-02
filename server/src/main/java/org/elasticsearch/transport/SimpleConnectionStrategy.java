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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SimpleConnectionStrategy extends RemoteConnectionStrategy {

    private static final Logger logger = LogManager.getLogger(SimpleConnectionStrategy.class);

    private final int maxNumRemoteConnections;
    private final AtomicLong counter = new AtomicLong();
    private final List<Supplier<TransportAddress>> addresses;
    private final SetOnce<ClusterName> remoteClusterName = new SetOnce<>();

    SimpleConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                             int maxNumRemoteConnections, List<Supplier<TransportAddress>> addresses) {
        super(clusterAlias, transportService, connectionManager);
        this.maxNumRemoteConnections = maxNumRemoteConnections;
        this.addresses = addresses;
    }

    @Override
    protected boolean shouldOpenMoreConnections() {
        return connectionManager.size() < maxNumRemoteConnections;
    }

    @Override
    protected void connectImpl(ActionListener<Void> listener) {
        performSimpleConnectionProcess(addresses.iterator(), listener);
    }

    private void performSimpleConnectionProcess(Iterator<Supplier<TransportAddress>> addressIter, ActionListener<Void> listener) {
        final Consumer<Exception> onFailure = e -> {
            if (e instanceof ConnectTransportException || e instanceof IOException || e instanceof IllegalStateException) {
                // ISE if we fail the handshake with an version incompatible node
                if (addressIter.hasNext()) {
                    logger.debug(() -> new ParameterizedMessage(
                        "handshaking with external cluster [{}] failed moving to next address", clusterAlias), e);
                    performSimpleConnectionProcess(addressIter, listener);
                    return;
                }
            }
            logger.warn(() -> new ParameterizedMessage("handshaking with external cluster [{}] failed", clusterAlias), e);
            listener.onFailure(e);
        };


        final StepListener<Void> handshakeStep = new StepListener<>();

        if (remoteClusterName.get() == null) {
            final StepListener<Transport.Connection> openConnectionStep = new StepListener<>();
            final ConnectionProfile profile = ConnectionProfile.buildSingleChannelProfile(TransportRequestOptions.Type.REG);
            TransportAddress address = addressIter.next().get();
            DiscoveryNode handshakeNode = new DiscoveryNode(clusterAlias + "#" + address, address,
                Version.CURRENT.minimumCompatibilityVersion());
            connectionManager.openConnection(handshakeNode, profile, openConnectionStep);

            openConnectionStep.whenComplete(connection -> {
                ConnectionProfile connectionProfile = connectionManager.getConnectionManager().getConnectionProfile();
                transportService.handshake(connection, connectionProfile.getHandshakeTimeout().millis(),
                    getRemoteClusterNamePredicate(remoteClusterName), new ActionListener<>() {
                        @Override
                        public void onResponse(TransportService.HandshakeResponse handshakeResponse) {
                            IOUtils.closeWhileHandlingException(connection);
                            handshakeStep.onResponse(null);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            IOUtils.closeWhileHandlingException(connection);
                            handshakeStep.onFailure(e);
                        }
                    });
            }, onFailure);
        } else {
            handshakeStep.onResponse(null);
        }

        handshakeStep.whenComplete(v -> openConnections(listener, 1), onFailure);

    }

    private void openConnections(ActionListener<Void> finished, int attemptNumber) {
        if (attemptNumber <= 3) {
            List<TransportAddress> resolved = addresses.stream().map(Supplier::get).collect(Collectors.toList());

            int remaining = maxNumRemoteConnections - connectionManager.size();
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
                TransportAddress address = nextAddress(resolved);
                DiscoveryNode node = new DiscoveryNode(clusterAlias + "#" + address, address, Version.CURRENT.minimumCompatibilityVersion());
                connectionManager.connectToNode(node, null, (connection, profile, listener1) -> listener1.onResponse(null),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Void v) {
                            compositeListener.onResponse(v);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.debug(() -> new ParameterizedMessage("failed to open remote connection to address {}", address), e);
                            compositeListener.onFailure(e);
                        }
                    });
            }
        } else {
            if (connectionManager.size() == 0) {
                finished.onFailure(new IllegalStateException("Unable to open any simple connections to remote cluster"));
            } else {
                finished.onResponse(null);
            }
        }
    }

    private TransportAddress nextAddress(List<TransportAddress> resolvedAddresses) {
        long curr;
        while ((curr = counter.incrementAndGet()) == Long.MIN_VALUE) ;
        return resolvedAddresses.get(Math.floorMod(curr, resolvedAddresses.size()));
    }
}
