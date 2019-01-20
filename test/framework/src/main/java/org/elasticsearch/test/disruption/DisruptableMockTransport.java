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
package org.elasticsearch.test.disruption;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.test.ESTestCase.copyWriteable;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;

public abstract class DisruptableMockTransport extends MockTransport {
    private final DiscoveryNode localNode;
    private final Logger logger;

    public DisruptableMockTransport(DiscoveryNode localNode, Logger logger) {
        this.localNode = localNode;
        this.logger = logger;
    }

    protected abstract ConnectionStatus getConnectionStatus(DiscoveryNode destination);

    protected abstract Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address);

    protected abstract void execute(Runnable runnable);

    protected final void execute(String action, Runnable runnable) {
        // handshake needs to run inline as the caller blockingly waits on the result
        if (action.equals(HANDSHAKE_ACTION_NAME)) {
            runnable.run();
        } else {
            execute(runnable);
        }
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    @Override
    public TransportService createTransportService(Settings settings, ThreadPool threadPool, TransportInterceptor interceptor,
                                                   Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                                   @Nullable ClusterSettings clusterSettings, Set<String> taskHeaders) {
        return new TransportService(settings, this, threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders);
    }

    @Override
    public Releasable openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> listener) {
        final Optional<DisruptableMockTransport> matchingTransport = getDisruptableMockTransport(node.getAddress());
        if (matchingTransport.isPresent()) {
            listener.onResponse(new CloseableConnection() {
                @Override
                public DiscoveryNode getNode() {
                    return node;
                }

                @Override
                public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                    throws TransportException {
                    onSendRequest(requestId, action, request, matchingTransport.get());
                }
            });
            return () -> {};
        } else {
            throw new ConnectTransportException(node, "node " + node + " does not exist");
        }
    }

    protected void onSendRequest(long requestId, String action, TransportRequest request,
                                 DisruptableMockTransport destinationTransport) {

        assert destinationTransport.getLocalNode().equals(getLocalNode()) == false :
            "non-local message from " + getLocalNode() + " to itself";

        execute(action, new Runnable() {
            @Override
            public void run() {
                switch (getConnectionStatus(destinationTransport.getLocalNode())) {
                    case BLACK_HOLE:
                        onBlackholedDuringSend(requestId, action, destinationTransport);
                        break;

                    case DISCONNECTED:
                        onDisconnectedDuringSend(requestId, action, destinationTransport);
                        break;

                    case CONNECTED:
                        onConnectedDuringSend(requestId, action, request, destinationTransport);
                        break;
                }
            }

            @Override
            public String toString() {
                return getRequestDescription(requestId, action, destinationTransport.getLocalNode());
            }
        });
    }

    protected Runnable getDisconnectException(long requestId, String action, DiscoveryNode destination) {
        return new Runnable() {
            @Override
            public void run() {
                handleError(requestId, new ConnectTransportException(destination, "disconnected"));
            }

            @Override
            public String toString() {
                return "disconnection response to " + getRequestDescription(requestId, action, destination);
            }
        };
    }

    protected String getRequestDescription(long requestId, String action, DiscoveryNode destination) {
        return new ParameterizedMessage("[{}][{}] from {} to {}",
            requestId, action, getLocalNode(), destination).getFormattedMessage();
    }

    protected void onBlackholedDuringSend(long requestId, String action, DisruptableMockTransport destinationTransport) {
        if (action.equals(HANDSHAKE_ACTION_NAME)) {
            logger.trace("ignoring blackhole and delivering {}",
                getRequestDescription(requestId, action, destinationTransport.getLocalNode()));
            // handshakes always have a timeout, and are sent in a blocking fashion, so we must respond with an exception.
            destinationTransport.execute(action, getDisconnectException(requestId, action, destinationTransport.getLocalNode()));
        } else {
            logger.trace("dropping {}", getRequestDescription(requestId, action, destinationTransport.getLocalNode()));
        }
    }

    protected void onDisconnectedDuringSend(long requestId, String action, DisruptableMockTransport destinationTransport) {
        destinationTransport.execute(action, getDisconnectException(requestId, action, destinationTransport.getLocalNode()));
    }

    protected void onConnectedDuringSend(long requestId, String action, TransportRequest request,
                                         DisruptableMockTransport destinationTransport) {
        final RequestHandlerRegistry<TransportRequest> requestHandler =
            destinationTransport.getRequestHandler(action);

        final DiscoveryNode destination = destinationTransport.getLocalNode();

        final String requestDescription = getRequestDescription(requestId, action, destination);

        final TransportChannel transportChannel = new TransportChannel() {
            @Override
            public String getProfileName() {
                return "default";
            }

            @Override
            public String getChannelType() {
                return "disruptable-mock-transport-channel";
            }

            @Override
            public void sendResponse(final TransportResponse response) {
                execute(action, new Runnable() {
                    @Override
                    public void run() {
                        if (destinationTransport.getConnectionStatus(getLocalNode()) != ConnectionStatus.CONNECTED) {
                            logger.trace("dropping response to {}: channel is not CONNECTED",
                                requestDescription);
                        } else {
                            handleResponse(requestId, response);
                        }
                    }

                    @Override
                    public String toString() {
                        return "response to " + requestDescription;
                    }
                });
            }

            @Override
            public void sendResponse(Exception exception) {
                execute(action, new Runnable() {
                    @Override
                    public void run() {
                        if (destinationTransport.getConnectionStatus(getLocalNode()) != ConnectionStatus.CONNECTED) {
                            logger.trace("dropping response to {}: channel is not CONNECTED",
                                requestDescription);
                        } else {
                            handleRemoteError(requestId, exception);
                        }
                    }

                    @Override
                    public String toString() {
                        return "error response to " + requestDescription;
                    }
                });
            }
        };

        final TransportRequest copiedRequest;
        try {
            copiedRequest = copyWriteable(request, writeableRegistry(), requestHandler::newRequest);
        } catch (IOException e) {
            throw new AssertionError("exception de/serializing request", e);
        }

        try {
            requestHandler.processMessageReceived(copiedRequest, transportChannel);
        } catch (Exception e) {
            try {
                transportChannel.sendResponse(e);
            } catch (Exception ee) {
                logger.warn("failed to send failure", e);
            }
        }
    }

    private NamedWriteableRegistry writeableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    public enum ConnectionStatus {
        CONNECTED,
        DISCONNECTED, // network requests to or from this node throw a ConnectTransportException
        BLACK_HOLE // network traffic to or from the corresponding node is silently discarded
    }
}
