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
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Optional;

import static org.elasticsearch.test.ESTestCase.copyWriteable;

public abstract class DisruptableMockTransport extends MockTransport {
    private final Logger logger;

    public DisruptableMockTransport(Logger logger) {
        this.logger = logger;
    }

    protected abstract DiscoveryNode getLocalNode();

    protected abstract ConnectionStatus getConnectionStatus(DiscoveryNode sender, DiscoveryNode destination);

    protected abstract Optional<DisruptableMockTransport> getDisruptedCapturingTransport(DiscoveryNode node, String action);

    protected abstract void handle(DiscoveryNode sender, DiscoveryNode destination, String action, Runnable doDelivery);

    protected final void sendFromTo(DiscoveryNode sender, DiscoveryNode destination, String action, Runnable doDelivery) {
        handle(sender, destination, action, new Runnable() {
            @Override
            public void run() {
                if (getDisruptedCapturingTransport(destination, action).isPresent()) {
                    doDelivery.run();
                } else {
                    logger.trace("unknown destination in {}", this);
                }
            }

            @Override
            public String toString() {
                return doDelivery.toString();
            }
        });
    }

    @Override
    protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode destination) {

        assert destination.equals(getLocalNode()) == false : "non-local message from " + getLocalNode() + " to itself";

        sendFromTo(getLocalNode(), destination, action, new Runnable() {
            @Override
            public void run() {
                switch (getConnectionStatus(getLocalNode(), destination)) {
                    case BLACK_HOLE:
                        onBlackholedDuringSend(requestId, action, destination);
                        break;

                    case DISCONNECTED:
                        onDisconnectedDuringSend(requestId, action, destination);
                        break;

                    case CONNECTED:
                        onConnectedDuringSend(requestId, action, request, destination);
                        break;
                }
            }

            @Override
            public String toString() {
                return getRequestDescription(requestId, action, destination);
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

    protected void onBlackholedDuringSend(long requestId, String action, DiscoveryNode destination) {
        logger.trace("dropping {}", getRequestDescription(requestId, action, destination));
    }

    protected void onDisconnectedDuringSend(long requestId, String action, DiscoveryNode destination) {
        sendFromTo(destination, getLocalNode(), action, getDisconnectException(requestId, action, destination));
    }

    protected void onConnectedDuringSend(long requestId, String action, TransportRequest request, DiscoveryNode destination) {
        Optional<DisruptableMockTransport> destinationTransport = getDisruptedCapturingTransport(destination, action);
        assert destinationTransport.isPresent();

        final RequestHandlerRegistry<TransportRequest> requestHandler =
            destinationTransport.get().getRequestHandler(action);

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
                sendFromTo(destination, getLocalNode(), action, new Runnable() {
                    @Override
                    public void run() {
                        if (getConnectionStatus(destination, getLocalNode()) != ConnectionStatus.CONNECTED) {
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
                sendFromTo(destination, getLocalNode(), action, new Runnable() {
                    @Override
                    public void run() {
                        if (getConnectionStatus(destination, getLocalNode()) != ConnectionStatus.CONNECTED) {
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
