/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.coordination.CleanableResponseHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.test.ESTestCase.copyWriteable;

public abstract class DisruptableMockTransport extends MockTransport {
    private final DiscoveryNode localNode;
    private static final Logger logger = LogManager.getLogger(DisruptableMockTransport.class);
    private final DeterministicTaskQueue deterministicTaskQueue;
    private final List<Runnable> blackholedRequests = new ArrayList<>();
    private final Set<String> blockedActions = new HashSet<>();

    public DisruptableMockTransport(DiscoveryNode localNode, DeterministicTaskQueue deterministicTaskQueue) {
        this.localNode = localNode;
        this.deterministicTaskQueue = deterministicTaskQueue;
    }

    protected abstract ConnectionStatus getConnectionStatus(DiscoveryNode destination);

    protected abstract Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address);

    protected abstract void execute(Runnable runnable);

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    @Override
    public TransportService createTransportService(
        Settings settings,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders
    ) {
        return new TransportService(
            settings,
            this,
            threadPool,
            interceptor,
            localNodeFactory,
            clusterSettings,
            new TaskManager(settings, threadPool, taskHeaders),
            Tracer.NOOP
        );
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> listener) {
        final Optional<DisruptableMockTransport> optionalMatchingTransport = getDisruptableMockTransport(node.getAddress());
        if (optionalMatchingTransport.isPresent()) {
            final DisruptableMockTransport matchingTransport = optionalMatchingTransport.get();
            final ConnectionStatus connectionStatus = getConnectionStatus(matchingTransport.getLocalNode());
            if (connectionStatus != ConnectionStatus.CONNECTED) {
                listener.onFailure(
                    new ConnectTransportException(node, "node [" + node + "] is [" + connectionStatus + "] not [CONNECTED]")
                );
            } else {
                listener.onResponse(new CloseableConnection() {
                    @Override
                    public DiscoveryNode getNode() {
                        return node;
                    }

                    @Override
                    public TransportVersion getTransportVersion() {
                        return TransportVersion.current();
                    }

                    @Override
                    public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                        throws TransportException {
                        if (blockedActions.contains(action)) {
                            execute(new RebootSensitiveRunnable() {
                                @Override
                                public void ifRebooted() {
                                    cleanupResponseHandler(requestId);
                                }

                                @Override
                                public void run() {
                                    handleError(
                                        requestId,
                                        new RemoteTransportException(
                                            node.getName(),
                                            node.getAddress(),
                                            action,
                                            new ElasticsearchException("action [" + action + "] is blocked")
                                        )
                                    );
                                }

                                @Override
                                public String toString() {
                                    return "error response delivery for blocked action [" + action + "] on node [" + node + "]";
                                }
                            });
                        } else {
                            onSendRequest(requestId, action, request, options, matchingTransport);
                        }
                    }
                });
            }
        } else {
            listener.onFailure(new ConnectTransportException(node, "node " + node + " does not exist"));
        }
    }

    protected void onSendRequest(
        long requestId,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        DisruptableMockTransport destinationTransport
    ) {
        assert destinationTransport.getLocalNode().equals(getLocalNode()) == false
            : "non-local message from " + getLocalNode() + " to itself";

        request.mustIncRef();

        destinationTransport.execute(new RebootSensitiveRunnable() {
            @Override
            public void run() {
                try {
                    final ConnectionStatus connectionStatus = getConnectionStatus(destinationTransport.getLocalNode());
                    switch (connectionStatus) {
                        case BLACK_HOLE, BLACK_HOLE_REQUESTS_ONLY -> onBlackholedDuringSend(requestId, action, destinationTransport);
                        case DISCONNECTED -> onDisconnectedDuringSend(requestId, action, destinationTransport);
                        case CONNECTED -> onConnectedDuringSend(requestId, action, request, destinationTransport);
                        default -> throw new AssertionError("unexpected status: " + connectionStatus);
                    }
                } finally {
                    request.decRef();
                }
            }

            @Override
            public void ifRebooted() {
                request.decRef();
                execute(new RebootSensitiveRunnable() {
                    @Override
                    public void ifRebooted() {
                        cleanupResponseHandler(requestId);
                    }

                    @Override
                    public void run() {
                        handleRemoteError(requestId, new NodeNotConnectedException(destinationTransport.getLocalNode(), "node rebooted"));
                    }

                    @Override
                    public String toString() {
                        return "error response (reboot) to " + internalToString();
                    }
                });
            }

            @Override
            public String toString() {
                return internalToString();
            }

            private String internalToString() {
                return getRequestDescription(requestId, action, destinationTransport.getLocalNode());
            }
        });
    }

    private Runnable getDisconnectException(long requestId, String action, DiscoveryNode destination) {
        return new RebootSensitiveRunnable() {
            @Override
            public void ifRebooted() {
                cleanupResponseHandler(requestId);
            }

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

    private String getRequestDescription(long requestId, String action, DiscoveryNode destination) {
        return format("[%s][%s] from %s to %s", requestId, action, getLocalNode(), destination);
    }

    private void onBlackholedDuringSend(long requestId, String action, DisruptableMockTransport destinationTransport) {
        logger.trace("dropping {}", getRequestDescription(requestId, action, destinationTransport.getLocalNode()));
        // Delaying the response until explicitly instructed, to simulate a very long delay
        blackholedRequests.add(new Runnable() {
            @Override
            public void run() {
                onDisconnectedDuringSend(requestId, action, destinationTransport);
            }

            @Override
            public String toString() {
                return "deferred handling of dropped " + getRequestDescription(requestId, action, destinationTransport.getLocalNode());
            }
        });
    }

    private void onDisconnectedDuringSend(long requestId, String action, DisruptableMockTransport destinationTransport) {
        execute(getDisconnectException(requestId, action, destinationTransport.getLocalNode()));
    }

    private void onConnectedDuringSend(
        long requestId,
        String action,
        TransportRequest request,
        DisruptableMockTransport destinationTransport
    ) {
        final RequestHandlerRegistry<TransportRequest> requestHandler = destinationTransport.getRequestHandlers().getHandler(action);

        final DiscoveryNode destination = destinationTransport.getLocalNode();

        final String requestDescription = getRequestDescription(requestId, action, destination);

        final TransportChannel transportChannel = new TransportChannel() {
            @Override
            public String getProfileName() {
                return "default";
            }

            @Override
            public void sendResponse(final TransportResponse response) {
                response.mustIncRef();
                final var releasable = Releasables.assertOnce(response::decRef);
                execute(new RebootSensitiveRunnable() {
                    @Override
                    public void ifRebooted() {
                        try (releasable) {
                            cleanupResponseHandler(requestId);
                        }
                    }

                    @Override
                    public void run() {
                        try (releasable) {
                            final ConnectionStatus connectionStatus = destinationTransport.getConnectionStatus(getLocalNode());
                            switch (connectionStatus) {
                                case CONNECTED, BLACK_HOLE_REQUESTS_ONLY -> handleResponse(requestId, response);
                                case BLACK_HOLE, DISCONNECTED -> {
                                    logger.trace("delaying response to {}: channel is {}", requestDescription, connectionStatus);
                                    onBlackholedDuringSend(requestId, action, destinationTransport);
                                }
                                default -> throw new AssertionError("unexpected status: " + connectionStatus);
                            }
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
                execute(new RebootSensitiveRunnable() {
                    @Override
                    public void ifRebooted() {
                        cleanupResponseHandler(requestId);
                    }

                    @Override
                    public void run() {
                        final ConnectionStatus connectionStatus = destinationTransport.getConnectionStatus(getLocalNode());
                        switch (connectionStatus) {
                            case CONNECTED, BLACK_HOLE_REQUESTS_ONLY -> handleRemoteError(requestId, exception);
                            case BLACK_HOLE, DISCONNECTED -> {
                                logger.trace("delaying exception response to {}: channel is {}", requestDescription, connectionStatus);
                                onBlackholedDuringSend(requestId, action, destinationTransport);
                            }
                            default -> throw new AssertionError("unexpected status: " + connectionStatus);
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
        } finally {
            copiedRequest.decRef();
        }
    }

    private void cleanupResponseHandler(long requestId) {
        TransportResponseHandler<?> handler = getTransportResponseHandler(requestId);
        while (handler instanceof TransportService.ContextRestoreResponseHandler<?> contextRestoreHandler) {
            handler = contextRestoreHandler.unwrap();
        }
        if (handler instanceof CleanableResponseHandler<?> cleanableResponseHandler) {
            cleanableResponseHandler.runCleanup();
        }
    }

    public boolean deliverBlackholedRequests() {
        if (blackholedRequests.isEmpty()) {
            return false;
        } else {
            blackholedRequests.forEach(deterministicTaskQueue::scheduleNow);
            blackholedRequests.clear();
            return true;
        }
    }

    public void addActionBlock(String action) {
        blockedActions.add(action);
    }

    public void clearActionBlocks() {
        blockedActions.clear();
    }

    /**
     * Response type from {@link DisruptableMockTransport#getConnectionStatus(DiscoveryNode)} indicating whether, and how, messages should
     * be disrupted on this transport.
     */
    public enum ConnectionStatus {
        /**
         * No disruption: deliver messages normally.
         */
        CONNECTED,

        /**
         * Simulate disconnection: inbound and outbound messages throw a {@link ConnectTransportException}.
         */
        DISCONNECTED,

        /**
         * Simulate a blackhole partition: inbound and outbound messages are silently discarded.
         */
        BLACK_HOLE,

        /**
         * Simulate an asymmetric partition: outbound messages are silently discarded, but inbound messages are delivered normally.
         */
        BLACK_HOLE_REQUESTS_ONLY
    }

    /**
     * When simulating sending requests to another node which might have rebooted, it's not realistic just to drop the action if the node
     * reboots; instead we need to simulate the error response that comes back.
     */
    public interface RebootSensitiveRunnable extends Runnable {
        /**
         * Cleanup action to run if the destination node reboots.
         */
        void ifRebooted();
    }
}
