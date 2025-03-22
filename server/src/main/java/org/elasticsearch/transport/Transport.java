/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.elasticsearch.transport.BytesRefRecycler.NON_RECYCLING_INSTANCE;

public interface Transport extends LifecycleComponent {

    /**
     * Registers a new request handler
     */
    default <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        getRequestHandlers().registerHandler(reg);
    }

    void setMessageListener(TransportMessageListener listener);

    default void setSlowLogThreshold(TimeValue slowLogThreshold) {}

    default boolean isSecure() {
        return false;
    }

    /**
     * The address the transport is bound on.
     */
    BoundTransportAddress boundAddress();

    /**
     * The address the Remote Access port is bound on, or <code>null</code> if it is not bound.
     */
    BoundTransportAddress boundRemoteIngressAddress();

    /**
     * Further profile bound addresses
     * @return <code>null</code> iff profiles are unsupported, otherwise a map with name of profile and its bound transport address
     */
    Map<String, BoundTransportAddress> profileBoundAddresses();

    /**
     * Returns an address from its string representation.
     */
    TransportAddress[] addressesFromString(String address) throws UnknownHostException;

    /**
     * Returns a list of all local addresses for this transport
     */
    List<String> getDefaultSeedAddresses();

    /**
     * Opens a new connection to the given node. When the connection is fully connected, the listener is called.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     */
    void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Transport.Connection> listener);

    TransportStats getStats();

    ResponseHandlers getResponseHandlers();

    RequestHandlers getRequestHandlers();

    default RecyclerBytesStreamOutput newNetworkBytesStream() {
        return new RecyclerBytesStreamOutput(NON_RECYCLING_INSTANCE);
    }

    /**
     * A unidirectional connection to a {@link DiscoveryNode}
     */
    interface Connection extends Closeable, RefCounted {
        /**
         * The node this connection is associated with
         */
        DiscoveryNode getNode();

        /**
         * Sends the request to the node this connection is associated with
         * @param requestId see {@link ResponseHandlers#add(TransportResponseHandler, Connection, String)} for details
         * @param action the action to execute
         * @param request the request to send
         * @param options request options to apply
         * @throws NodeNotConnectedException if the given node is not connected
         */
        void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException,
            TransportException;

        /**
         * The listener's {@link ActionListener#onResponse(Object)} method will be called when this
         * connection is closed. No implementations currently throw an exception during close, so
         * {@link ActionListener#onFailure(Exception)} will not be called.
         *
         * @param listener to be called
         */
        void addCloseListener(ActionListener<Void> listener);

        boolean isClosed();

        /**
         * Returns the version of the data to communicate in this channel.
         */
        TransportVersion getTransportVersion();

        /**
         * Returns a key that this connection can be cached on. Delegating subclasses must delegate method call to
         * the original connection.
         */
        default Object getCacheKey() {
            return this;
        }

        @Override
        void close();

        /**
         * Called after this connection is removed from the transport service.
         */
        void onRemoved();

        /**
         * Similar to {@link #addCloseListener} except that these listeners are notified once the connection is removed from the transport
         * service.
         */
        void addRemovedListener(ActionListener<Void> listener);
    }

    /**
     * This class represents a response context that encapsulates the actual response handler, the action. the connection it was
     * executed on, and the request ID.
     */
    record ResponseContext<T extends TransportResponse>(
        TransportResponseHandler<T> handler,
        Connection connection,
        String action,
        long requestId
    ) {};

    /**
     * This class is a registry that allows
     */
    final class ResponseHandlers {
        private final Map<Long, ResponseContext<? extends TransportResponse>> handlers = ConcurrentCollections
            .newConcurrentMapWithAggressiveConcurrency();
        private final AtomicLong requestIdGenerator = new AtomicLong();

        /**
         * Returns <code>true</code> if the give request ID has a context associated with it.
         */
        public boolean contains(long requestId) {
            return handlers.containsKey(requestId);
        }

        /**
         * Removes and return the {@link ResponseContext} for the given request ID or returns
         * <code>null</code> if no context is associated with this request ID.
         */
        public ResponseContext<? extends TransportResponse> remove(long requestId) {
            return handlers.remove(requestId);
        }

        /**
         * Adds a new response context and associates it with a new request ID.
         * @return the new response context
         * @see Connection#sendRequest(long, String, TransportRequest, TransportRequestOptions)
         */
        public ResponseContext<? extends TransportResponse> add(
            TransportResponseHandler<? extends TransportResponse> handler,
            Connection connection,
            String action
        ) {
            long requestId = newRequestId();
            ResponseContext<? extends TransportResponse> holder = new ResponseContext<>(handler, connection, action, requestId);
            ResponseContext<? extends TransportResponse> existing = handlers.put(requestId, holder);
            assert existing == null : "request ID already in use: " + requestId;
            return holder;
        }

        /**
         * Returns a new request ID to use when sending a message via {@link Connection#sendRequest(long, String,
         * TransportRequest, TransportRequestOptions)}
         */
        long newRequestId() {
            return requestIdGenerator.incrementAndGet();
        }

        /**
         * Removes and returns all {@link ResponseContext} instances that match the predicate
         */
        public List<ResponseContext<? extends TransportResponse>> prune(Predicate<ResponseContext<? extends TransportResponse>> predicate) {
            final List<ResponseContext<? extends TransportResponse>> holders = new ArrayList<>();
            for (Map.Entry<Long, ResponseContext<? extends TransportResponse>> entry : handlers.entrySet()) {
                ResponseContext<? extends TransportResponse> holder = entry.getValue();
                if (predicate.test(holder)) {
                    ResponseContext<? extends TransportResponse> remove = handlers.remove(entry.getKey());
                    if (remove != null) {
                        holders.add(holder);
                    }
                }
            }
            return holders;
        }

        /**
         * called by the {@link Transport} implementation when a response or an exception has been received for a previously
         * sent request (before any processing or deserialization was done). Returns the appropriate response handler or null if not
         * found.
         */
        public TransportResponseHandler<? extends TransportResponse> onResponseReceived(
            final long requestId,
            final TransportMessageListener listener
        ) {
            ResponseContext<? extends TransportResponse> context = handlers.remove(requestId);
            listener.onResponseReceived(requestId, context);
            if (context == null) {
                return null;
            } else {
                return context.handler();
            }
        }
    }

    final class RequestHandlers {

        private volatile Map<String, RequestHandlerRegistry<? extends TransportRequest>> requestHandlers = Collections.emptyMap();

        synchronized <Request extends TransportRequest> void registerHandler(RequestHandlerRegistry<Request> reg) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            requestHandlers = Maps.copyMapWithAddedEntry(requestHandlers, reg.getAction(), reg);
        }

        // TODO: Only visible for testing. Perhaps move StubbableTransport from
        // org.elasticsearch.test.transport to org.elasticsearch.transport
        public synchronized <Request extends TransportRequest> void forceRegister(RequestHandlerRegistry<Request> reg) {
            requestHandlers = Maps.copyMapWithAddedOrReplacedEntry(requestHandlers, reg.getAction(), reg);
        }

        @SuppressWarnings("unchecked")
        public <T extends TransportRequest> RequestHandlerRegistry<T> getHandler(String action) {
            return (RequestHandlerRegistry<T>) requestHandlers.get(action);
        }

        public Map<String, TransportActionStats> getStats() {
            return requestHandlers.values()
                .stream()
                .filter(reg -> reg.getStats().requestCount() > 0 || reg.getStats().responseCount() > 0)
                .collect(Maps.toUnmodifiableSortedMap(RequestHandlerRegistry::getAction, RequestHandlerRegistry::getStats));
        }
    }
}
