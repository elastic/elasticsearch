/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.timer.Timeout;
import org.elasticsearch.common.timer.TimerTask;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.timer.TimerService;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.*;

/**
 * @author kimchy (shay.banon)
 */
public class TransportService extends AbstractLifecycleComponent<TransportService> {

    private final Transport transport;

    private final ThreadPool threadPool;

    private final TimerService timerService;

    final ConcurrentMap<String, TransportRequestHandler> serverHandlers = newConcurrentMap();

    final ConcurrentMapLong<RequestHolder> clientHandlers = ConcurrentCollections.newConcurrentMapLong();

    final AtomicLong requestIds = new AtomicLong();

    final CopyOnWriteArrayList<TransportConnectionListener> connectionListeners = new CopyOnWriteArrayList<TransportConnectionListener>();

    final AtomicLong rxBytes = new AtomicLong();
    final AtomicLong rxCount = new AtomicLong();
    final AtomicLong txBytes = new AtomicLong();
    final AtomicLong txCount = new AtomicLong();

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers = Collections.synchronizedMap(new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 100;
        }
    });

    private boolean throwConnectException = false;

    public TransportService(Transport transport, ThreadPool threadPool, TimerService timerService) {
        this(EMPTY_SETTINGS, transport, threadPool, timerService);
    }

    @Inject public TransportService(Settings settings, Transport transport, ThreadPool threadPool, TimerService timerService) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;
        this.timerService = timerService;
    }

    @Override protected void doStart() throws ElasticSearchException {
        // register us as an adapter for the transport service
        transport.transportServiceAdapter(new Adapter());
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
    }

    @Override protected void doStop() throws ElasticSearchException {
        transport.stop();
    }

    @Override protected void doClose() throws ElasticSearchException {
        transport.close();
    }

    public boolean addressSupported(Class<? extends TransportAddress> address) {
        return transport.addressSupported(address);
    }

    public TransportInfo info() {
        return new TransportInfo(boundAddress());
    }

    public TransportStats stats() {
        return new TransportStats(rxCount.get(), rxBytes.get(), txCount.get(), txBytes.get());
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public boolean nodeConnected(DiscoveryNode node) {
        return transport.nodeConnected(node);
    }

    public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
        transport.connectToNode(node);
    }

    public void disconnectFromNode(DiscoveryNode node) {
        transport.disconnectFromNode(node);
    }

    public void addConnectionListener(TransportConnectionListener listener) {
        connectionListeners.add(listener);
    }

    public void removeConnectionListener(TransportConnectionListener listener) {
        connectionListeners.remove(listener);
    }

    /**
     * Set to <tt>true</tt> to indicate that a {@link ConnectTransportException} should be thrown when
     * sending a message (otherwise, it will be passed to the response handler). Defaults to <tt>false</tt>.
     *
     * <p>This is useful when logic based on connect failure is needed without having to wrap the handler,
     * for example, in case of retries across several nodes.
     */
    public void throwConnectException(boolean throwConnectException) {
        this.throwConnectException = throwConnectException;
    }

    public <T extends Streamable> TransportFuture<T> submitRequest(DiscoveryNode node, String action, Streamable message,
                                                                   TransportResponseHandler<T> handler) throws TransportException {
        return submitRequest(node, action, message, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends Streamable> TransportFuture<T> submitRequest(DiscoveryNode node, String action, Streamable message,
                                                                   TransportRequestOptions options, TransportResponseHandler<T> handler) throws TransportException {
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<T>(handler);
        sendRequest(node, action, message, options, futureHandler);
        return futureHandler;
    }

    public <T extends Streamable> void sendRequest(final DiscoveryNode node, final String action, final Streamable message,
                                                   final TransportResponseHandler<T> handler) throws TransportException {
        sendRequest(node, action, message, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends Streamable> void sendRequest(final DiscoveryNode node, final String action, final Streamable message,
                                                   final TransportRequestOptions options, final TransportResponseHandler<T> handler) throws TransportException {
        final long requestId = newRequestId();
        Timeout timeoutX = null;
        try {
            if (options.timeout() != null) {
                timeoutX = timerService.newTimeout(new TimeoutTimerTask(requestId), options.timeout(), TimerService.ExecutionType.THREADED);
            }
            clientHandlers.put(requestId, new RequestHolder<T>(handler, node, action, timeoutX));
            transport.sendRequest(node, requestId, action, message, options);
        } catch (final Exception e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            clientHandlers.remove(requestId);
            if (timeoutX != null) {
                timeoutX.cancel();
            }
            if (throwConnectException) {
                if (e instanceof ConnectTransportException) {
                    throw (ConnectTransportException) e;
                }
            }
            // callback that an exception happened, but on a different thread since we don't
            // want handlers to worry about stack overflows
            final SendRequestTransportException sendRequestException = new SendRequestTransportException(node, action, e);
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    handler.handleException(sendRequestException);
                }
            });
        }
    }

    private long newRequestId() {
        return requestIds.getAndIncrement();
    }

    public TransportAddress[] addressesFromString(String address) throws Exception {
        return transport.addressesFromString(address);
    }

    public void registerHandler(ActionTransportRequestHandler handler) {
        registerHandler(handler.action(), handler);
    }

    public void registerHandler(String action, TransportRequestHandler handler) {
        TransportRequestHandler handlerReplaced = serverHandlers.put(action, handler);
        if (handlerReplaced != null) {
            logger.warn("Registered two transport handlers for action {}, handlers: {}, {}", action, handler, handlerReplaced);
        }
    }

    public void removeHandler(String action) {
        serverHandlers.remove(action);
    }

    class Adapter implements TransportServiceAdapter {

        @Override public void received(long size) {
            rxCount.getAndIncrement();
            rxBytes.addAndGet(size);
        }

        @Override public void sent(long size) {
            txCount.getAndIncrement();
            txBytes.addAndGet(size);
        }

        @Override public TransportRequestHandler handler(String action) {
            return serverHandlers.get(action);
        }

        @Override public TransportResponseHandler remove(long requestId) {
            RequestHolder holder = clientHandlers.remove(requestId);
            if (holder == null) {
                // lets see if its in the timeout holder
                TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId);
                if (timeoutInfoHolder != null) {
                    logger.warn("Received response for a request that has timed out, action [{}], node [{}], id [{}]", timeoutInfoHolder.action(), timeoutInfoHolder.node(), requestId);
                } else {
                    logger.warn("Transport response handler not found of id [{}]", requestId);
                }
                return null;
            }
            if (holder.timeout() != null) {
                holder.timeout().cancel();
            }
            return holder.handler();
        }

        @Override public void raiseNodeConnected(final DiscoveryNode node) {
            threadPool.cached().execute(new Runnable() {
                @Override public void run() {
                    for (TransportConnectionListener connectionListener : connectionListeners) {
                        connectionListener.onNodeConnected(node);
                    }
                }
            });
        }

        @Override public void raiseNodeDisconnected(final DiscoveryNode node) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    for (TransportConnectionListener connectionListener : connectionListeners) {
                        connectionListener.onNodeDisconnected(node);
                    }
                    // node got disconnected, raise disconnection on possible ongoing handlers
                    for (Map.Entry<Long, RequestHolder> entry : clientHandlers.entrySet()) {
                        RequestHolder holder = entry.getValue();
                        if (holder.node().equals(node)) {
                            final RequestHolder holderToNotify = clientHandlers.remove(entry.getKey());
                            if (holderToNotify != null) {
                                // callback that an exception happened, but on a different thread since we don't
                                // want handlers to worry about stack overflows
                                threadPool.execute(new Runnable() {
                                    @Override public void run() {
                                        holderToNotify.handler().handleException(new NodeDisconnectedException(node, holderToNotify.action()));
                                    }
                                });
                            }
                        }
                    }
                }
            });
        }
    }

    class TimeoutTimerTask implements TimerTask {

        private final long requestId;

        TimeoutTimerTask(long requestId) {
            this.requestId = requestId;
        }

        @Override public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            final RequestHolder holder = clientHandlers.remove(requestId);
            if (holder != null) {
                // add it to the timeout information holder, in case we are going to get a response later
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(holder.node(), holder.action()));
                holder.handler().handleException(new ReceiveTimeoutTransportException(holder.node(), holder.action(), "request_id [" + requestId + "]"));
            }
        }
    }


    static class TimeoutInfoHolder {

        private final DiscoveryNode node;

        private final String action;

        TimeoutInfoHolder(DiscoveryNode node, String action) {
            this.node = node;
            this.action = action;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String action() {
            return action;
        }
    }

    static class RequestHolder<T extends Streamable> {

        private final TransportResponseHandler<T> handler;

        private final DiscoveryNode node;

        private final String action;

        private final Timeout timeout;

        RequestHolder(TransportResponseHandler<T> handler, DiscoveryNode node, String action, Timeout timeout) {
            this.handler = handler;
            this.node = node;
            this.action = action;
            this.timeout = timeout;
        }

        public TransportResponseHandler<T> handler() {
            return handler;
        }

        public DiscoveryNode node() {
            return this.node;
        }

        public String action() {
            return this.action;
        }

        public Timeout timeout() {
            return timeout;
        }
    }
}