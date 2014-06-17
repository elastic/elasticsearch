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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 *
 */
public class TransportService extends AbstractLifecycleComponent<TransportService> {

    protected final Transport transport;
    protected final ThreadPool threadPool;

    volatile ImmutableMap<String, TransportRequestHandler> serverHandlers = ImmutableMap.of();
    final Object serverHandlersMutex = new Object();

    final ConcurrentMapLong<RequestHolder> clientHandlers = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    final AtomicLong requestIds = new AtomicLong();

    final CopyOnWriteArrayList<TransportConnectionListener> connectionListeners = new CopyOnWriteArrayList<>();

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers = Collections.synchronizedMap(new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 100;
        }
    });

    private boolean throwConnectException = false;
    private final TransportService.Adapter adapter = new Adapter();

    public TransportService(Transport transport, ThreadPool threadPool) {
        this(EMPTY_SETTINGS, transport, threadPool);
    }

    @Inject
    public TransportService(Settings settings, Transport transport, ThreadPool threadPool) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        adapter.rxMetric.clear();
        adapter.txMetric.clear();
        transport.transportServiceAdapter(adapter);
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        try {
            transport.stop();
        } finally {
            // in case the transport is not connected to our local node (thus cleaned on node disconnect)
            // make sure to clean any leftover on going handles
            for (Map.Entry<Long, RequestHolder> entry : clientHandlers.entrySet()) {
                final RequestHolder holderToNotify = clientHandlers.remove(entry.getKey());
                if (holderToNotify != null) {
                    // callback that an exception happened, but on a different thread since we don't
                    // want handlers to worry about stack overflows
                    threadPool.generic().execute(new Runnable() {
                        @Override
                        public void run() {
                            holderToNotify.handler().handleException(new TransportException("transport stopped, action: " + holderToNotify.action()));
                        }
                    });
                }
            }
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        transport.close();
    }

    public boolean addressSupported(Class<? extends TransportAddress> address) {
        return transport.addressSupported(address);
    }

    public TransportInfo info() {
        return new TransportInfo(boundAddress());
    }

    public TransportStats stats() {
        return new TransportStats(transport.serverOpen(), adapter.rxMetric.count(), adapter.rxMetric.sum(), adapter.txMetric.count(), adapter.txMetric.sum());
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

    public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {
        transport.connectToNodeLight(node);
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
     * <p/>
     * <p>This is useful when logic based on connect failure is needed without having to wrap the handler,
     * for example, in case of retries across several nodes.
     */
    public void throwConnectException(boolean throwConnectException) {
        this.throwConnectException = throwConnectException;
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action, TransportRequest request,
                                                                          TransportResponseHandler<T> handler) throws TransportException {
        return submitRequest(node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action, TransportRequest request,
                                                                          TransportRequestOptions options, TransportResponseHandler<T> handler) throws TransportException {
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<>(handler);
        sendRequest(node, action, request, options, futureHandler);
        return futureHandler;
    }

    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action, final TransportRequest request,
                                                          final TransportResponseHandler<T> handler) throws TransportException {
        sendRequest(node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action, final TransportRequest request,
                                                          final TransportRequestOptions options, TransportResponseHandler<T> handler) throws TransportException {
        if (node == null) {
            throw new ElasticsearchIllegalStateException("can't send request to a null node");
        }
        final long requestId = newRequestId();
        TimeoutHandler timeoutHandler = null;
        try {
            if (options.timeout() != null) {
                timeoutHandler = new TimeoutHandler(requestId);
                timeoutHandler.future = threadPool.schedule(options.timeout(), ThreadPool.Names.GENERIC, timeoutHandler);
            }
            clientHandlers.put(requestId, new RequestHolder<>(handler, node, action, timeoutHandler));
            transport.sendRequest(node, requestId, action, request, options);
        } catch (final Throwable e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            final RequestHolder holderToNotify = clientHandlers.remove(requestId);
            if (timeoutHandler != null) {
                timeoutHandler.future.cancel(false);
            }

            // If holderToNotify == null then handler has already been taken care of.
            if (holderToNotify != null) {
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows
                final SendRequestTransportException sendRequestException = new SendRequestTransportException(node, action, e);
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                    @Override
                    public void run() {
                        holderToNotify.handler().handleException(sendRequestException);
                    }
                });
            }

            if (throwConnectException) {
                if (e instanceof ConnectTransportException) {
                    throw (ConnectTransportException) e;
                }
            }
        }
    }

    private long newRequestId() {
        return requestIds.getAndIncrement();
    }

    public TransportAddress[] addressesFromString(String address) throws Exception {
        return transport.addressesFromString(address);
    }

    public void registerHandler(String action, TransportRequestHandler handler) {
        synchronized (serverHandlersMutex) {
            TransportRequestHandler handlerReplaced = serverHandlers.get(action);
            serverHandlers = MapBuilder.newMapBuilder(serverHandlers).put(action, handler).immutableMap();
            if (handlerReplaced != null) {
                logger.warn("Registered two transport handlers for action {}, handlers: {}, {}", action, handler, handlerReplaced);
            }
        }
    }

    public void removeHandler(String action) {
        synchronized (serverHandlersMutex) {
            serverHandlers = MapBuilder.newMapBuilder(serverHandlers).remove(action).immutableMap();
        }
    }

    class Adapter implements TransportServiceAdapter {

        final MeanMetric rxMetric = new MeanMetric();
        final MeanMetric txMetric = new MeanMetric();

        @Override
        public void received(long size) {
            rxMetric.inc(size);
        }

        @Override
        public void sent(long size) {
            txMetric.inc(size);
        }

        @Override
        public TransportRequestHandler handler(String action) {
            return serverHandlers.get(action);
        }

        @Override
        public TransportResponseHandler remove(long requestId) {
            RequestHolder holder = clientHandlers.remove(requestId);
            if (holder == null) {
                // lets see if its in the timeout holder
                TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId);
                if (timeoutInfoHolder != null) {
                    long time = System.currentTimeMillis();
                    logger.warn("Received response for a request that has timed out, sent [{}ms] ago, timed out [{}ms] ago, action [{}], node [{}], id [{}]", time - timeoutInfoHolder.sentTime(), time - timeoutInfoHolder.timeoutTime(), timeoutInfoHolder.action(), timeoutInfoHolder.node(), requestId);
                } else {
                    logger.warn("Transport response handler not found of id [{}]", requestId);
                }
                return null;
            }
            holder.cancel();
            return holder.handler();
        }

        @Override
        public void raiseNodeConnected(final DiscoveryNode node) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    for (TransportConnectionListener connectionListener : connectionListeners) {
                        connectionListener.onNodeConnected(node);
                    }
                }
            });
        }

        @Override
        public void raiseNodeDisconnected(final DiscoveryNode node) {
            try {
                for (final TransportConnectionListener connectionListener : connectionListeners) {
                    threadPool.generic().execute(new Runnable() {
                        @Override
                        public void run() {
                            connectionListener.onNodeDisconnected(node);
                        }
                    });
                }
                for (Map.Entry<Long, RequestHolder> entry : clientHandlers.entrySet()) {
                    RequestHolder holder = entry.getValue();
                    if (holder.node().equals(node)) {
                        final RequestHolder holderToNotify = clientHandlers.remove(entry.getKey());
                        if (holderToNotify != null) {
                            // callback that an exception happened, but on a different thread since we don't
                            // want handlers to worry about stack overflows
                            threadPool.generic().execute(new Runnable() {
                                @Override
                                public void run() {
                                    holderToNotify.handler().handleException(new NodeDisconnectedException(node, holderToNotify.action()));
                                }
                            });
                        }
                    }
                }
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Rejected execution on NodeDisconnected", ex);
            }
        }
    }

    class TimeoutHandler implements Runnable {

        private final long requestId;

        private final long sentTime = System.currentTimeMillis();

        ScheduledFuture future;

        TimeoutHandler(long requestId) {
            this.requestId = requestId;
        }

        public long sentTime() {
            return sentTime;
        }

        @Override
        public void run() {
            if (future.isCancelled()) {
                return;
            }
            final RequestHolder holder = clientHandlers.remove(requestId);
            if (holder != null) {
                // add it to the timeout information holder, in case we are going to get a response later
                long timeoutTime = System.currentTimeMillis();
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(holder.node(), holder.action(), sentTime, timeoutTime));
                holder.handler().handleException(new ReceiveTimeoutTransportException(holder.node(), holder.action(), "request_id [" + requestId + "] timed out after [" + (timeoutTime - sentTime) + "ms]"));
            }
        }
    }


    static class TimeoutInfoHolder {

        private final DiscoveryNode node;

        private final String action;

        private final long sentTime;

        private final long timeoutTime;

        TimeoutInfoHolder(DiscoveryNode node, String action, long sentTime, long timeoutTime) {
            this.node = node;
            this.action = action;
            this.sentTime = sentTime;
            this.timeoutTime = timeoutTime;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String action() {
            return action;
        }

        public long sentTime() {
            return sentTime;
        }

        public long timeoutTime() {
            return timeoutTime;
        }
    }

    static class RequestHolder<T extends TransportResponse> {

        private final TransportResponseHandler<T> handler;

        private final DiscoveryNode node;

        private final String action;

        private final TimeoutHandler timeout;

        RequestHolder(TransportResponseHandler<T> handler, DiscoveryNode node, String action, TimeoutHandler timeout) {
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

        public void cancel() {
            if (timeout != null) {
                timeout.future.cancel(false);
            }
        }
    }
}