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

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.concurrent.highscalelib.NonBlockingHashMapLong;
import org.elasticsearch.util.io.stream.Streamable;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.transport.BoundTransportAddress;
import org.elasticsearch.util.transport.TransportAddress;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.util.concurrent.ConcurrentMaps.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;

/**
 * @author kimchy (shay.banon)
 */
public class TransportService extends AbstractLifecycleComponent<TransportService> {

    private final Transport transport;

    private final ThreadPool threadPool;

    final ConcurrentMap<String, TransportRequestHandler> serverHandlers = newConcurrentMap();

    final NonBlockingHashMapLong<TransportResponseHandler> clientHandlers = new NonBlockingHashMapLong<TransportResponseHandler>();

    final AtomicLong requestIds = new AtomicLong();

    private boolean throwConnectException = false;

    public TransportService(Transport transport, ThreadPool threadPool) {
        this(EMPTY_SETTINGS, transport, threadPool);
    }

    @Inject public TransportService(Settings settings, Transport transport, ThreadPool threadPool) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;
    }

    @Override protected void doStart() throws ElasticSearchException {
        // register us as an adapter for the transport service
        transport.transportServiceAdapter(new TransportServiceAdapter() {
            @Override public TransportRequestHandler handler(String action) {
                return serverHandlers.get(action);
            }

            @Override public TransportResponseHandler remove(long requestId) {
                return clientHandlers.remove(requestId);
            }
        });
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

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public void nodesAdded(Iterable<DiscoveryNode> nodes) {
        try {
            transport.nodesAdded(nodes);
        } catch (Exception e) {
            logger.warn("Failed add nodes [" + nodes + "] to transport", e);
        }
    }

    public void nodesRemoved(Iterable<DiscoveryNode> nodes) {
        try {
            transport.nodesRemoved(nodes);
        } catch (Exception e) {
            logger.warn("Failed to remove nodes[" + nodes + "] from transport", e);
        }
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
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<T>(handler);
        sendRequest(node, action, message, futureHandler);
        return futureHandler;
    }

    public <T extends Streamable> void sendRequest(final DiscoveryNode node, final String action, final Streamable message,
                                                   final TransportResponseHandler<T> handler) throws TransportException {
        final long requestId = newRequestId();
        try {
            clientHandlers.put(requestId, handler);
            transport.sendRequest(node, requestId, action, message, handler);
        } catch (final Exception e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            clientHandlers.remove(requestId);
            if (throwConnectException) {
                if (e instanceof ConnectTransportException) {
                    throw (ConnectTransportException) e;
                }
            }
            // callback that an exception happened, but on a different thread since we don't
            // want handlers to worry about stack overflows
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    handler.handleException(new SendRequestTransportException(node, action, e));
                }
            });
        }
    }

    private long newRequestId() {
        return requestIds.getAndIncrement();
    }

    public void registerHandler(ActionTransportRequestHandler handler) {
        registerHandler(handler.action(), handler);
    }

    public void registerHandler(String action, TransportRequestHandler handler) {
        serverHandlers.put(action, handler);
    }

    public void removeHandler(String action) {
        serverHandlers.remove(action);
    }
}