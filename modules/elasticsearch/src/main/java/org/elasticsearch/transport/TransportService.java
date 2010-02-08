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
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.concurrent.highscalelib.NonBlockingHashMapLong;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.transport.BoundTransportAddress;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.util.concurrent.ConcurrentMaps.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportService extends AbstractComponent implements LifecycleComponent<TransportService> {

    private final Lifecycle lifecycle = new Lifecycle();

    private final Transport transport;

    private final ConcurrentMap<String, TransportRequestHandler> serverHandlers = newConcurrentMap();

    private final NonBlockingHashMapLong<TransportResponseHandler> clientHandlers = new NonBlockingHashMapLong<TransportResponseHandler>();

    final AtomicLong requestIds = new AtomicLong();

    public TransportService(Transport transport) {
        this(EMPTY_SETTINGS, transport);
    }

    @Inject public TransportService(Settings settings, Transport transport) {
        super(settings);
        this.transport = transport;
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    public TransportService start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
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
        return this;
    }

    public TransportService stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        transport.stop();
        return this;
    }

    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        transport.close();
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public void nodesAdded(Iterable<Node> nodes) {
        try {
            transport.nodesAdded(nodes);
        } catch (Exception e) {
            logger.warn("Failed add nodes [" + nodes + "] to transport", e);
        }
    }

    public void nodesRemoved(Iterable<Node> nodes) {
        try {
            transport.nodesRemoved(nodes);
        } catch (Exception e) {
            logger.warn("Failed to remove nodes[" + nodes + "] from transport", e);
        }
    }

    public <T extends Streamable> TransportFuture<T> submitRequest(Node node, String action, Streamable message,
                                                                   TransportResponseHandler<T> handler) throws TransportException {
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<T>(handler);
        sendRequest(node, action, message, futureHandler);
        return futureHandler;
    }

    public <T extends Streamable> void sendRequest(Node node, String action, Streamable message,
                                                   TransportResponseHandler<T> handler) throws TransportException {
        try {
            final long requestId = newRequestId();
            clientHandlers.put(requestId, handler);
            transport.sendRequest(node, requestId, action, message, handler);
        } catch (IOException e) {
            throw new TransportException("Can't serialize request", e);
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