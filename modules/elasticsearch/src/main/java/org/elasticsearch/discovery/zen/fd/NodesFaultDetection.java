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

package org.elasticsearch.discovery.zen.fd;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.cluster.node.DiscoveryNodes.*;
import static org.elasticsearch.util.TimeValue.*;
import static org.elasticsearch.util.concurrent.ConcurrentCollections.*;

/**
 * @author kimchy (shay.banon)
 */
public class NodesFaultDetection extends AbstractComponent {

    public static interface Listener {

        void onNodeFailure(DiscoveryNode node);
    }

    private final ThreadPool threadPool;

    private final TransportService transportService;


    private final boolean connectOnNetworkDisconnect;

    private final TimeValue pingInterval;

    private final TimeValue pingRetryTimeout;

    private final int pingRetryCount;


    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<Listener>();

    private final ConcurrentMap<DiscoveryNode, NodeFD> nodesFD = newConcurrentMap();

    private final FDConnectionListener connectionListener;

    private volatile DiscoveryNodes latestNodes = EMPTY_NODES;

    private volatile boolean running = false;

    public NodesFaultDetection(Settings settings, ThreadPool threadPool, TransportService transportService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;

        this.connectOnNetworkDisconnect = componentSettings.getAsBoolean("connect_on_network_disconnect", false);
        this.pingInterval = componentSettings.getAsTime("ping_interval", timeValueSeconds(1));
        this.pingRetryTimeout = componentSettings.getAsTime("ping_timeout", timeValueSeconds(6));
        this.pingRetryCount = componentSettings.getAsInt("ping_retries", 5);

        transportService.registerHandler(PingRequestHandler.ACTION, new PingRequestHandler());

        this.connectionListener = new FDConnectionListener();
        transportService.addConnectionListener(connectionListener);
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    public void updateNodes(DiscoveryNodes nodes) {
        DiscoveryNodes prevNodes = latestNodes;
        this.latestNodes = nodes;
        if (!running) {
            return;
        }
        DiscoveryNodes.Delta delta = nodes.delta(prevNodes);
        for (DiscoveryNode newNode : delta.addedNodes()) {
            if (!nodesFD.containsKey(newNode)) {
                nodesFD.put(newNode, new NodeFD());
                threadPool.schedule(new SendPingRequest(newNode), pingInterval);
            }
        }
        for (DiscoveryNode removedNode : delta.removedNodes()) {
            nodesFD.remove(removedNode);
        }
    }

    public NodesFaultDetection start() {
        if (running) {
            return this;
        }
        running = true;
        return this;
    }

    public NodesFaultDetection stop() {
        if (!running) {
            return this;
        }
        running = false;
        return this;
    }

    public void close() {
        stop();
        transportService.removeHandler(PingRequestHandler.ACTION);
        transportService.removeConnectionListener(connectionListener);
    }

    private void handleTransportDisconnect(DiscoveryNode node) {
        if (!latestNodes.nodeExists(node.id())) {
            return;
        }
        NodeFD nodeFD = nodesFD.remove(node);
        if (nodeFD == null) {
            return;
        }
        if (!running) {
            return;
        }
        if (connectOnNetworkDisconnect) {
            try {
                transportService.connectToNode(node);
            } catch (Exception e) {
                logger.trace("Node [{}] failed on disconnect (with verified connect)", node);
                notifyNodeFailure(node);
            }
        } else {
            logger.trace("Node [{}] failed on disconnect", node);
            notifyNodeFailure(node);
        }
    }

    private void notifyNodeFailure(DiscoveryNode node) {
        for (Listener listener : listeners) {
            listener.onNodeFailure(node);
        }
    }

    private class SendPingRequest implements Runnable {

        private final DiscoveryNode node;

        private SendPingRequest(DiscoveryNode node) {
            this.node = node;
        }

        @Override public void run() {
            if (!running) {
                return;
            }
            transportService.sendRequest(node, PingRequestHandler.ACTION, new PingRequest(), pingRetryTimeout,
                    new BaseTransportResponseHandler<PingResponse>() {
                        @Override public PingResponse newInstance() {
                            return new PingResponse();
                        }

                        @Override public void handleResponse(PingResponse response) {
                            if (running) {
                                NodeFD nodeFD = nodesFD.get(node);
                                if (nodeFD != null) {
                                    nodeFD.retryCount = 0;
                                    threadPool.schedule(SendPingRequest.this, pingInterval);
                                }
                            }
                        }

                        @Override public void handleException(RemoteTransportException exp) {
                            // check if the master node did not get switched on us...
                            if (running) {
                                NodeFD nodeFD = nodesFD.get(node);
                                if (nodeFD != null) {
                                    int retryCount = ++nodeFD.retryCount;
                                    logger.trace("Node [{}] failed to ping, retry [{}] out of [{}]", exp, node, retryCount, pingRetryCount);
                                    if (retryCount >= pingRetryCount) {
                                        logger.trace("Node [{}] failed on ping", node);
                                        // not good, failure
                                        if (nodesFD.remove(node) != null) {
                                            notifyNodeFailure(node);
                                        }
                                    }
                                }
                            }
                        }
                    });
        }
    }

    static class NodeFD {
        volatile int retryCount;
    }

    private class FDConnectionListener implements TransportConnectionListener {
        @Override public void onNodeConnected(DiscoveryNode node) {
        }

        @Override public void onNodeDisconnected(DiscoveryNode node) {
            handleTransportDisconnect(node);
        }
    }


    private class PingRequestHandler extends BaseTransportRequestHandler<PingRequest> {

        public static final String ACTION = "discovery/zen/fd/ping";

        @Override public PingRequest newInstance() {
            return new PingRequest();
        }

        @Override public void messageReceived(PingRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(new PingResponse());
        }
    }


    private class PingRequest implements Streamable {

        private PingRequest() {
        }

        @Override public void readFrom(StreamInput in) throws IOException {
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
        }
    }

    private class PingResponse implements Streamable {

        private PingResponse() {
        }

        @Override public void readFrom(StreamInput in) throws IOException {
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
        }
    }
}
