/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.transport.TransportRequestOptions.options;

/**
 * A fault detection that pings the master periodically to see if its alive.
 */
public class MasterFaultDetection extends AbstractComponent {

    public static interface Listener {

        void onMasterFailure(DiscoveryNode masterNode, String reason);

        void onDisconnectedFromMaster();
    }

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final DiscoveryNodesProvider nodesProvider;

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<Listener>();


    private final boolean connectOnNetworkDisconnect;

    private final TimeValue pingInterval;

    private final TimeValue pingRetryTimeout;

    private final int pingRetryCount;

    // used mainly for testing, should always be true
    private final boolean registerConnectionListener;


    private final FDConnectionListener connectionListener;

    private volatile MasterPinger masterPinger;

    private final Object masterNodeMutex = new Object();

    private volatile DiscoveryNode masterNode;

    private volatile int retryCount;

    private final AtomicBoolean notifiedMasterFailure = new AtomicBoolean();

    public MasterFaultDetection(Settings settings, ThreadPool threadPool, TransportService transportService, DiscoveryNodesProvider nodesProvider) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.nodesProvider = nodesProvider;

        this.connectOnNetworkDisconnect = componentSettings.getAsBoolean("connect_on_network_disconnect", true);
        this.pingInterval = componentSettings.getAsTime("ping_interval", timeValueSeconds(1));
        this.pingRetryTimeout = componentSettings.getAsTime("ping_timeout", timeValueSeconds(30));
        this.pingRetryCount = componentSettings.getAsInt("ping_retries", 3);
        this.registerConnectionListener = componentSettings.getAsBoolean("register_connection_listener", true);

        logger.debug("[master] uses ping_interval [{}], ping_timeout [{}], ping_retries [{}]", pingInterval, pingRetryTimeout, pingRetryCount);

        this.connectionListener = new FDConnectionListener();
        if (registerConnectionListener) {
            transportService.addConnectionListener(connectionListener);
        }

        transportService.registerHandler(MasterPingRequestHandler.ACTION, new MasterPingRequestHandler());
    }

    public DiscoveryNode masterNode() {
        return this.masterNode;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    public void restart(DiscoveryNode masterNode, String reason) {
        synchronized (masterNodeMutex) {
            if (logger.isDebugEnabled()) {
                logger.debug("[master] restarting fault detection against master [{}], reason [{}]", masterNode, reason);
            }
            innerStop();
            innerStart(masterNode);
        }
    }

    public void start(final DiscoveryNode masterNode, String reason) {
        synchronized (masterNodeMutex) {
            if (logger.isDebugEnabled()) {
                logger.debug("[master] starting fault detection against master [{}], reason [{}]", masterNode, reason);
            }
            innerStart(masterNode);
        }
    }

    private void innerStart(final DiscoveryNode masterNode) {
        this.masterNode = masterNode;
        this.retryCount = 0;
        this.notifiedMasterFailure.set(false);

        // try and connect to make sure we are connected
        try {
            transportService.connectToNode(masterNode);
        } catch (final Exception e) {
            // notify master failure (which stops also) and bail..
            notifyMasterFailure(masterNode, "failed to perform initial connect [" + e.getMessage() + "]");
            return;
        }
        if (masterPinger != null) {
            masterPinger.stop();
        }
        this.masterPinger = new MasterPinger();
        // start the ping process
        threadPool.schedule(pingInterval, ThreadPool.Names.SAME, masterPinger);
    }

    public void stop(String reason) {
        synchronized (masterNodeMutex) {
            if (masterNode != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[master] stopping fault detection against master [{}], reason [{}]", masterNode, reason);
                }
            }
            innerStop();
        }
    }

    private void innerStop() {
        // also will stop the next ping schedule
        this.retryCount = 0;
        if (masterPinger != null) {
            masterPinger.stop();
            masterPinger = null;
        }
        this.masterNode = null;
    }

    public void close() {
        stop("closing");
        this.listeners.clear();
        transportService.removeConnectionListener(connectionListener);
        transportService.removeHandler(MasterPingRequestHandler.ACTION);
    }

    private void handleTransportDisconnect(DiscoveryNode node) {
        synchronized (masterNodeMutex) {
            if (!node.equals(this.masterNode)) {
                return;
            }
            if (connectOnNetworkDisconnect) {
                try {
                    transportService.connectToNode(node);
                    // if all is well, make sure we restart the pinger
                    if (masterPinger != null) {
                        masterPinger.stop();
                    }
                    this.masterPinger = new MasterPinger();
                    threadPool.schedule(pingInterval, ThreadPool.Names.SAME, masterPinger);
                } catch (Exception e) {
                    logger.trace("[master] [{}] transport disconnected (with verified connect)", masterNode);
                    notifyMasterFailure(masterNode, "transport disconnected (with verified connect)");
                }
            } else {
                logger.trace("[master] [{}] transport disconnected", node);
                notifyMasterFailure(node, "transport disconnected");
            }
        }
    }

    private void notifyDisconnectedFromMaster() {
        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
                for (Listener listener : listeners) {
                    listener.onDisconnectedFromMaster();
                }
            }
        });
    }

    private void notifyMasterFailure(final DiscoveryNode masterNode, final String reason) {
        if (notifiedMasterFailure.compareAndSet(false, true)) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    for (Listener listener : listeners) {
                        listener.onMasterFailure(masterNode, reason);
                    }
                }
            });
            stop("master failure, " + reason);
        }
    }

    private class FDConnectionListener implements TransportConnectionListener {
        @Override
        public void onNodeConnected(DiscoveryNode node) {
        }

        @Override
        public void onNodeDisconnected(DiscoveryNode node) {
            handleTransportDisconnect(node);
        }
    }

    private class MasterPinger implements Runnable {

        private volatile boolean running = true;

        public void stop() {
            this.running = false;
        }

        @Override
        public void run() {
            if (!running) {
                // return and don't spawn...
                return;
            }
            final DiscoveryNode masterToPing = masterNode;
            if (masterToPing == null) {
                // master is null, should not happen, but we are still running, so reschedule
                threadPool.schedule(pingInterval, ThreadPool.Names.SAME, MasterPinger.this);
                return;
            }
            transportService.sendRequest(masterToPing, MasterPingRequestHandler.ACTION, new MasterPingRequest(nodesProvider.nodes().localNode().id(), masterToPing.id()), options().withHighType().withTimeout(pingRetryTimeout),
                    new BaseTransportResponseHandler<MasterPingResponseResponse>() {
                        @Override
                        public MasterPingResponseResponse newInstance() {
                            return new MasterPingResponseResponse();
                        }

                        @Override
                        public void handleResponse(MasterPingResponseResponse response) {
                            if (!running) {
                                return;
                            }
                            // reset the counter, we got a good result
                            MasterFaultDetection.this.retryCount = 0;
                            // check if the master node did not get switched on us..., if it did, we simply return with no reschedule
                            if (masterToPing.equals(MasterFaultDetection.this.masterNode())) {
                                if (!response.connectedToMaster) {
                                    logger.trace("[master] [{}] does not have us registered with it...", masterToPing);
                                    notifyDisconnectedFromMaster();
                                }
                                // we don't stop on disconnection from master, we keep pinging it
                                threadPool.schedule(pingInterval, ThreadPool.Names.SAME, MasterPinger.this);
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            if (!running) {
                                return;
                            }
                            if (exp instanceof ConnectTransportException) {
                                // ignore this one, we already handle it by registering a connection listener
                                return;
                            }
                            synchronized (masterNodeMutex) {
                                // check if the master node did not get switched on us...
                                if (masterToPing.equals(MasterFaultDetection.this.masterNode())) {
                                    if (exp.getCause() instanceof NoLongerMasterException) {
                                        logger.debug("[master] pinging a master {} that is no longer a master", masterNode);
                                        notifyMasterFailure(masterToPing, "no longer master");
                                        return;
                                    } else if (exp.getCause() instanceof NotMasterException) {
                                        logger.debug("[master] pinging a master {} that is not the master", masterNode);
                                        notifyMasterFailure(masterToPing, "no longer master");
                                        return;
                                    } else if (exp.getCause() instanceof NodeDoesNotExistOnMasterException) {
                                        logger.debug("[master] pinging a master {} but we do not exists on it, act as if its master failure", masterNode);
                                        notifyMasterFailure(masterToPing, "do not exists on master, act as master failure");
                                        return;
                                    }
                                    int retryCount = ++MasterFaultDetection.this.retryCount;
                                    logger.trace("[master] failed to ping [{}], retry [{}] out of [{}]", exp, masterNode, retryCount, pingRetryCount);
                                    if (retryCount >= pingRetryCount) {
                                        logger.debug("[master] failed to ping [{}], tried [{}] times, each with maximum [{}] timeout", masterNode, pingRetryCount, pingRetryTimeout);
                                        // not good, failure
                                        notifyMasterFailure(masterToPing, "failed to ping, tried [" + pingRetryCount + "] times, each with  maximum [" + pingRetryTimeout + "] timeout");
                                    } else {
                                        // resend the request, not reschedule, rely on send timeout
                                        transportService.sendRequest(masterToPing, MasterPingRequestHandler.ACTION, new MasterPingRequest(nodesProvider.nodes().localNode().id(), masterToPing.id()), options().withHighType().withTimeout(pingRetryTimeout), this);
                                    }
                                }
                            }
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    });
        }
    }

    static class NoLongerMasterException extends ElasticSearchIllegalStateException {
        @Override
        public Throwable fillInStackTrace() {
            return null;
        }
    }

    static class NotMasterException extends ElasticSearchIllegalStateException {
        @Override
        public Throwable fillInStackTrace() {
            return null;
        }
    }

    static class NodeDoesNotExistOnMasterException extends ElasticSearchIllegalStateException {
        @Override
        public Throwable fillInStackTrace() {
            return null;
        }
    }

    private class MasterPingRequestHandler extends BaseTransportRequestHandler<MasterPingRequest> {

        public static final String ACTION = "discovery/zen/fd/masterPing";

        @Override
        public MasterPingRequest newInstance() {
            return new MasterPingRequest();
        }

        @Override
        public void messageReceived(MasterPingRequest request, TransportChannel channel) throws Exception {
            DiscoveryNodes nodes = nodesProvider.nodes();
            // check if we are really the same master as the one we seemed to be think we are
            // this can happen if the master got "kill -9" and then another node started using the same port
            if (!request.masterNodeId.equals(nodes.localNodeId())) {
                throw new NotMasterException();
            }
            // if we are no longer master, fail...
            if (!nodes.localNodeMaster()) {
                throw new NoLongerMasterException();
            }
            if (!nodes.nodeExists(request.nodeId)) {
                throw new NodeDoesNotExistOnMasterException();
            }
            // send a response, and note if we are connected to the master or not
            channel.sendResponse(new MasterPingResponseResponse(nodes.nodeExists(request.nodeId)));
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }


    private static class MasterPingRequest implements Streamable {

        private String nodeId;

        private String masterNodeId;

        private MasterPingRequest() {
        }

        private MasterPingRequest(String nodeId, String masterNodeId) {
            this.nodeId = nodeId;
            this.masterNodeId = masterNodeId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            nodeId = in.readUTF();
            masterNodeId = in.readUTF();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeUTF(nodeId);
            out.writeUTF(masterNodeId);
        }
    }

    private static class MasterPingResponseResponse implements Streamable {

        private boolean connectedToMaster;

        private MasterPingResponseResponse() {
        }

        private MasterPingResponseResponse(boolean connectedToMaster) {
            this.connectedToMaster = connectedToMaster;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            connectedToMaster = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(connectedToMaster);
        }
    }
}
