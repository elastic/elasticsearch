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

package org.elasticsearch.discovery.zen.ping.multicast;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.CachedStreamInput;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.HandlesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.VoidStreamable;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.discovery.zen.ping.ZenPingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.VoidTransportResponseHandler;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.node.DiscoveryNode.*;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.*;
import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public class MulticastZenPing extends AbstractLifecycleComponent<ZenPing> implements ZenPing {

    private final String address;

    private final int port;

    private final String group;

    private final int bufferSize;

    private final int ttl;

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final ClusterName clusterName;

    private final NetworkService networkService;


    private volatile DiscoveryNodesProvider nodesProvider;

    private volatile Receiver receiver;

    private volatile Thread receiverThread;

    private MulticastSocket multicastSocket;

    private DatagramPacket datagramPacketSend;

    private DatagramPacket datagramPacketReceive;

    private final AtomicInteger pingIdGenerator = new AtomicInteger();

    private final Map<Integer, ConcurrentMap<DiscoveryNode, PingResponse>> receivedResponses = newConcurrentMap();

    private final Object sendMutex = new Object();

    private final Object receiveMutex = new Object();

    public MulticastZenPing(ThreadPool threadPool, TransportService transportService, ClusterName clusterName) {
        this(EMPTY_SETTINGS, threadPool, transportService, clusterName, new NetworkService(EMPTY_SETTINGS));
    }

    public MulticastZenPing(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterName clusterName, NetworkService networkService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterName = clusterName;
        this.networkService = networkService;

        this.address = componentSettings.get("address");
        this.port = componentSettings.getAsInt("port", 54328);
        this.group = componentSettings.get("group", "224.2.2.4");
        this.bufferSize = componentSettings.getAsInt("buffer_size", 2048);
        this.ttl = componentSettings.getAsInt("ttl", 3);

        logger.debug("using group [{}], with port [{}], ttl [{}], and address [{}]", group, port, ttl, address);

        this.transportService.registerHandler(MulticastPingResponseRequestHandler.ACTION, new MulticastPingResponseRequestHandler());
    }

    @Override public void setNodesProvider(DiscoveryNodesProvider nodesProvider) {
        if (lifecycle.started()) {
            throw new ElasticSearchIllegalStateException("Can't set nodes provider when started");
        }
        this.nodesProvider = nodesProvider;
    }

    @Override protected void doStart() throws ElasticSearchException {
        try {
            this.datagramPacketReceive = new DatagramPacket(new byte[bufferSize], bufferSize);
            this.datagramPacketSend = new DatagramPacket(new byte[bufferSize], bufferSize, InetAddress.getByName(group), port);
        } catch (Exception e) {
            logger.warn("disabled, failed to setup multicast (datagram) discovery : {}", e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("disabled, failed to setup multicast (datagram) discovery", e);
            }
            return;
        }

        InetAddress multicastInterface = null;
        try {
            MulticastSocket multicastSocket;
//            if (NetworkUtils.canBindToMcastAddress()) {
//                try {
//                    multicastSocket = new MulticastSocket(new InetSocketAddress(group, port));
//                } catch (Exception e) {
//                    logger.debug("Failed to create multicast socket by binding to group address, binding to port", e);
//                    multicastSocket = new MulticastSocket(port);
//                }
//            } else {
            multicastSocket = new MulticastSocket(port);
//            }

            multicastSocket.setTimeToLive(ttl);

            // set the send interface
            multicastInterface = networkService.resolvePublishHostAddress(address);
            multicastSocket.setInterface(multicastInterface);
            multicastSocket.joinGroup(InetAddress.getByName(group));

            multicastSocket.setReceiveBufferSize(bufferSize);
            multicastSocket.setSendBufferSize(bufferSize);
            multicastSocket.setSoTimeout(60000);

            this.multicastSocket = multicastSocket;

            this.receiver = new Receiver();
            this.receiverThread = daemonThreadFactory(settings, "discovery#multicast#receiver").newThread(receiver);
            this.receiverThread.start();
        } catch (Exception e) {
            datagramPacketReceive = null;
            datagramPacketSend = null;
            if (multicastSocket != null) {
                multicastSocket.close();
                multicastSocket = null;
            }
            logger.warn("disabled, failed to setup multicast discovery on {}: {}", multicastInterface, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("disabled, failed to setup multicast discovery on {}", e, multicastInterface);
            }
        }
    }

    @Override protected void doStop() throws ElasticSearchException {
        if (receiver != null) {
            receiver.stop();
        }
        if (receiverThread != null) {
            receiverThread.interrupt();
        }
        if (multicastSocket != null) {
            multicastSocket.close();
            multicastSocket = null;
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    public PingResponse[] pingAndWait(TimeValue timeout) {
        final AtomicReference<PingResponse[]> response = new AtomicReference<PingResponse[]>();
        final CountDownLatch latch = new CountDownLatch(1);
        ping(new PingListener() {
            @Override public void onPing(PingResponse[] pings) {
                response.set(pings);
                latch.countDown();
            }
        }, timeout);
        try {
            latch.await();
            return response.get();
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override public void ping(final PingListener listener, final TimeValue timeout) {
        final int id = pingIdGenerator.incrementAndGet();
        receivedResponses.put(id, new ConcurrentHashMap<DiscoveryNode, PingResponse>());
        sendPingRequest(id, true);
        // try and send another ping request halfway through (just in case someone woke up during it...)
        // this can be a good trade-off to nailing the initial lookup or un-delivered messages
        threadPool.schedule(TimeValue.timeValueMillis(timeout.millis() / 2), ThreadPool.Names.CACHED, new Runnable() {
            @Override public void run() {
                try {
                    sendPingRequest(id, false);
                } catch (Exception e) {
                    logger.warn("[{}] failed to send second ping request", e, id);
                }
            }
        });
        threadPool.schedule(timeout, ThreadPool.Names.CACHED, new Runnable() {
            @Override public void run() {
                ConcurrentMap<DiscoveryNode, PingResponse> responses = receivedResponses.remove(id);
                listener.onPing(responses.values().toArray(new PingResponse[responses.size()]));
            }
        });
    }

    private void sendPingRequest(int id, boolean remove) {
        if (multicastSocket == null) {
            return;
        }
        synchronized (sendMutex) {
            CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
            try {
                HandlesStreamOutput out = cachedEntry.cachedHandlesBytes();
                out.writeInt(id);
                clusterName.writeTo(out);
                nodesProvider.nodes().localNode().writeTo(out);
                datagramPacketSend.setData(cachedEntry.bytes().copiedByteArray());
            } catch (IOException e) {
                if (remove) {
                    receivedResponses.remove(id);
                }
                throw new ZenPingException("Failed to serialize ping request", e);
            } finally {
                CachedStreamOutput.pushEntry(cachedEntry);
            }
            try {
                multicastSocket.send(datagramPacketSend);
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] sending ping request", id);
                }
            } catch (IOException e) {
                if (remove) {
                    receivedResponses.remove(id);
                }
                if (lifecycle.stoppedOrClosed()) {
                    return;
                }
                throw new ZenPingException("Failed to send ping request over multicast on " + multicastSocket, e);
            }
        }
    }

    class MulticastPingResponseRequestHandler extends BaseTransportRequestHandler<MulticastPingResponse> {

        static final String ACTION = "discovery/zen/multicast";

        @Override public MulticastPingResponse newInstance() {
            return new MulticastPingResponse();
        }

        @Override public void messageReceived(MulticastPingResponse request, TransportChannel channel) throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] received {}", request.id, request.pingResponse);
            }
            ConcurrentMap<DiscoveryNode, PingResponse> responses = receivedResponses.get(request.id);
            if (responses == null) {
                logger.warn("received ping response {} with no matching id [{}]", request.pingResponse, request.id);
            } else {
                responses.put(request.pingResponse.target(), request.pingResponse);
            }
            channel.sendResponse(VoidStreamable.INSTANCE);
        }

        @Override public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    static class MulticastPingResponse implements Streamable {

        int id;

        PingResponse pingResponse;

        MulticastPingResponse() {
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            id = in.readInt();
            pingResponse = PingResponse.readPingResponse(in);
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(id);
            pingResponse.writeTo(out);
        }
    }


    private class Receiver implements Runnable {

        private volatile boolean running = true;

        public void stop() {
            running = false;
        }

        @Override public void run() {
            while (running) {
                try {
                    int id;
                    DiscoveryNode requestingNodeX;
                    ClusterName clusterName;
                    synchronized (receiveMutex) {
                        try {
                            multicastSocket.receive(datagramPacketReceive);
                        } catch (SocketTimeoutException ignore) {
                            continue;
                        } catch (Exception e) {
                            if (running) {
                                logger.warn("failed to receive packet", e);
                            }
                            continue;
                        }
                        try {
                            StreamInput input = CachedStreamInput.cachedHandles(new BytesStreamInput(datagramPacketReceive.getData(), datagramPacketReceive.getOffset(), datagramPacketReceive.getLength()));
                            id = input.readInt();
                            clusterName = ClusterName.readClusterName(input);
                            requestingNodeX = readNode(input);
                        } catch (Exception e) {
                            logger.warn("failed to read requesting node from {}", e, datagramPacketReceive.getSocketAddress());
                            continue;
                        }
                    }
                    DiscoveryNodes discoveryNodes = nodesProvider.nodes();
                    final DiscoveryNode requestingNode = requestingNodeX;
                    if (requestingNode.id().equals(discoveryNodes.localNodeId())) {
                        // that's me, ignore
                        continue;
                    }
                    if (!clusterName.equals(MulticastZenPing.this.clusterName)) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("[{}] received ping_request from [{}], but wrong cluster_name [{}], expected [{}], ignoring", id, requestingNode, clusterName, MulticastZenPing.this.clusterName);
                        }
                        continue;
                    }
                    // don't connect between two client nodes, no need for that...
                    if (!discoveryNodes.localNode().shouldConnectTo(requestingNode)) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("[{}] received ping_request from [{}], both are client nodes, ignoring", id, requestingNode, clusterName);
                        }
                        continue;
                    }
                    final MulticastPingResponse multicastPingResponse = new MulticastPingResponse();
                    multicastPingResponse.id = id;
                    multicastPingResponse.pingResponse = new PingResponse(discoveryNodes.localNode(), discoveryNodes.masterNode(), clusterName);

                    if (logger.isTraceEnabled()) {
                        logger.trace("[{}] received ping_request from [{}], sending {}", id, requestingNode, multicastPingResponse.pingResponse);
                    }

                    if (!transportService.nodeConnected(requestingNode)) {
                        // do the connect and send on a thread pool
                        threadPool.cached().execute(new Runnable() {
                            @Override public void run() {
                                // connect to the node if possible
                                try {
                                    transportService.connectToNode(requestingNode);
                                    transportService.sendRequest(requestingNode, MulticastPingResponseRequestHandler.ACTION, multicastPingResponse, new VoidTransportResponseHandler(ThreadPool.Names.SAME) {
                                        @Override public void handleException(TransportException exp) {
                                            logger.warn("failed to receive confirmation on sent ping response to [{}]", exp, requestingNode);
                                        }
                                    });
                                } catch (Exception e) {
                                    logger.warn("failed to connect to requesting node {}", e, requestingNode);
                                }
                            }
                        });
                    } else {
                        transportService.sendRequest(requestingNode, MulticastPingResponseRequestHandler.ACTION, multicastPingResponse, new VoidTransportResponseHandler(ThreadPool.Names.SAME) {
                            @Override public void handleException(TransportException exp) {
                                logger.warn("failed to receive confirmation on sent ping response to [{}]", exp, requestingNode);
                            }
                        });
                    }
                } catch (Exception e) {
                    logger.warn("unexpected exception in multicast receiver", e);
                }
            }
        }
    }
}
