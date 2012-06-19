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

package org.elasticsearch.discovery.zen.ping.multicast;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.*;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.node.DiscoveryNode.readNode;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 *
 */
public class MulticastZenPing extends AbstractLifecycleComponent<ZenPing> implements ZenPing {

    private static final byte[] INTERNAL_HEADER = new byte[]{1, 9, 8, 4};

    private final String address;

    private final int port;

    private final String group;

    private final int bufferSize;

    private final int ttl;

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final ClusterName clusterName;

    private final NetworkService networkService;

    private final boolean pingEnabled;


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

        this.pingEnabled = componentSettings.getAsBoolean("ping.enabled", true);

        logger.debug("using group [{}], with port [{}], ttl [{}], and address [{}]", group, port, ttl, address);

        this.transportService.registerHandler(MulticastPingResponseRequestHandler.ACTION, new MulticastPingResponseRequestHandler());
    }

    @Override
    public void setNodesProvider(DiscoveryNodesProvider nodesProvider) {
        if (lifecycle.started()) {
            throw new ElasticSearchIllegalStateException("Can't set nodes provider when started");
        }
        this.nodesProvider = nodesProvider;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
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
            logger.warn("disabled, failed to setup multicast discovery on port [{}], [{}]: {}", port, multicastInterface, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("disabled, failed to setup multicast discovery on {}", e, multicastInterface);
            }
        }
    }

    @Override
    protected void doStop() throws ElasticSearchException {
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

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    public PingResponse[] pingAndWait(TimeValue timeout) {
        final AtomicReference<PingResponse[]> response = new AtomicReference<PingResponse[]>();
        final CountDownLatch latch = new CountDownLatch(1);
        ping(new PingListener() {
            @Override
            public void onPing(PingResponse[] pings) {
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

    @Override
    public void ping(final PingListener listener, final TimeValue timeout) {
        if (!pingEnabled) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    listener.onPing(new PingResponse[0]);
                }
            });
            return;
        }
        final int id = pingIdGenerator.incrementAndGet();
        receivedResponses.put(id, ConcurrentCollections.<DiscoveryNode, PingResponse>newConcurrentMap());
        sendPingRequest(id);
        // try and send another ping request halfway through (just in case someone woke up during it...)
        // this can be a good trade-off to nailing the initial lookup or un-delivered messages
        threadPool.schedule(TimeValue.timeValueMillis(timeout.millis() / 2), ThreadPool.Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                try {
                    sendPingRequest(id);
                } catch (Exception e) {
                    logger.warn("[{}] failed to send second ping request", e, id);
                }
            }
        });
        threadPool.schedule(timeout, ThreadPool.Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                ConcurrentMap<DiscoveryNode, PingResponse> responses = receivedResponses.remove(id);
                listener.onPing(responses.values().toArray(new PingResponse[responses.size()]));
            }
        });
    }

    private void sendPingRequest(int id) {
        if (multicastSocket == null) {
            return;
        }
        synchronized (sendMutex) {
            CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
            try {
                StreamOutput out = cachedEntry.cachedHandles();
                out.writeBytes(INTERNAL_HEADER);
                Version.writeVersion(Version.CURRENT, out);
                out.writeInt(id);
                clusterName.writeTo(out);
                nodesProvider.nodes().localNode().writeTo(out);
                out.close();
                datagramPacketSend.setData(cachedEntry.bytes().copiedByteArray());
                multicastSocket.send(datagramPacketSend);
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] sending ping request", id);
                }
            } catch (Exception e) {
                if (lifecycle.stoppedOrClosed()) {
                    return;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to send multicast ping request", e);
                } else {
                    logger.warn("failed to send multicast ping request: {}", ExceptionsHelper.detailedMessage(e));
                }
            } finally {
                CachedStreamOutput.pushEntry(cachedEntry);
            }
        }
    }

    class MulticastPingResponseRequestHandler extends BaseTransportRequestHandler<MulticastPingResponse> {

        static final String ACTION = "discovery/zen/multicast";

        @Override
        public MulticastPingResponse newInstance() {
            return new MulticastPingResponse();
        }

        @Override
        public void messageReceived(MulticastPingResponse request, TransportChannel channel) throws Exception {
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

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    static class MulticastPingResponse implements Streamable {

        int id;

        PingResponse pingResponse;

        MulticastPingResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            id = in.readInt();
            pingResponse = PingResponse.readPingResponse(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(id);
            pingResponse.writeTo(out);
        }
    }


    private class Receiver implements Runnable {

        private volatile boolean running = true;

        public void stop() {
            running = false;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    int id = -1;
                    DiscoveryNode requestingNodeX = null;
                    ClusterName clusterName = null;

                    Map<String, Object> externalPingData = null;
                    XContentType xContentType = null;

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
                            boolean internal = false;
                            if (datagramPacketReceive.getLength() > 4) {
                                int counter = 0;
                                for (; counter < INTERNAL_HEADER.length; counter++) {
                                    if (datagramPacketReceive.getData()[datagramPacketReceive.getOffset() + counter] != INTERNAL_HEADER[counter]) {
                                        break;
                                    }
                                }
                                if (counter == INTERNAL_HEADER.length) {
                                    internal = true;
                                }
                            }
                            if (internal) {
                                StreamInput input = CachedStreamInput.cachedHandles(new BytesStreamInput(datagramPacketReceive.getData(), datagramPacketReceive.getOffset() + INTERNAL_HEADER.length, datagramPacketReceive.getLength(), true));
                                Version version = Version.readVersion(input);
                                id = input.readInt();
                                clusterName = ClusterName.readClusterName(input);
                                requestingNodeX = readNode(input);
                            } else {
                                xContentType = XContentFactory.xContentType(datagramPacketReceive.getData(), datagramPacketReceive.getOffset(), datagramPacketReceive.getLength());
                                if (xContentType != null) {
                                    // an external ping
                                    externalPingData = XContentFactory.xContent(xContentType)
                                            .createParser(datagramPacketReceive.getData(), datagramPacketReceive.getOffset(), datagramPacketReceive.getLength())
                                            .mapAndClose();
                                } else {
                                    throw new ElasticSearchIllegalStateException("failed multicast message, probably message from previous version");
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("failed to read requesting data from {}", e, datagramPacketReceive.getSocketAddress());
                            continue;
                        }
                    }
                    if (externalPingData != null) {
                        handleExternalPingRequest(externalPingData, xContentType, datagramPacketReceive.getSocketAddress());
                    } else {
                        handleNodePingRequest(id, requestingNodeX, clusterName);
                    }
                } catch (Exception e) {
                    logger.warn("unexpected exception in multicast receiver", e);
                }
            }
        }

        @SuppressWarnings("unchecked")
        private void handleExternalPingRequest(Map<String, Object> externalPingData, XContentType contentType, SocketAddress remoteAddress) {
            if (externalPingData.containsKey("response")) {
                // ignoring responses sent over the multicast channel
                logger.trace("got an external ping response (ignoring) from {}, content {}", remoteAddress, externalPingData);
                return;
            }

            if (multicastSocket == null) {
                logger.debug("can't send ping response, no socket, from {}, content {}", remoteAddress, externalPingData);
                return;
            }

            Map<String, Object> request = (Map<String, Object>) externalPingData.get("request");
            if (request == null) {
                logger.warn("malformed external ping request, no 'request' element from {}, content {}", remoteAddress, externalPingData);
                return;
            }

            String clusterName = request.containsKey("cluster_name") ? request.get("cluster_name").toString() : request.containsKey("clusterName") ? request.get("clusterName").toString() : null;
            if (clusterName == null) {
                logger.warn("malformed external ping request, missing 'cluster_name' element within request, from {}, content {}", remoteAddress, externalPingData);
                return;
            }

            if (!clusterName.equals(MulticastZenPing.this.clusterName.value())) {
                logger.trace("got request for cluster_name {}, but our cluster_name is {}, from {}, content {}", clusterName, MulticastZenPing.this.clusterName.value(), remoteAddress, externalPingData);
                return;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("got external ping request from {}, content {}", remoteAddress, externalPingData);
            }

            try {
                DiscoveryNode localNode = nodesProvider.nodes().localNode();

                XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                builder.startObject().startObject("response");
                builder.field("cluster_name", MulticastZenPing.this.clusterName.value());
                builder.startObject("version").field("number", Version.CURRENT.number()).field("snapshot_build", Version.CURRENT.snapshot).endObject();
                builder.field("transport_address", localNode.address().toString());

                if (nodesProvider.nodeService() != null) {
                    for (Map.Entry<String, String> attr : nodesProvider.nodeService().attributes().entrySet()) {
                        builder.field(attr.getKey(), attr.getValue());
                    }
                }

                builder.startObject("attributes");
                for (Map.Entry<String, String> attr : localNode.attributes().entrySet()) {
                    builder.field(attr.getKey(), attr.getValue());
                }
                builder.endObject();

                builder.endObject().endObject();
                synchronized (sendMutex) {
                    datagramPacketSend.setData(builder.underlyingBytes(), 0, builder.underlyingBytesLength());
                    multicastSocket.send(datagramPacketSend);
                    if (logger.isTraceEnabled()) {
                        logger.trace("sending external ping response {}", builder.string());
                    }
                }
            } catch (Exception e) {
                logger.warn("failed to send external multicast response", e);
            }
        }

        private void handleNodePingRequest(int id, DiscoveryNode requestingNodeX, ClusterName clusterName) {
            if (!pingEnabled) {
                return;
            }
            DiscoveryNodes discoveryNodes = nodesProvider.nodes();
            final DiscoveryNode requestingNode = requestingNodeX;
            if (requestingNode.id().equals(discoveryNodes.localNodeId())) {
                // that's me, ignore
                return;
            }
            if (!clusterName.equals(MulticastZenPing.this.clusterName)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] received ping_request from [{}], but wrong cluster_name [{}], expected [{}], ignoring", id, requestingNode, clusterName, MulticastZenPing.this.clusterName);
                }
                return;
            }
            // don't connect between two client nodes, no need for that...
            if (!discoveryNodes.localNode().shouldConnectTo(requestingNode)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] received ping_request from [{}], both are client nodes, ignoring", id, requestingNode, clusterName);
                }
                return;
            }
            final MulticastPingResponse multicastPingResponse = new MulticastPingResponse();
            multicastPingResponse.id = id;
            multicastPingResponse.pingResponse = new PingResponse(discoveryNodes.localNode(), discoveryNodes.masterNode(), clusterName);

            if (logger.isTraceEnabled()) {
                logger.trace("[{}] received ping_request from [{}], sending {}", id, requestingNode, multicastPingResponse.pingResponse);
            }

            if (!transportService.nodeConnected(requestingNode)) {
                // do the connect and send on a thread pool
                threadPool.generic().execute(new Runnable() {
                    @Override
                    public void run() {
                        // connect to the node if possible
                        try {
                            transportService.connectToNode(requestingNode);
                            transportService.sendRequest(requestingNode, MulticastPingResponseRequestHandler.ACTION, multicastPingResponse, new VoidTransportResponseHandler(ThreadPool.Names.SAME) {
                                @Override
                                public void handleException(TransportException exp) {
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
                    @Override
                    public void handleException(TransportException exp) {
                        logger.warn("failed to receive confirmation on sent ping response to [{}]", exp, requestingNode);
                    }
                });
            }
        }
    }
}
