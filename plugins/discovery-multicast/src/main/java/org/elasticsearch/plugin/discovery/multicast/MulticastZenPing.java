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

package org.elasticsearch.plugin.discovery.multicast;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.zen.ping.PingContextProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.node.DiscoveryNode.readNode;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 *
 */
public class MulticastZenPing extends AbstractLifecycleComponent<ZenPing> implements ZenPing {

    public static final String ACTION_NAME = "internal:discovery/zen/multicast";

    private static final byte[] INTERNAL_HEADER = new byte[]{1, 9, 8, 4};

    private static final int PING_SIZE_ESTIMATE = 150;

    private final String address;
    private final int port;
    private final String group;
    private final int bufferSize;
    private final int ttl;

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterName clusterName;
    private final NetworkService networkService;
    private final Version version;
    private volatile PingContextProvider contextProvider;

    private final boolean pingEnabled;

    private volatile MulticastChannel multicastChannel;

    private final AtomicInteger pingIdGenerator = new AtomicInteger();
    private final Map<Integer, PingCollection> receivedResponses = newConcurrentMap();

    public MulticastZenPing(ThreadPool threadPool, TransportService transportService, ClusterName clusterName, Version version) {
        this(EMPTY_SETTINGS, threadPool, transportService, clusterName, new NetworkService(EMPTY_SETTINGS), version);
    }

    @Inject
    public MulticastZenPing(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterName clusterName, NetworkService networkService, Version version) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterName = clusterName;
        this.networkService = networkService;
        this.version = version;

        this.address = this.settings.get("discovery.zen.ping.multicast.address");
        this.port = this.settings.getAsInt("discovery.zen.ping.multicast.port", 54328);
        this.group = this.settings.get("discovery.zen.ping.multicast.group", "224.2.2.4");
        this.bufferSize = this.settings.getAsInt("discovery.zen.ping.multicast.buffer_size", 2048);
        this.ttl = this.settings.getAsInt("discovery.zen.ping.multicast.ttl", 3);

        this.pingEnabled = this.settings.getAsBoolean("discovery.zen.ping.multicast.ping.enabled", true);

        logger.debug("using group [{}], with port [{}], ttl [{}], and address [{}]", group, port, ttl, address);

        this.transportService.registerRequestHandler(ACTION_NAME, MulticastPingResponse::new, ThreadPool.Names.SAME, new MulticastPingResponseRequestHandler());
    }

    @Override
    public void setPingContextProvider(PingContextProvider nodesProvider) {
        if (lifecycle.started()) {
            throw new IllegalStateException("Can't set nodes provider when started");
        }
        this.contextProvider = nodesProvider;
    }

    @Override
    protected void doStart() {
        try {
            // we know OSX has bugs in the JVM when creating multiple instances of multicast sockets
            // causing for "socket close" exceptions when receive and/or crashes
            boolean shared = settings.getAsBoolean("discovery.zen.ping.multicast.shared", Constants.MAC_OS_X);
            // OSX does not correctly send multicasts FROM the right interface
            boolean deferToInterface = settings.getAsBoolean("discovery.zen.ping.multicast.defer_group_to_set_interface", Constants.MAC_OS_X);
            multicastChannel = MulticastChannel.getChannel(nodeName(), shared,
                    new MulticastChannel.Config(port, group, bufferSize, ttl,
                            // don't use publish address, the use case for that is e.g. a firewall or proxy and
                            // may not even be bound to an interface on this machine! use the first bound address.
                            networkService.resolveBindHostAddress(address)[0],
                            deferToInterface),
                    new Receiver());
        } catch (Throwable t) {
            String msg = "multicast failed to start [{}], disabling. Consider using IPv4 only (by defining env. variable `ES_USE_IPV4`)";
            if (logger.isDebugEnabled()) {
                logger.debug(msg, t, ExceptionsHelper.detailedMessage(t));
            } else {
                logger.info(msg, ExceptionsHelper.detailedMessage(t));
            }
        }
    }

    @Override
    protected void doStop() {
        if (multicastChannel != null) {
            multicastChannel.close();
            multicastChannel = null;
        }
    }

    @Override
    protected void doClose() {
    }

    public PingResponse[] pingAndWait(TimeValue timeout) {
        final AtomicReference<PingResponse[]> response = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            ping(new PingListener() {
                @Override
                public void onPing(PingResponse[] pings) {
                    response.set(pings);
                    latch.countDown();
                }
            }, timeout);
        } catch (EsRejectedExecutionException ex) {
            logger.debug("Ping execution rejected", ex);
            return PingResponse.EMPTY;
        }
        try {
            latch.await();
            return response.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return PingResponse.EMPTY;
        }
    }

    @Override
    public void ping(final PingListener listener, final TimeValue timeout) {
        if (!pingEnabled || multicastChannel == null) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    listener.onPing(PingResponse.EMPTY);
                }
            });
            return;
        }
        final int id = pingIdGenerator.incrementAndGet();
        try {
            receivedResponses.put(id, new PingCollection());
            sendPingRequest(id);
            // try and send another ping request halfway through (just in case someone woke up during it...)
            // this can be a good trade-off to nailing the initial lookup or un-delivered messages
            threadPool.schedule(TimeValue.timeValueMillis(timeout.millis() / 2), ThreadPool.Names.GENERIC, new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    logger.warn("[{}] failed to send second ping request", t, id);
                    finalizePingCycle(id, listener);
                }

                @Override
                public void doRun() {
                    sendPingRequest(id);
                    threadPool.schedule(TimeValue.timeValueMillis(timeout.millis() / 2), ThreadPool.Names.GENERIC, new AbstractRunnable() {
                        @Override
                        public void onFailure(Throwable t) {
                            logger.warn("[{}] failed to send third ping request", t, id);
                            finalizePingCycle(id, listener);
                        }

                        @Override
                        public void doRun() {
                            // make one last ping, but finalize as soon as all nodes have responded or a timeout has past
                            PingCollection collection = receivedResponses.get(id);
                            FinalizingPingCollection finalizingPingCollection = new FinalizingPingCollection(id, collection, collection.size(), listener);
                            receivedResponses.put(id, finalizingPingCollection);
                            logger.trace("[{}] sending last pings", id);
                            sendPingRequest(id);
                            threadPool.schedule(TimeValue.timeValueMillis(timeout.millis() / 4), ThreadPool.Names.GENERIC, new AbstractRunnable() {
                                @Override
                                public void onFailure(Throwable t) {
                                    logger.warn("[{}] failed to finalize ping", t, id);
                                }

                                @Override
                                protected void doRun() throws Exception {
                                    finalizePingCycle(id, listener);
                                }
                            });
                        }
                    });
                }
            });
        } catch (Exception e) {
            logger.warn("failed to ping", e);
            finalizePingCycle(id, listener);
        }
    }

    /**
     * takes all pings collected for a given id and pass them to the given listener.
     * this method is safe to call multiple times as is guaranteed to only finalize once.
     */
    private void finalizePingCycle(int id, final PingListener listener) {
        PingCollection responses = receivedResponses.remove(id);
        if (responses != null) {
            listener.onPing(responses.toArray());
        }
    }

    private void sendPingRequest(int id) {
        try {
            BytesStreamOutput out = new BytesStreamOutput(PING_SIZE_ESTIMATE);
            out.writeBytes(INTERNAL_HEADER);
            // TODO: change to min_required version!
            Version.writeVersion(version, out);
            out.writeInt(id);
            clusterName.writeTo(out);
            contextProvider.nodes().localNode().writeTo(out);
            out.close();
            multicastChannel.send(out.bytes());
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
        }
    }

    class FinalizingPingCollection extends PingCollection {
        final private PingCollection internalCollection;
        final private int expectedResponses;
        final private AtomicInteger responseCount;
        final private PingListener listener;
        final private int id;

        public FinalizingPingCollection(int id, PingCollection internalCollection, int expectedResponses, PingListener listener) {
            this.id = id;
            this.internalCollection = internalCollection;
            this.expectedResponses = expectedResponses;
            this.responseCount = new AtomicInteger();
            this.listener = listener;
        }

        @Override
        public synchronized boolean addPing(PingResponse ping) {
            if (internalCollection.addPing(ping)) {
                if (responseCount.incrementAndGet() >= expectedResponses) {
                    logger.trace("[{}] all nodes responded", id);
                    finish();
                }
                return true;
            }
            return false;
        }

        @Override
        public synchronized void addPings(PingResponse[] pings) {
            internalCollection.addPings(pings);
        }

        @Override
        public synchronized PingResponse[] toArray() {
            return internalCollection.toArray();
        }

        void finish() {
            // spawn another thread as we may be running on a network thread
            threadPool.generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    logger.error("failed to call ping listener", t);
                }

                @Override
                protected void doRun() throws Exception {
                    finalizePingCycle(id, listener);
                }
            });
        }
    }

    class MulticastPingResponseRequestHandler implements TransportRequestHandler<MulticastPingResponse> {
        @Override
        public void messageReceived(MulticastPingResponse request, TransportChannel channel) throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] received {}", request.id, request.pingResponse);
            }
            PingCollection responses = receivedResponses.get(request.id);
            if (responses == null) {
                logger.warn("received ping response {} with no matching id [{}]", request.pingResponse, request.id);
            } else {
                responses.addPing(request.pingResponse);
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    public static class MulticastPingResponse extends TransportRequest {

        int id;

        PingResponse pingResponse;

        public MulticastPingResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            id = in.readInt();
            pingResponse = PingResponse.readPingResponse(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            pingResponse.writeTo(out);
        }
    }


    private class Receiver implements MulticastChannel.Listener {

        @Override
        public void onMessage(BytesReference data, SocketAddress address) {
            int id = -1;
            DiscoveryNode requestingNodeX = null;
            ClusterName clusterName = null;

            Map<String, Object> externalPingData = null;
            XContentType xContentType = null;

            try {
                boolean internal = false;
                if (data.length() > 4) {
                    int counter = 0;
                    for (; counter < INTERNAL_HEADER.length; counter++) {
                        if (data.get(counter) != INTERNAL_HEADER[counter]) {
                            break;
                        }
                    }
                    if (counter == INTERNAL_HEADER.length) {
                        internal = true;
                    }
                }
                if (internal) {
                    StreamInput input = StreamInput.wrap(new BytesArray(data.toBytes(), INTERNAL_HEADER.length, data.length() - INTERNAL_HEADER.length));
                    Version version = Version.readVersion(input);
                    input.setVersion(version);
                    id = input.readInt();
                    clusterName = ClusterName.readClusterName(input);
                    requestingNodeX = readNode(input);
                } else {
                    xContentType = XContentFactory.xContentType(data);
                    if (xContentType != null) {
                        // an external ping
                        try (XContentParser parser = XContentFactory.xContent(xContentType).createParser(data)) {
                            externalPingData = parser.map();
                        }
                    } else {
                        throw new IllegalStateException("failed multicast message, probably message from previous version");
                    }
                }
                if (externalPingData != null) {
                    handleExternalPingRequest(externalPingData, xContentType, address);
                } else {
                    handleNodePingRequest(id, requestingNodeX, clusterName);
                }
            } catch (Exception e) {
                if (!lifecycle.started() || (e instanceof EsRejectedExecutionException)) {
                    logger.debug("failed to read requesting data from {}", e, address);
                } else {
                    logger.warn("failed to read requesting data from {}", e, address);
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

            if (multicastChannel == null) {
                logger.debug("can't send ping response, no socket, from {}, content {}", remoteAddress, externalPingData);
                return;
            }

            Map<String, Object> request = (Map<String, Object>) externalPingData.get("request");
            if (request == null) {
                logger.warn("malformed external ping request, no 'request' element from {}, content {}", remoteAddress, externalPingData);
                return;
            }

            final String requestClusterName = request.containsKey("cluster_name") ? request.get("cluster_name").toString() : request.containsKey("clusterName") ? request.get("clusterName").toString() : null;
            if (requestClusterName == null) {
                logger.warn("malformed external ping request, missing 'cluster_name' element within request, from {}, content {}", remoteAddress, externalPingData);
                return;
            }

            if (!requestClusterName.equals(clusterName.value())) {
                logger.trace("got request for cluster_name {}, but our cluster_name is {}, from {}, content {}",
                        requestClusterName, clusterName.value(), remoteAddress, externalPingData);
                return;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("got external ping request from {}, content {}", remoteAddress, externalPingData);
            }

            try {
                DiscoveryNode localNode = contextProvider.nodes().localNode();

                XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                builder.startObject().startObject("response");
                builder.field("cluster_name", clusterName.value());
                builder.startObject("version").field("number", version.number()).field("snapshot_build", version.snapshot).endObject();
                builder.field("transport_address", localNode.address().toString());

                if (contextProvider.nodeService() != null) {
                    for (Map.Entry<String, String> attr : contextProvider.nodeService().attributes().entrySet()) {
                        builder.field(attr.getKey(), attr.getValue());
                    }
                }

                builder.startObject("attributes");
                for (Map.Entry<String, String> attr : localNode.attributes().entrySet()) {
                    builder.field(attr.getKey(), attr.getValue());
                }
                builder.endObject();

                builder.endObject().endObject();
                multicastChannel.send(builder.bytes());
                if (logger.isTraceEnabled()) {
                    logger.trace("sending external ping response {}", builder.string());
                }
            } catch (Exception e) {
                logger.warn("failed to send external multicast response", e);
            }
        }

        private void handleNodePingRequest(int id, DiscoveryNode requestingNodeX, ClusterName requestClusterName) {
            if (!pingEnabled || multicastChannel == null) {
                return;
            }
            final DiscoveryNodes discoveryNodes = contextProvider.nodes();
            final DiscoveryNode requestingNode = requestingNodeX;
            if (requestingNode.id().equals(discoveryNodes.localNodeId())) {
                // that's me, ignore
                return;
            }
            if (!requestClusterName.equals(clusterName)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] received ping_request from [{}], but wrong cluster_name [{}], expected [{}], ignoring",
                            id, requestingNode, requestClusterName.value(), clusterName.value());
                }
                return;
            }
            // don't connect between two client nodes, no need for that...
            if (!discoveryNodes.localNode().shouldConnectTo(requestingNode)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] received ping_request from [{}], both are client nodes, ignoring", id, requestingNode, requestClusterName);
                }
                return;
            }
            final MulticastPingResponse multicastPingResponse = new MulticastPingResponse();
            multicastPingResponse.id = id;
            multicastPingResponse.pingResponse = new PingResponse(discoveryNodes.localNode(), discoveryNodes.masterNode(), clusterName, contextProvider.nodeHasJoinedClusterOnce());

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
                            transportService.sendRequest(requestingNode, ACTION_NAME, multicastPingResponse, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                                @Override
                                public void handleException(TransportException exp) {
                                    logger.warn("failed to receive confirmation on sent ping response to [{}]", exp, requestingNode);
                                }
                            });
                        } catch (Exception e) {
                            if (lifecycle.started()) {
                                logger.warn("failed to connect to requesting node {}", e, requestingNode);
                            }
                        }
                    }
                });
            } else {
                transportService.sendRequest(requestingNode, ACTION_NAME, multicastPingResponse, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleException(TransportException exp) {
                        if (lifecycle.started()) {
                            logger.warn("failed to receive confirmation on sent ping response to [{}]", exp, requestingNode);
                        }
                    }
                });
            }
        }
    }
}
