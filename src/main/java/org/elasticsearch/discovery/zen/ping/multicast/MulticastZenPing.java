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

package org.elasticsearch.discovery.zen.ping.multicast;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.*;
import org.elasticsearch.common.network.MulticastChannel;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.node.DiscoveryNode.readNode;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

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
    private final Version version;
    private volatile DiscoveryNodesProvider nodesProvider;

    private final boolean pingEnabled;

    private volatile MulticastChannel multicastChannel;

    private final AtomicInteger pingIdGenerator = new AtomicInteger();
    private final Map<Integer, ConcurrentMap<DiscoveryNode, PingResponse>> receivedResponses = newConcurrentMap();

    public MulticastZenPing(ThreadPool threadPool, TransportService transportService, ClusterName clusterName, Version version) {
        this(EMPTY_SETTINGS, threadPool, transportService, clusterName, new NetworkService(EMPTY_SETTINGS), version);
    }

    public MulticastZenPing(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterName clusterName, NetworkService networkService, Version version) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterName = clusterName;
        this.networkService = networkService;
        this.version = version;

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
            throw new ElasticsearchIllegalStateException("Can't set nodes provider when started");
        }
        this.nodesProvider = nodesProvider;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        try {
            // we know OSX has bugs in the JVM when creating multiple instances of multicast sockets
            // causing for "socket close" exceptions when receive and/or crashes
            boolean shared = componentSettings.getAsBoolean("shared", Constants.MAC_OS_X);
            multicastChannel = MulticastChannel.getChannel(nodeName(), shared,
                    new MulticastChannel.Config(port, group, bufferSize, ttl, networkService.resolvePublishHostAddress(address)),
                    new Receiver());
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("multicast failed to start [{}], disabling", t, ExceptionsHelper.detailedMessage(t));
            } else {
                logger.info("multicast failed to start [{}], disabling", ExceptionsHelper.detailedMessage(t));
            }
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if (multicastChannel != null) {
            multicastChannel.close();
            multicastChannel = null;
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
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
        if (!pingEnabled) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    listener.onPing(PingResponse.EMPTY);
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
        try {
            BytesStreamOutput bStream = new BytesStreamOutput();
            StreamOutput out = new HandlesStreamOutput(bStream);
            out.writeBytes(INTERNAL_HEADER);
            Version.writeVersion(version, out);
            out.writeInt(id);
            clusterName.writeTo(out);
            nodesProvider.nodes().localNode().writeTo(out);
            out.close();
            multicastChannel.send(bStream.bytes());
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
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    static class MulticastPingResponse extends TransportRequest {

        int id;

        PingResponse pingResponse;

        MulticastPingResponse() {
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
                    StreamInput input = CachedStreamInput.cachedHandles(new BytesStreamInput(new BytesArray(data.toBytes(), INTERNAL_HEADER.length, data.length() - INTERNAL_HEADER.length)));
                    Version version = Version.readVersion(input);
                    input.setVersion(version);
                    id = input.readInt();
                    clusterName = ClusterName.readClusterName(input);
                    requestingNodeX = readNode(input);
                } else {
                    xContentType = XContentFactory.xContentType(data);
                    if (xContentType != null) {
                        // an external ping
                        externalPingData = XContentFactory.xContent(xContentType)
                                .createParser(data)
                                .mapAndClose();
                    } else {
                        throw new ElasticsearchIllegalStateException("failed multicast message, probably message from previous version");
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
                builder.startObject("version").field("number", version.number()).field("snapshot_build", version.snapshot).endObject();
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
                multicastChannel.send(builder.bytes());
                if (logger.isTraceEnabled()) {
                    logger.trace("sending external ping response {}", builder.string());
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
                            transportService.sendRequest(requestingNode, MulticastPingResponseRequestHandler.ACTION, multicastPingResponse, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
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
                transportService.sendRequest(requestingNode, MulticastPingResponseRequestHandler.ACTION, multicastPingResponse, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
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
