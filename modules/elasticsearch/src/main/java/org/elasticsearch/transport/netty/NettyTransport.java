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

package org.elasticsearch.transport.netty;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.io.stream.BytesStreamOutput;
import org.elasticsearch.util.io.stream.HandlesStreamOutput;
import org.elasticsearch.util.io.stream.Streamable;
import org.elasticsearch.util.network.NetworkService;
import org.elasticsearch.util.network.NetworkUtils;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.transport.BoundTransportAddress;
import org.elasticsearch.util.transport.InetSocketTransportAddress;
import org.elasticsearch.util.transport.PortsRange;
import org.elasticsearch.util.transport.TransportAddress;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.logging.InternalLogger;
import org.jboss.netty.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.transport.Transport.Helper.*;
import static org.elasticsearch.util.TimeValue.*;
import static org.elasticsearch.util.collect.Lists.*;
import static org.elasticsearch.util.concurrent.ConcurrentCollections.*;
import static org.elasticsearch.util.concurrent.DynamicExecutors.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.util.transport.NetworkExceptionHelper.*;

/**
 * @author kimchy (shay.banon)
 */
public class NettyTransport extends AbstractLifecycleComponent<Transport> implements Transport {

    static {
        InternalLoggerFactory.setDefaultFactory(new NettyInternalESLoggerFactory() {
            @Override public InternalLogger newInstance(String name) {
                return super.newInstance(name.replace("org.jboss.netty.", "netty."));
            }
        });
    }

    private final NetworkService networkService;

    final int workerCount;

    final String port;

    final String bindHost;

    final String publishHost;

    final TimeValue connectTimeout;

    final int connectionsPerNode;

    final Boolean tcpNoDelay;

    final Boolean tcpKeepAlive;

    final Boolean reuseAddress;

    final SizeValue tcpSendBufferSize;

    final SizeValue tcpReceiveBufferSize;

    private final ThreadPool threadPool;

    private volatile OpenChannelsHandler serverOpenChannels;

    private volatile ClientBootstrap clientBootstrap;

    private volatile ServerBootstrap serverBootstrap;

    // node id to actual channel
    final ConcurrentMap<String, NodeConnections> connectedNodes = newConcurrentMap();


    private volatile Channel serverChannel;

    private volatile TransportServiceAdapter transportServiceAdapter;

    private volatile BoundTransportAddress boundAddress;

    public NettyTransport(ThreadPool threadPool) {
        this(EMPTY_SETTINGS, threadPool, new NetworkService(EMPTY_SETTINGS));
    }

    public NettyTransport(Settings settings, ThreadPool threadPool) {
        this(settings, threadPool, new NetworkService(settings));
    }

    @Inject public NettyTransport(Settings settings, ThreadPool threadPool, NetworkService networkService) {
        super(settings);
        this.threadPool = threadPool;
        this.networkService = networkService;

        this.workerCount = componentSettings.getAsInt("worker_count", Runtime.getRuntime().availableProcessors());
        this.port = componentSettings.get("port", "9300-9400");
        this.bindHost = componentSettings.get("bind_host");
        this.connectionsPerNode = componentSettings.getAsInt("connections_per_node", 5);
        this.publishHost = componentSettings.get("publish_host");
        this.connectTimeout = componentSettings.getAsTime("connect_timeout", timeValueSeconds(1));
        this.tcpNoDelay = componentSettings.getAsBoolean("tcp_no_delay", true);
        this.tcpKeepAlive = componentSettings.getAsBoolean("tcp_keep_alive", null);
        this.reuseAddress = componentSettings.getAsBoolean("reuse_address", NetworkUtils.defaultReuseAddress());
        this.tcpSendBufferSize = componentSettings.getAsSize("tcp_send_buffer_size", null);
        this.tcpReceiveBufferSize = componentSettings.getAsSize("tcp_receive_buffer_size", null);
    }

    public Settings settings() {
        return this.settings;
    }

    @Override public void transportServiceAdapter(TransportServiceAdapter service) {
        this.transportServiceAdapter = service;
    }

    TransportServiceAdapter transportServiceAdapter() {
        return transportServiceAdapter;
    }

    ThreadPool threadPool() {
        return threadPool;
    }

    @Override protected void doStart() throws ElasticSearchException {
        clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(daemonThreadFactory(settings, "transportClientBoss")),
                Executors.newCachedThreadPool(daemonThreadFactory(settings, "transportClientIoWorker")),
                workerCount));
        ChannelPipelineFactory clientPipelineFactory = new ChannelPipelineFactory() {
            @Override public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new SizeHeaderFrameDecoder());
                pipeline.addLast("dispatcher", new MessageChannelHandler(NettyTransport.this, logger));
                return pipeline;
            }
        };
        clientBootstrap.setPipelineFactory(clientPipelineFactory);
        clientBootstrap.setOption("connectTimeoutMillis", connectTimeout.millis());
        if (tcpNoDelay != null) {
            clientBootstrap.setOption("tcpNoDelay", tcpNoDelay);
        }
        if (tcpKeepAlive != null) {
            clientBootstrap.setOption("keepAlive", tcpKeepAlive);
        }
        if (tcpSendBufferSize != null) {
            clientBootstrap.setOption("sendBufferSize", tcpSendBufferSize.bytes());
        }
        if (tcpReceiveBufferSize != null) {
            clientBootstrap.setOption("receiveBufferSize", tcpReceiveBufferSize.bytes());
        }
        if (reuseAddress != null) {
            clientBootstrap.setOption("reuseAddress", reuseAddress);
        }

        if (!settings.getAsBoolean("network.server", true)) {
            return;
        }

        serverOpenChannels = new OpenChannelsHandler();
        serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(daemonThreadFactory(settings, "transportServerBoss")),
                Executors.newCachedThreadPool(daemonThreadFactory(settings, "transportServerIoWorker")),
                workerCount));
        ChannelPipelineFactory serverPipelineFactory = new ChannelPipelineFactory() {
            @Override public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("openChannels", serverOpenChannels);
                pipeline.addLast("decoder", new SizeHeaderFrameDecoder());
                pipeline.addLast("dispatcher", new MessageChannelHandler(NettyTransport.this, logger));
                return pipeline;
            }
        };
        serverBootstrap.setPipelineFactory(serverPipelineFactory);
        if (tcpNoDelay != null) {
            serverBootstrap.setOption("child.tcpNoDelay", tcpNoDelay);
        }
        if (tcpKeepAlive != null) {
            serverBootstrap.setOption("child.keepAlive", tcpKeepAlive);
        }
        if (tcpSendBufferSize != null) {
            serverBootstrap.setOption("child.sendBufferSize", tcpSendBufferSize.bytes());
        }
        if (tcpReceiveBufferSize != null) {
            serverBootstrap.setOption("child.receiveBufferSize", tcpReceiveBufferSize.bytes());
        }
        if (reuseAddress != null) {
            serverBootstrap.setOption("reuseAddress", reuseAddress);
            serverBootstrap.setOption("child.reuseAddress", reuseAddress);
        }

        // Bind and start to accept incoming connections.
        InetAddress hostAddressX;
        try {
            hostAddressX = networkService.resolveBindHostAddress(bindHost);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host [" + bindHost + "]", e);
        }
        final InetAddress hostAddress = hostAddressX;

        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override public boolean onPortNumber(int portNumber) {
                try {
                    serverChannel = serverBootstrap.bind(new InetSocketAddress(hostAddress, portNumber));
                } catch (Exception e) {
                    lastException.set(e);
                    return false;
                }
                return true;
            }
        });
        if (!success) {
            throw new BindTransportException("Failed to bind to [" + port + "]", lastException.get());
        }

        logger.debug("Bound to address [{}]", serverChannel.getLocalAddress());

        InetSocketAddress boundAddress = (InetSocketAddress) serverChannel.getLocalAddress();
        InetSocketAddress publishAddress;
        try {
            publishAddress = new InetSocketAddress(networkService.resolvePublishHostAddress(publishHost), boundAddress.getPort());
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
        this.boundAddress = new BoundTransportAddress(new InetSocketTransportAddress(boundAddress), new InetSocketTransportAddress(publishAddress));
    }

    @Override protected void doStop() throws ElasticSearchException {
        if (serverChannel != null) {
            try {
                serverChannel.close().awaitUninterruptibly();
            } finally {
                serverChannel = null;
            }
        }

        if (serverOpenChannels != null) {
            serverOpenChannels.close();
            serverOpenChannels = null;
        }

        if (serverBootstrap != null) {
            serverBootstrap.releaseExternalResources();
            serverBootstrap = null;
        }

        for (Iterator<NodeConnections> it = connectedNodes.values().iterator(); it.hasNext();) {
            NodeConnections nodeConnections = it.next();
            it.remove();
            nodeConnections.close();
        }

        if (clientBootstrap != null) {
            // HACK, make sure we try and close open client channels also after
            // we releaseExternalResources, they seem to hang when there are open client channels
            ScheduledFuture<?> scheduledFuture = threadPool.schedule(new Runnable() {
                @Override public void run() {
                    try {
                        for (Iterator<NodeConnections> it = connectedNodes.values().iterator(); it.hasNext();) {
                            NodeConnections nodeConnections = it.next();
                            it.remove();
                            nodeConnections.close();
                        }
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }, 500, TimeUnit.MILLISECONDS);
            clientBootstrap.releaseExternalResources();
            scheduledFuture.cancel(false);
            clientBootstrap = null;
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    @Override public TransportAddress[] addressesFromString(String address) throws Exception {
        int index = address.indexOf('[');
        if (index != -1) {
            String host = address.substring(0, index);
            Set<String> ports = Strings.commaDelimitedListToSet(address.substring(index + 1, address.indexOf(']')));
            List<TransportAddress> addresses = Lists.newArrayList();
            for (String port : ports) {
                int[] iPorts = new PortsRange(port).ports();
                for (int iPort : iPorts) {
                    addresses.add(new InetSocketTransportAddress(host, iPort));
                }
            }
            return addresses.toArray(new TransportAddress[addresses.size()]);
        } else {
            index = address.lastIndexOf(':');
            if (index == -1) {
                throw new ElasticSearchIllegalStateException("Port must be provided to create inet address from [" + address + "]");
            }
            String host = address.substring(0, index);
            int port = Integer.parseInt(address.substring(index + 1));
            return new TransportAddress[]{new InetSocketTransportAddress(host, port)};
        }
    }

    @Override public boolean addressSupported(Class<? extends TransportAddress> address) {
        return InetSocketTransportAddress.class.equals(address);
    }

    @Override public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if (!lifecycle.started()) {
            // ignore
        }
        if (isCloseConnectionException(e.getCause()) || isConnectException(e.getCause())) {
            if (logger.isTraceEnabled()) {
                logger.trace("(Ignoring) Exception caught on netty layer [" + ctx.getChannel() + "]", e.getCause());
            }
        } else {
            logger.warn("Exception caught on netty layer [" + ctx.getChannel() + "]", e.getCause());
        }
    }

    TransportAddress wrapAddress(SocketAddress socketAddress) {
        return new InetSocketTransportAddress((InetSocketAddress) socketAddress);
    }

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    @Override public <T extends Streamable> void sendRequest(DiscoveryNode node, long requestId, String action,
                                                             Streamable streamable, final TransportResponseHandler<T> handler) throws IOException, TransportException {

        Channel targetChannel = nodeChannel(node);

        HandlesStreamOutput stream = BytesStreamOutput.Cached.cachedHandles();
        stream.writeBytes(LENGTH_PLACEHOLDER); // fake size

        stream.writeLong(requestId);
        byte status = 0;
        status = setRequest(status);
        stream.writeByte(status); // 0 for request, 1 for response.

        stream.writeUTF(action);
        streamable.writeTo(stream);

        byte[] data = ((BytesStreamOutput) stream.wrappedOut()).copiedByteArray();
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(data);

        int size = buffer.writerIndex() - 4;
        if (size == 0) {
            handler.handleException(new RemoteTransportException("", new FailedCommunicationException("Trying to send a stream with 0 size")));
        }
        buffer.setInt(0, size); // update real size.
        ChannelFuture channelFuture = targetChannel.write(buffer);
        // TODO do we need this listener?
//        channelFuture.addListener(new ChannelFutureListener() {
//            @Override public void operationComplete(ChannelFuture future) throws Exception {
//                if (!future.isSuccess()) {
//                    // maybe add back the retry?
//                    handler.handleException(new RemoteTransportException("", new FailedCommunicationException("Error sending request", future.getCause())));
//                }
//            }
//        });
    }

    @Override public boolean nodeConnected(DiscoveryNode node) {
        return connectedNodes.containsKey(node.id());
    }

    @Override public void connectToNode(DiscoveryNode node) {
        if (!lifecycle.started()) {
            throw new ElasticSearchIllegalStateException("Can't add nodes to a stopped transport");
        }
        try {
            if (node == null) {
                throw new ConnectTransportException(node, "Can't connect to a null node");
            }
            NodeConnections nodeConnections = connectedNodes.get(node.id());
            if (nodeConnections != null) {
                return;
            }
            synchronized (this) {
                // recheck here, within the sync block (we cache connections, so we don't care about this single sync block)
                nodeConnections = connectedNodes.get(node.id());
                if (nodeConnections != null) {
                    return;
                }
                List<ChannelFuture> connectFutures = newArrayList();
                for (int connectionIndex = 0; connectionIndex < connectionsPerNode; connectionIndex++) {
                    InetSocketAddress address = ((InetSocketTransportAddress) node.address()).address();
                    connectFutures.add(clientBootstrap.connect(address));

                }
                List<Channel> channels = newArrayList();
                Throwable lastConnectException = null;
                for (ChannelFuture connectFuture : connectFutures) {
                    if (!lifecycle.started()) {
                        for (Channel channel : channels) {
                            channel.close().awaitUninterruptibly();
                        }
                        throw new ConnectTransportException(node, "Can't connect when the transport is stopped");
                    }
                    connectFuture.awaitUninterruptibly((long) (connectTimeout.millis() * 1.25));
                    if (!connectFuture.isSuccess()) {
                        lastConnectException = connectFuture.getCause();
                    } else {
                        Channel channel = connectFuture.getChannel();
                        channel.getCloseFuture().addListener(new ChannelCloseListener(node.id()));
                        channels.add(channel);
                    }
                }
                if (channels.isEmpty()) {
                    if (lastConnectException != null) {
                        throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", lastConnectException);
                    }
                    throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "], reason unknown");
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Connected to node[{}], number_of_connections[{}]", node, channels.size());
                }
                connectedNodes.put(node.id(), new NodeConnections(node, channels.toArray(new Channel[channels.size()])));
                transportServiceAdapter.raiseNodeConnected(node);
            }
        } catch (Exception e) {
            throw new ConnectTransportException(node, "General node connection failure", e);
        }
    }

    @Override public void disconnectFromNode(DiscoveryNode node) {
        NodeConnections nodeConnections = connectedNodes.remove(node.id());
        if (nodeConnections != null) {
            nodeConnections.close();
        }
    }

    private Channel nodeChannel(DiscoveryNode node) throws ConnectTransportException {
        NettyTransport.NodeConnections nodeConnections = connectedNodes.get(node.id());
        if (nodeConnections == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        Channel channel = nodeConnections.channel();
        if (channel == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        return channel;
    }

    public class NodeConnections {

        private final DiscoveryNode node;

        private final AtomicInteger counter = new AtomicInteger();

        private volatile Channel[] channels;

        private volatile boolean closed = false;

        private NodeConnections(DiscoveryNode node, Channel[] channels) {
            this.node = node;
            this.channels = channels;
        }

        private Channel channel() {
            return channels[Math.abs(counter.incrementAndGet()) % channels.length];
        }

        private void channelClosed(Channel closedChannel) {
            List<Channel> updated = newArrayList();
            for (Channel channel : channels) {
                if (!channel.getId().equals(closedChannel.getId())) {
                    updated.add(channel);
                }
            }
            this.channels = updated.toArray(new Channel[updated.size()]);
        }

        private int numberOfChannels() {
            return channels.length;
        }

        private synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            Channel[] channelsToClose = channels;
            channels = new Channel[0];
            for (Channel channel : channelsToClose) {
                if (channel.isOpen()) {
                    channel.close().awaitUninterruptibly();
                }
            }
            logger.debug("Disconnected from [{}]", node);
            transportServiceAdapter.raiseNodeDisconnected(node);
        }
    }

    private class ChannelCloseListener implements ChannelFutureListener {

        private final String nodeId;

        private ChannelCloseListener(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override public void operationComplete(ChannelFuture future) throws Exception {
            final NodeConnections nodeConnections = connectedNodes.get(nodeId);
            if (nodeConnections != null) {
                nodeConnections.channelClosed(future.getChannel());
                if (nodeConnections.numberOfChannels() == 0) {
                    // all the channels in the node connections are closed, remove it from
                    // our client channels
                    connectedNodes.remove(nodeId);
                    // and close it
                    nodeConnections.close();
                }
            }
        }
    }
}
