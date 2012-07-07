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

package org.elasticsearch.transport.netty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.netty.NettyStaticSetup;
import org.elasticsearch.common.netty.OpenChannelsHandler;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.support.TransportStreams;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.network.NetworkService.TcpSettings.*;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isCloseConnectionException;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isConnectException;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * There are 3 types of connections per node, low/med/high. Low if for batch oriented APIs (like recovery or
 * batch) with high payload that will cause regular request. (like search or single index) to take
 * longer. Med is for the typical search / single doc index. And High is for ping type requests (like FD).
 */
public class NettyTransport extends AbstractLifecycleComponent<Transport> implements Transport {

    static {
        NettyStaticSetup.setup();
    }

    private final NetworkService networkService;

    final int workerCount;

    final boolean blockingServer;

    final boolean blockingClient;

    final String port;

    final String bindHost;

    final String publishHost;

    final boolean compress;

    final TimeValue connectTimeout;

    final Boolean tcpNoDelay;

    final Boolean tcpKeepAlive;

    final Boolean reuseAddress;

    final ByteSizeValue tcpSendBufferSize;
    final ByteSizeValue tcpReceiveBufferSize;

    final int connectionsPerNodeLow;
    final int connectionsPerNodeMed;
    final int connectionsPerNodeHigh;

    final ByteSizeValue maxCumulationBufferCapacity;
    final int maxCompositeBufferComponents;

    private final ThreadPool threadPool;

    private volatile OpenChannelsHandler serverOpenChannels;

    private volatile ClientBootstrap clientBootstrap;

    private volatile ServerBootstrap serverBootstrap;

    // node id to actual channel
    final ConcurrentMap<DiscoveryNode, NodeChannels> connectedNodes = newConcurrentMap();


    private volatile Channel serverChannel;

    private volatile TransportServiceAdapter transportServiceAdapter;

    private volatile BoundTransportAddress boundAddress;

    private final Object[] connectMutex;

    public NettyTransport(ThreadPool threadPool) {
        this(EMPTY_SETTINGS, threadPool, new NetworkService(EMPTY_SETTINGS));
    }

    public NettyTransport(Settings settings, ThreadPool threadPool) {
        this(settings, threadPool, new NetworkService(settings));
    }

    @Inject
    public NettyTransport(Settings settings, ThreadPool threadPool, NetworkService networkService) {
        super(settings);
        this.threadPool = threadPool;
        this.networkService = networkService;

        this.connectMutex = new Object[500];
        for (int i = 0; i < connectMutex.length; i++) {
            connectMutex[i] = new Object();
        }

        this.workerCount = componentSettings.getAsInt("worker_count", Runtime.getRuntime().availableProcessors() * 2);
        this.blockingServer = settings.getAsBoolean("transport.tcp.blocking_server", settings.getAsBoolean(TCP_BLOCKING_SERVER, settings.getAsBoolean(TCP_BLOCKING, false)));
        this.blockingClient = settings.getAsBoolean("transport.tcp.blocking_client", settings.getAsBoolean(TCP_BLOCKING_CLIENT, settings.getAsBoolean(TCP_BLOCKING, false)));
        this.port = componentSettings.get("port", settings.get("transport.tcp.port", "9300-9400"));
        this.bindHost = componentSettings.get("bind_host", settings.get("transport.bind_host", settings.get("transport.host")));
        this.publishHost = componentSettings.get("publish_host", settings.get("transport.publish_host", settings.get("transport.host")));
        this.compress = settings.getAsBoolean("transport.tcp.compress", false);
        this.connectTimeout = componentSettings.getAsTime("connect_timeout", settings.getAsTime("transport.tcp.connect_timeout", settings.getAsTime(TCP_CONNECT_TIMEOUT, TCP_DEFAULT_CONNECT_TIMEOUT)));
        this.tcpNoDelay = componentSettings.getAsBoolean("tcp_no_delay", settings.getAsBoolean(TCP_NO_DELAY, true));
        this.tcpKeepAlive = componentSettings.getAsBoolean("tcp_keep_alive", settings.getAsBoolean(TCP_KEEP_ALIVE, true));
        this.reuseAddress = componentSettings.getAsBoolean("reuse_address", settings.getAsBoolean(TCP_REUSE_ADDRESS, NetworkUtils.defaultReuseAddress()));
        this.tcpSendBufferSize = componentSettings.getAsBytesSize("tcp_send_buffer_size", settings.getAsBytesSize(TCP_SEND_BUFFER_SIZE, TCP_DEFAULT_SEND_BUFFER_SIZE));
        this.tcpReceiveBufferSize = componentSettings.getAsBytesSize("tcp_receive_buffer_size", settings.getAsBytesSize(TCP_RECEIVE_BUFFER_SIZE, TCP_DEFAULT_RECEIVE_BUFFER_SIZE));
        this.connectionsPerNodeLow = componentSettings.getAsInt("connections_per_node.low", settings.getAsInt("transport.connections_per_node.low", 2));
        this.connectionsPerNodeMed = componentSettings.getAsInt("connections_per_node.med", settings.getAsInt("transport.connections_per_node.med", 6));
        this.connectionsPerNodeHigh = componentSettings.getAsInt("connections_per_node.high", settings.getAsInt("transport.connections_per_node.high", 1));

        this.maxCumulationBufferCapacity = componentSettings.getAsBytesSize("max_cumulation_buffer_capacity", null);
        this.maxCompositeBufferComponents = componentSettings.getAsInt("max_composite_buffer_components", -1);

        logger.debug("using worker_count[{}], port[{}], bind_host[{}], publish_host[{}], compress[{}], connect_timeout[{}], connections_per_node[{}/{}/{}]",
                workerCount, port, bindHost, publishHost, compress, connectTimeout, connectionsPerNodeLow, connectionsPerNodeMed, connectionsPerNodeHigh);
    }

    public Settings settings() {
        return this.settings;
    }

    @Override
    public void transportServiceAdapter(TransportServiceAdapter service) {
        this.transportServiceAdapter = service;
    }

    TransportServiceAdapter transportServiceAdapter() {
        return transportServiceAdapter;
    }

    ThreadPool threadPool() {
        return threadPool;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        if (blockingClient) {
            clientBootstrap = new ClientBootstrap(new OioClientSocketChannelFactory(Executors.newCachedThreadPool(daemonThreadFactory(settings, "transport_client_worker"))));
        } else {
            clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "transport_client_boss")),
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "transport_client_worker")),
                    workerCount));
        }
        ChannelPipelineFactory clientPipelineFactory = new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                SizeHeaderFrameDecoder sizeHeader = new SizeHeaderFrameDecoder();
                if (maxCumulationBufferCapacity != null) {
                    if (maxCumulationBufferCapacity.bytes() > Integer.MAX_VALUE) {
                        sizeHeader.setMaxCumulationBufferCapacity(Integer.MAX_VALUE);
                    } else {
                        sizeHeader.setMaxCumulationBufferCapacity((int) maxCumulationBufferCapacity.bytes());
                    }
                }
                if (maxCompositeBufferComponents != -1) {
                    sizeHeader.setMaxCumulationBufferComponents(maxCompositeBufferComponents);
                }
                pipeline.addLast("size", sizeHeader);
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
        if (tcpSendBufferSize != null && tcpSendBufferSize.bytes() > 0) {
            clientBootstrap.setOption("sendBufferSize", tcpSendBufferSize.bytes());
        }
        if (tcpReceiveBufferSize != null && tcpReceiveBufferSize.bytes() > 0) {
            clientBootstrap.setOption("receiveBufferSize", tcpReceiveBufferSize.bytes());
        }
        if (reuseAddress != null) {
            clientBootstrap.setOption("reuseAddress", reuseAddress);
        }

        if (!settings.getAsBoolean("network.server", true)) {
            return;
        }

        serverOpenChannels = new OpenChannelsHandler(logger);
        if (blockingServer) {
            serverBootstrap = new ServerBootstrap(new OioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "transport_server_boss")),
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "transport_server_worker"))
            ));
        } else {
            serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "transport_server_boss")),
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "transport_server_worker")),
                    workerCount));
        }
        ChannelPipelineFactory serverPipelineFactory = new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("openChannels", serverOpenChannels);
                SizeHeaderFrameDecoder sizeHeader = new SizeHeaderFrameDecoder();
                if (maxCumulationBufferCapacity != null) {
                    if (maxCumulationBufferCapacity.bytes() > Integer.MAX_VALUE) {
                        sizeHeader.setMaxCumulationBufferCapacity(Integer.MAX_VALUE);
                    } else {
                        sizeHeader.setMaxCumulationBufferCapacity((int) maxCumulationBufferCapacity.bytes());
                    }
                }
                if (maxCompositeBufferComponents != -1) {
                    sizeHeader.setMaxCumulationBufferComponents(maxCompositeBufferComponents);
                }
                pipeline.addLast("size", sizeHeader);
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
        if (tcpSendBufferSize != null && tcpSendBufferSize.bytes() > 0) {
            serverBootstrap.setOption("child.sendBufferSize", tcpSendBufferSize.bytes());
        }
        if (tcpReceiveBufferSize != null && tcpReceiveBufferSize.bytes() > 0) {
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
            @Override
            public boolean onPortNumber(int portNumber) {
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

    @Override
    protected void doStop() throws ElasticSearchException {
        final CountDownLatch latch = new CountDownLatch(1);
        // make sure we run it on another thread than a possible IO handler thread
        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    for (Iterator<NodeChannels> it = connectedNodes.values().iterator(); it.hasNext(); ) {
                        NodeChannels nodeChannels = it.next();
                        it.remove();
                        nodeChannels.close();
                    }

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

                    for (Iterator<NodeChannels> it = connectedNodes.values().iterator(); it.hasNext(); ) {
                        NodeChannels nodeChannels = it.next();
                        it.remove();
                        nodeChannels.close();
                    }

                    if (clientBootstrap != null) {
                        clientBootstrap.releaseExternalResources();
                        clientBootstrap = null;
                    }
                } finally {
                    latch.countDown();
                }
            }
        });

        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    @Override
    public TransportAddress[] addressesFromString(String address) throws Exception {
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
                List<TransportAddress> addresses = Lists.newArrayList();
                int[] iPorts = new PortsRange(this.port).ports();
                for (int iPort : iPorts) {
                    addresses.add(new InetSocketTransportAddress(address, iPort));
                }
                return addresses.toArray(new TransportAddress[addresses.size()]);
            } else {
                String host = address.substring(0, index);
                int port = Integer.parseInt(address.substring(index + 1));
                return new TransportAddress[]{new InetSocketTransportAddress(host, port)};
            }
        }
    }

    @Override
    public boolean addressSupported(Class<? extends TransportAddress> address) {
        return InetSocketTransportAddress.class.equals(address);
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if (!lifecycle.started()) {
            // ignore
        }
        if (isCloseConnectionException(e.getCause())) {
            // disconnect the node
            Channel channel = ctx.getChannel();
            for (Map.Entry<DiscoveryNode, NodeChannels> entry : connectedNodes.entrySet()) {
                if (entry.getValue().hasChannel(channel)) {
                    disconnectFromNode(entry.getKey());
                }
            }
        } else if (isConnectException(e.getCause())) {
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

    @Override
    public long serverOpen() {
        OpenChannelsHandler channels = serverOpenChannels;
        return channels == null ? 0 : channels.numberOfOpenChannels();
    }

    @Override
    public <T extends Streamable> void sendRequest(final DiscoveryNode node, final long requestId, final String action, final Streamable message, TransportRequestOptions options) throws IOException, TransportException {
        Channel targetChannel = nodeChannel(node, options);

        if (compress) {
            options.withCompress(true);
        }

        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
        TransportStreams.buildRequest(cachedEntry, requestId, action, message, options);
        BytesReference bytes = cachedEntry.bytes().bytes();
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(bytes.array(), bytes.arrayOffset(), bytes.length());
        ChannelFuture future = targetChannel.write(buffer);
        future.addListener(new CacheFutureListener(cachedEntry));
        // We handle close connection exception in the #exceptionCaught method, which is the main reason we want to add this future
//        channelFuture.addListener(new ChannelFutureListener() {
//            @Override public void operationComplete(ChannelFuture future) throws Exception {
//                if (!future.isSuccess()) {
//                    // maybe add back the retry?
//                    TransportResponseHandler handler = transportServiceAdapter.remove(requestId);
//                    if (handler != null) {
//                        handler.handleException(new RemoteTransportException("Failed write request", new SendRequestTransportException(node, action, future.getCause())));
//                    }
//                }
//            }
//        });
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return connectedNodes.containsKey(node);
    }

    @Override
    public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {
        connectToNode(node, true);
    }

    @Override
    public void connectToNode(DiscoveryNode node) {
        connectToNode(node, false);
    }

    public void connectToNode(DiscoveryNode node, boolean light) {
        if (!lifecycle.started()) {
            throw new ElasticSearchIllegalStateException("Can't add nodes to a stopped transport");
        }
        if (node == null) {
            throw new ConnectTransportException(null, "Can't connect to a null node");
        }
        synchronized (connectLock(node.id())) {
            try {
                NodeChannels nodeChannels = connectedNodes.get(node);
                if (nodeChannels != null) {
                    return;
                }

                if (light) {
                    nodeChannels = connectToChannelsLight(node);
                } else {
                    nodeChannels = new NodeChannels(new Channel[connectionsPerNodeLow], new Channel[connectionsPerNodeMed], new Channel[connectionsPerNodeHigh]);
                    try {
                        connectToChannels(nodeChannels, node);
                    } catch (Exception e) {
                        nodeChannels.close();
                        throw e;
                    }
                }

                NodeChannels existing = connectedNodes.putIfAbsent(node, nodeChannels);
                if (existing != null) {
                    // we are already connected to a node, close this ones
                    nodeChannels.close();
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("connected to node [{}]", node);
                    }
                    transportServiceAdapter.raiseNodeConnected(node);
                }

            } catch (ConnectTransportException e) {
                throw e;
            } catch (Exception e) {
                throw new ConnectTransportException(node, "General node connection failure", e);
            }
        }
    }

    private NodeChannels connectToChannelsLight(DiscoveryNode node) {
        InetSocketAddress address = ((InetSocketTransportAddress) node.address()).address();
        ChannelFuture connect = clientBootstrap.connect(address);
        connect.awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
        if (!connect.isSuccess()) {
            throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", connect.getCause());
        }
        Channel[] channels = new Channel[1];
        channels[0] = connect.getChannel();
        channels[0].getCloseFuture().addListener(new ChannelCloseListener(node));
        return new NodeChannels(channels, channels, channels);
    }

    private void connectToChannels(NodeChannels nodeChannels, DiscoveryNode node) {
        ChannelFuture[] connectLow = new ChannelFuture[nodeChannels.low.length];
        ChannelFuture[] connectMed = new ChannelFuture[nodeChannels.med.length];
        ChannelFuture[] connectHigh = new ChannelFuture[nodeChannels.high.length];
        InetSocketAddress address = ((InetSocketTransportAddress) node.address()).address();
        for (int i = 0; i < connectLow.length; i++) {
            connectLow[i] = clientBootstrap.connect(address);
        }
        for (int i = 0; i < connectMed.length; i++) {
            connectMed[i] = clientBootstrap.connect(address);
        }
        for (int i = 0; i < connectHigh.length; i++) {
            connectHigh[i] = clientBootstrap.connect(address);
        }

        try {
            for (int i = 0; i < connectLow.length; i++) {
                connectLow[i].awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
                if (!connectLow[i].isSuccess()) {
                    throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", connectLow[i].getCause());
                }
                nodeChannels.low[i] = connectLow[i].getChannel();
                nodeChannels.low[i].getCloseFuture().addListener(new ChannelCloseListener(node));
            }

            for (int i = 0; i < connectMed.length; i++) {
                connectMed[i].awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
                if (!connectMed[i].isSuccess()) {
                    throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", connectMed[i].getCause());
                }
                nodeChannels.med[i] = connectMed[i].getChannel();
                nodeChannels.med[i].getCloseFuture().addListener(new ChannelCloseListener(node));
            }

            for (int i = 0; i < connectHigh.length; i++) {
                connectHigh[i].awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
                if (!connectHigh[i].isSuccess()) {
                    throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", connectHigh[i].getCause());
                }
                nodeChannels.high[i] = connectHigh[i].getChannel();
                nodeChannels.high[i].getCloseFuture().addListener(new ChannelCloseListener(node));
            }

            if (nodeChannels.low.length == 0) {
                if (nodeChannels.med.length > 0) {
                    nodeChannels.low = nodeChannels.med;
                } else {
                    nodeChannels.low = nodeChannels.high;
                }
            }
            if (nodeChannels.med.length == 0) {
                if (nodeChannels.high.length > 0) {
                    nodeChannels.med = nodeChannels.high;
                } else {
                    nodeChannels.med = nodeChannels.low;
                }
            }
            if (nodeChannels.high.length == 0) {
                if (nodeChannels.med.length > 0) {
                    nodeChannels.high = nodeChannels.med;
                } else {
                    nodeChannels.high = nodeChannels.low;
                }
            }
        } catch (RuntimeException e) {
            // clean the futures
            for (ChannelFuture future : ImmutableList.<ChannelFuture>builder().add(connectLow).add(connectMed).add(connectHigh).build()) {
                future.cancel();
                if (future.getChannel() != null && future.getChannel().isOpen()) {
                    try {
                        future.getChannel().close();
                    } catch (Exception e1) {
                        // ignore
                    }
                }
            }
            throw e;
        }
    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        synchronized (connectLock(node.id())) {
            NodeChannels nodeChannels = connectedNodes.remove(node);
            if (nodeChannels != null) {
                try {
                    nodeChannels.close();
                } finally {
                    logger.debug("disconnected from [{}]", node);
                    transportServiceAdapter.raiseNodeDisconnected(node);
                }
            }
        }
    }

    private Channel nodeChannel(DiscoveryNode node, TransportRequestOptions options) throws ConnectTransportException {
        NodeChannels nodeChannels = connectedNodes.get(node);
        if (nodeChannels == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        return nodeChannels.channel(options.type());
    }

    private Object connectLock(String nodeId) {
        int hash = nodeId.hashCode();
        // abs returns Integer.MIN_VALUE, so we need to protect against it...
        if (hash == Integer.MIN_VALUE) {
            hash = 0;
        }
        return connectMutex[Math.abs(hash) % connectMutex.length];
    }

    private class ChannelCloseListener implements ChannelFutureListener {

        private final DiscoveryNode node;

        private ChannelCloseListener(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            disconnectFromNode(node);
        }
    }

    public static class NodeChannels {

        private Channel[] low;
        private final AtomicInteger lowCounter = new AtomicInteger();
        private Channel[] med;
        private final AtomicInteger medCounter = new AtomicInteger();
        private Channel[] high;
        private final AtomicInteger highCounter = new AtomicInteger();

        public NodeChannels(Channel[] low, Channel[] med, Channel[] high) {
            this.low = low;
            this.med = med;
            this.high = high;
        }

        public boolean hasChannel(Channel channel) {
            return hasChannel(channel, low) || hasChannel(channel, med) || hasChannel(channel, high);
        }

        private boolean hasChannel(Channel channel, Channel[] channels) {
            for (Channel channel1 : channels) {
                if (channel.equals(channel1)) {
                    return true;
                }
            }
            return false;
        }

        public Channel channel(TransportRequestOptions.Type type) {
            if (type == TransportRequestOptions.Type.MED) {
                return med[Math.abs(medCounter.incrementAndGet()) % med.length];
            } else if (type == TransportRequestOptions.Type.HIGH) {
                return high[Math.abs(highCounter.incrementAndGet()) % high.length];
            } else {
                return low[Math.abs(lowCounter.incrementAndGet()) % low.length];
            }
        }

        public synchronized void close() {
            List<ChannelFuture> futures = new ArrayList<ChannelFuture>();
            closeChannelsAndWait(low, futures);
            closeChannelsAndWait(med, futures);
            closeChannelsAndWait(high, futures);
            for (ChannelFuture future : futures) {
                future.awaitUninterruptibly();
            }
        }

        private void closeChannelsAndWait(Channel[] channels, List<ChannelFuture> futures) {
            for (Channel channel : channels) {
                try {
                    if (channel != null && channel.isOpen()) {
                        futures.add(channel.close());
                    }
                } catch (Exception e) {
                    //ignore
                }
            }
        }
    }

    public static class CacheFutureListener implements ChannelFutureListener {

        private final CachedStreamOutput.Entry cachedEntry;

        public CacheFutureListener(CachedStreamOutput.Entry cachedEntry) {
            this.cachedEntry = cachedEntry;
        }

        @Override
        public void operationComplete(ChannelFuture channelFuture) throws Exception {
            CachedStreamOutput.pushEntry(cachedEntry);
        }
    }

}
