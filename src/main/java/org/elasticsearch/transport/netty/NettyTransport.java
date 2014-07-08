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

package org.elasticsearch.transport.netty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.HandlesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.netty.NettyUtils;
import org.elasticsearch.common.netty.OpenChannelsHandler;
import org.elasticsearch.common.netty.ReleaseChannelFutureListener;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.support.TransportStatus;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.CompositeChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.channel.socket.oio.OioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.common.network.NetworkService.TcpSettings.*;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isCloseConnectionException;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isConnectException;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * There are 4 types of connections per node, low/med/high/ping. Low if for batch oriented APIs (like recovery or
 * batch) with high payload that will cause regular request. (like search or single index) to take
 * longer. Med is for the typical search / single doc index. And High for things like cluster state. Ping is reserved for
 * sending out ping requests to other nodes.
 */
public class NettyTransport extends AbstractLifecycleComponent<Transport> implements Transport {

    static {
        NettyUtils.setup();
    }

    public static final String WORKER_COUNT = "transport.netty.worker_count";
    public static final String CONNECTIONS_PER_NODE_RECOVERY = "transport.connections_per_node.recovery";
    public static final String CONNECTIONS_PER_NODE_BULK = "transport.connections_per_node.bulk";
    public static final String CONNECTIONS_PER_NODE_REG = "transport.connections_per_node.reg";
    public static final String CONNECTIONS_PER_NODE_STATE = "transport.connections_per_node.state";
    public static final String CONNECTIONS_PER_NODE_PING = "transport.connections_per_node.ping";

    private final NetworkService networkService;
    final Version version;

    final int workerCount;
    final int bossCount;

    final boolean blockingServer;

    final boolean blockingClient;

    final String port;

    final String bindHost;

    final String publishHost;

    final int publishPort;

    final boolean compress;

    final TimeValue connectTimeout;

    final Boolean tcpNoDelay;

    final Boolean tcpKeepAlive;

    final Boolean reuseAddress;

    final ByteSizeValue tcpSendBufferSize;
    final ByteSizeValue tcpReceiveBufferSize;
    final ReceiveBufferSizePredictorFactory receiveBufferSizePredictorFactory;

    final int connectionsPerNodeRecovery;
    final int connectionsPerNodeBulk;
    final int connectionsPerNodeReg;
    final int connectionsPerNodeState;
    final int connectionsPerNodePing;

    final ByteSizeValue maxCumulationBufferCapacity;
    final int maxCompositeBufferComponents;

    final BigArrays bigArrays;

    private final ThreadPool threadPool;

    private volatile OpenChannelsHandler serverOpenChannels;

    private volatile ClientBootstrap clientBootstrap;

    private volatile ServerBootstrap serverBootstrap;

    // node id to actual channel
    final ConcurrentMap<DiscoveryNode, NodeChannels> connectedNodes = newConcurrentMap();


    private volatile Channel serverChannel;

    private volatile TransportServiceAdapter transportServiceAdapter;

    private volatile BoundTransportAddress boundAddress;

    private final KeyedLock<String> connectionLock = new KeyedLock<>();

    // this lock is here to make sure we close this transport and disconnect all the client nodes
    // connections while no connect operations is going on... (this might help with 100% CPU when stopping the transport?)
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();

    @Inject
    public NettyTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays, Version version) {
        super(settings);
        this.threadPool = threadPool;
        this.networkService = networkService;
        this.bigArrays = bigArrays;
        this.version = version;

        if (settings.getAsBoolean("netty.epollBugWorkaround", false)) {
            System.setProperty("org.jboss.netty.epollBugWorkaround", "true");
        }

        this.workerCount = settings.getAsInt(WORKER_COUNT, EsExecutors.boundedNumberOfProcessors(settings) * 2);
        this.bossCount = componentSettings.getAsInt("boss_count", 1);
        this.blockingServer = settings.getAsBoolean("transport.tcp.blocking_server", settings.getAsBoolean(TCP_BLOCKING_SERVER, settings.getAsBoolean(TCP_BLOCKING, false)));
        this.blockingClient = settings.getAsBoolean("transport.tcp.blocking_client", settings.getAsBoolean(TCP_BLOCKING_CLIENT, settings.getAsBoolean(TCP_BLOCKING, false)));
        this.port = componentSettings.get("port", settings.get("transport.tcp.port", "9300-9400"));
        this.bindHost = componentSettings.get("bind_host", settings.get("transport.bind_host", settings.get("transport.host")));
        this.publishHost = componentSettings.get("publish_host", settings.get("transport.publish_host", settings.get("transport.host")));
        this.publishPort = componentSettings.getAsInt("publish_port", settings.getAsInt("transport.publish_port", 0));
        this.compress = settings.getAsBoolean(TransportSettings.TRANSPORT_TCP_COMPRESS, false);
        this.connectTimeout = componentSettings.getAsTime("connect_timeout", settings.getAsTime("transport.tcp.connect_timeout", settings.getAsTime(TCP_CONNECT_TIMEOUT, TCP_DEFAULT_CONNECT_TIMEOUT)));
        this.tcpNoDelay = componentSettings.getAsBoolean("tcp_no_delay", settings.getAsBoolean(TCP_NO_DELAY, true));
        this.tcpKeepAlive = componentSettings.getAsBoolean("tcp_keep_alive", settings.getAsBoolean(TCP_KEEP_ALIVE, true));
        this.reuseAddress = componentSettings.getAsBoolean("reuse_address", settings.getAsBoolean(TCP_REUSE_ADDRESS, NetworkUtils.defaultReuseAddress()));
        this.tcpSendBufferSize = componentSettings.getAsBytesSize("tcp_send_buffer_size", settings.getAsBytesSize(TCP_SEND_BUFFER_SIZE, TCP_DEFAULT_SEND_BUFFER_SIZE));
        this.tcpReceiveBufferSize = componentSettings.getAsBytesSize("tcp_receive_buffer_size", settings.getAsBytesSize(TCP_RECEIVE_BUFFER_SIZE, TCP_DEFAULT_RECEIVE_BUFFER_SIZE));
        this.connectionsPerNodeRecovery = componentSettings.getAsInt("connections_per_node.recovery", settings.getAsInt(CONNECTIONS_PER_NODE_RECOVERY, 2));
        this.connectionsPerNodeBulk = componentSettings.getAsInt("connections_per_node.bulk", settings.getAsInt(CONNECTIONS_PER_NODE_BULK, 3));
        this.connectionsPerNodeReg = componentSettings.getAsInt("connections_per_node.reg", settings.getAsInt(CONNECTIONS_PER_NODE_REG, 6));
        this.connectionsPerNodeState = componentSettings.getAsInt("connections_per_node.high", settings.getAsInt(CONNECTIONS_PER_NODE_STATE, 1));
        this.connectionsPerNodePing = componentSettings.getAsInt("connections_per_node.ping", settings.getAsInt(CONNECTIONS_PER_NODE_PING, 1));

        // we want to have at least 1 for reg/state/ping
        if (this.connectionsPerNodeReg == 0) {
            throw new ElasticsearchIllegalArgumentException("can't set [connection_per_node.reg] to 0");
        }
        if (this.connectionsPerNodePing == 0) {
            throw new ElasticsearchIllegalArgumentException("can't set [connection_per_node.ping] to 0");
        }
        if (this.connectionsPerNodeState == 0) {
            throw new ElasticsearchIllegalArgumentException("can't set [connection_per_node.state] to 0");
        }

        this.maxCumulationBufferCapacity = componentSettings.getAsBytesSize("max_cumulation_buffer_capacity", null);
        this.maxCompositeBufferComponents = componentSettings.getAsInt("max_composite_buffer_components", -1);

        long defaultReceiverPredictor = 512 * 1024;
        if (JvmInfo.jvmInfo().mem().directMemoryMax().bytes() > 0) {
            // we can guess a better default...
            long l = (long) ((0.3 * JvmInfo.jvmInfo().mem().directMemoryMax().bytes()) / workerCount);
            defaultReceiverPredictor = Math.min(defaultReceiverPredictor, Math.max(l, 64 * 1024));
        }

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        ByteSizeValue receivePredictorMin = componentSettings.getAsBytesSize("receive_predictor_min", componentSettings.getAsBytesSize("receive_predictor_size", new ByteSizeValue(defaultReceiverPredictor)));
        ByteSizeValue receivePredictorMax = componentSettings.getAsBytesSize("receive_predictor_max", componentSettings.getAsBytesSize("receive_predictor_size", new ByteSizeValue(defaultReceiverPredictor)));
        if (receivePredictorMax.bytes() == receivePredictorMin.bytes()) {
            receiveBufferSizePredictorFactory = new FixedReceiveBufferSizePredictorFactory((int) receivePredictorMax.bytes());
        } else {
            receiveBufferSizePredictorFactory = new AdaptiveReceiveBufferSizePredictorFactory((int) receivePredictorMin.bytes(), (int) receivePredictorMin.bytes(), (int) receivePredictorMax.bytes());
        }

        logger.debug("using worker_count[{}], port[{}], bind_host[{}], publish_host[{}], compress[{}], connect_timeout[{}], connections_per_node[{}/{}/{}/{}/{}], receive_predictor[{}->{}]",
                workerCount, port, bindHost, publishHost, compress, connectTimeout, connectionsPerNodeRecovery, connectionsPerNodeBulk, connectionsPerNodeReg, connectionsPerNodeState, connectionsPerNodePing, receivePredictorMin, receivePredictorMax);
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
    protected void doStart() throws ElasticsearchException {
        if (blockingClient) {
            clientBootstrap = new ClientBootstrap(new OioClientSocketChannelFactory(Executors.newCachedThreadPool(daemonThreadFactory(settings, "transport_client_worker"))));
        } else {
            clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, "transport_client_boss")),
                    bossCount,
                    new NioWorkerPool(Executors.newCachedThreadPool(daemonThreadFactory(settings, "transport_client_worker")), workerCount),
                    new HashedWheelTimer(daemonThreadFactory(settings, "transport_client_timer"))));
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
        clientBootstrap.setOption("receiveBufferSizePredictorFactory", receiveBufferSizePredictorFactory);
        if (reuseAddress != null) {
            clientBootstrap.setOption("reuseAddress", reuseAddress);
        }

        if (!settings.getAsBoolean("network.server", true)) {
            return;
        }

        final OpenChannelsHandler openChannels = new OpenChannelsHandler(logger);
        this.serverOpenChannels = openChannels;
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
                pipeline.addLast("openChannels", openChannels);
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
        serverBootstrap.setOption("receiveBufferSizePredictorFactory", receiveBufferSizePredictorFactory);
        serverBootstrap.setOption("child.receiveBufferSizePredictorFactory", receiveBufferSizePredictorFactory);
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
        final AtomicReference<Exception> lastException = new AtomicReference<>();
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
        int publishPort = this.publishPort;
        if (0 == publishPort) {
            publishPort = boundAddress.getPort();
        }
        try {
            publishAddress = new InetSocketAddress(networkService.resolvePublishHostAddress(publishHost), publishPort);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
        this.boundAddress = new BoundTransportAddress(new InetSocketTransportAddress(boundAddress), new InetSocketTransportAddress(publishAddress));
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        final CountDownLatch latch = new CountDownLatch(1);
        // make sure we run it on another thread than a possible IO handler thread
        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
                globalLock.writeLock().lock();
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
                    globalLock.writeLock().unlock();
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
    protected void doClose() throws ElasticsearchException {
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
            logger.trace("close connection exception caught on transport layer [{}], disconnecting from relevant node", e.getCause(), ctx.getChannel());
            // close the channel, which will cause a node to be disconnected if relevant
            ctx.getChannel().close();
            disconnectFromNodeChannel(ctx.getChannel(), e.getCause());
        } else if (isConnectException(e.getCause())) {
            logger.trace("connect exception caught on transport layer [{}]", e.getCause(), ctx.getChannel());
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            ctx.getChannel().close();
            disconnectFromNodeChannel(ctx.getChannel(), e.getCause());
        } else if (e.getCause() instanceof CancelledKeyException) {
            logger.trace("cancelled key exception caught on transport layer [{}], disconnecting from relevant node", e.getCause(), ctx.getChannel());
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            ctx.getChannel().close();
            disconnectFromNodeChannel(ctx.getChannel(), e.getCause());
        } else {
            logger.warn("exception caught on transport layer [{}], closing connection", e.getCause(), ctx.getChannel());
            // close the channel, which will cause a node to be disconnected if relevant
            ctx.getChannel().close();
            disconnectFromNodeChannel(ctx.getChannel(), e.getCause());
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
    public void sendRequest(final DiscoveryNode node, final long requestId, final String action, final TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
        Channel targetChannel = nodeChannel(node, options);

        if (compress) {
            options.withCompress(true);
        }

        byte status = 0;
        status = TransportStatus.setRequest(status);

        ReleasableBytesStreamOutput bStream = new ReleasableBytesStreamOutput(bigArrays);
        boolean addedReleaseListener = false;
        try {
            bStream.skip(NettyHeader.HEADER_SIZE);
            StreamOutput stream = bStream;
            // only compress if asked, and, the request is not bytes, since then only
            // the header part is compressed, and the "body" can't be extracted as compressed
            if (options.compress() && (!(request instanceof BytesTransportRequest))) {
                status = TransportStatus.setCompress(status);
                stream = CompressorFactory.defaultCompressor().streamOutput(stream);
            }
            stream = new HandlesStreamOutput(stream);

            // we pick the smallest of the 2, to support both backward and forward compatibility
            // note, this is the only place we need to do this, since from here on, we use the serialized version
            // as the version to use also when the node receiving this request will send the response with
            Version version = Version.smallest(this.version, node.version());

            stream.setVersion(version);
            stream.writeString(action);

            ReleasableBytesReference bytes;
            ChannelBuffer buffer;
            // it might be nice to somehow generalize this optimization, maybe a smart "paged" bytes output
            // that create paged channel buffers, but its tricky to know when to do it (where this option is
            // more explicit).
            if (request instanceof BytesTransportRequest) {
                BytesTransportRequest bRequest = (BytesTransportRequest) request;
                assert node.version().equals(bRequest.version());
                bRequest.writeThin(stream);
                stream.close();
                bytes = bStream.bytes();
                ChannelBuffer headerBuffer = bytes.toChannelBuffer();
                ChannelBuffer contentBuffer = bRequest.bytes().toChannelBuffer();
                buffer = NettyUtils.buildComposite(false, headerBuffer, contentBuffer);
            } else {
                request.writeTo(stream);
                stream.close();
                bytes = bStream.bytes();
                buffer = bytes.toChannelBuffer();
            }
            NettyHeader.writeHeader(buffer, requestId, status, version);
            ChannelFuture future = targetChannel.write(buffer);
            ReleaseChannelFutureListener listener = new ReleaseChannelFutureListener(bytes);
            future.addListener(listener);
            addedReleaseListener = true;
        } finally {
            if (!addedReleaseListener) {
                Releasables.close(bStream.bytes());
            }
        }
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
            throw new ElasticsearchIllegalStateException("can't add nodes to a stopped transport");
        }
        if (node == null) {
            throw new ConnectTransportException(null, "can't connect to a null node");
        }
        globalLock.readLock().lock();
        try {
            if (!lifecycle.started()) {
                throw new ElasticsearchIllegalStateException("can't add nodes to a stopped transport");
            }
            NodeChannels nodeChannels = connectedNodes.get(node);
            if (nodeChannels != null) {
                return;
            }
            connectionLock.acquire(node.id());
            try {
                if (!lifecycle.started()) {
                    throw new ElasticsearchIllegalStateException("can't add nodes to a stopped transport");
                }
                try {


                    if (light) {
                        nodeChannels = connectToChannelsLight(node);
                    } else {
                        nodeChannels = new NodeChannels(new Channel[connectionsPerNodeRecovery], new Channel[connectionsPerNodeBulk], new Channel[connectionsPerNodeReg], new Channel[connectionsPerNodeState], new Channel[connectionsPerNodePing]);
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
            } finally {
                connectionLock.release(node.id());
            }
        } finally {
            globalLock.readLock().unlock();
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
        return new NodeChannels(channels, channels, channels, channels, channels);
    }

    private void connectToChannels(NodeChannels nodeChannels, DiscoveryNode node) {
        ChannelFuture[] connectRecovery = new ChannelFuture[nodeChannels.recovery.length];
        ChannelFuture[] connectBulk = new ChannelFuture[nodeChannels.bulk.length];
        ChannelFuture[] connectReg = new ChannelFuture[nodeChannels.reg.length];
        ChannelFuture[] connectState = new ChannelFuture[nodeChannels.state.length];
        ChannelFuture[] connectPing = new ChannelFuture[nodeChannels.ping.length];
        InetSocketAddress address = ((InetSocketTransportAddress) node.address()).address();
        for (int i = 0; i < connectRecovery.length; i++) {
            connectRecovery[i] = clientBootstrap.connect(address);
        }
        for (int i = 0; i < connectBulk.length; i++) {
            connectBulk[i] = clientBootstrap.connect(address);
        }
        for (int i = 0; i < connectReg.length; i++) {
            connectReg[i] = clientBootstrap.connect(address);
        }
        for (int i = 0; i < connectState.length; i++) {
            connectState[i] = clientBootstrap.connect(address);
        }
        for (int i = 0; i < connectPing.length; i++) {
            connectPing[i] = clientBootstrap.connect(address);
        }

        try {
            for (int i = 0; i < connectRecovery.length; i++) {
                connectRecovery[i].awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
                if (!connectRecovery[i].isSuccess()) {
                    throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", connectRecovery[i].getCause());
                }
                nodeChannels.recovery[i] = connectRecovery[i].getChannel();
                nodeChannels.recovery[i].getCloseFuture().addListener(new ChannelCloseListener(node));
            }

            for (int i = 0; i < connectBulk.length; i++) {
                connectBulk[i].awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
                if (!connectBulk[i].isSuccess()) {
                    throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", connectBulk[i].getCause());
                }
                nodeChannels.bulk[i] = connectBulk[i].getChannel();
                nodeChannels.bulk[i].getCloseFuture().addListener(new ChannelCloseListener(node));
            }

            for (int i = 0; i < connectReg.length; i++) {
                connectReg[i].awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
                if (!connectReg[i].isSuccess()) {
                    throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", connectReg[i].getCause());
                }
                nodeChannels.reg[i] = connectReg[i].getChannel();
                nodeChannels.reg[i].getCloseFuture().addListener(new ChannelCloseListener(node));
            }

            for (int i = 0; i < connectState.length; i++) {
                connectState[i].awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
                if (!connectState[i].isSuccess()) {
                    throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", connectState[i].getCause());
                }
                nodeChannels.state[i] = connectState[i].getChannel();
                nodeChannels.state[i].getCloseFuture().addListener(new ChannelCloseListener(node));
            }

            for (int i = 0; i < connectPing.length; i++) {
                connectPing[i].awaitUninterruptibly((long) (connectTimeout.millis() * 1.5));
                if (!connectPing[i].isSuccess()) {
                    throw new ConnectTransportException(node, "connect_timeout[" + connectTimeout + "]", connectPing[i].getCause());
                }
                nodeChannels.ping[i] = connectPing[i].getChannel();
                nodeChannels.ping[i].getCloseFuture().addListener(new ChannelCloseListener(node));
            }

            if (nodeChannels.recovery.length == 0) {
                if (nodeChannels.bulk.length > 0) {
                    nodeChannels.recovery = nodeChannels.bulk;
                } else {
                    nodeChannels.recovery = nodeChannels.reg;
                }
            }
            if (nodeChannels.bulk.length == 0) {
                nodeChannels.bulk = nodeChannels.reg;
            }
        } catch (RuntimeException e) {
            // clean the futures
            for (ChannelFuture future : ImmutableList.<ChannelFuture>builder().add(connectRecovery).add(connectBulk).add(connectReg).add(connectState).add(connectPing).build()) {
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
        NodeChannels nodeChannels = connectedNodes.remove(node);
        if (nodeChannels != null) {
            connectionLock.acquire(node.id());
            try {
                try {
                    nodeChannels.close();
                } finally {
                    logger.debug("disconnected from [{}]", node);
                    transportServiceAdapter.raiseNodeDisconnected(node);
                }
            } finally {
                connectionLock.release(node.id());
            }
        }
    }

    /**
     * Disconnects from a node, only if the relevant channel is found to be part of the node channels.
     */
    private void disconnectFromNode(DiscoveryNode node, Channel channel, String reason) {
        NodeChannels nodeChannels = connectedNodes.get(node);
        if (nodeChannels != null && nodeChannels.hasChannel(channel)) {
            connectionLock.acquire(node.id());
            if (!nodeChannels.hasChannel(channel)) { //might have been removed in the meanwhile, safety check
                assert !connectedNodes.containsKey(node);
            } else {
                try {
                    connectedNodes.remove(node);
                    try {
                        nodeChannels.close();
                    } finally {
                        logger.debug("disconnected from [{}], {}", node, reason);
                        transportServiceAdapter.raiseNodeDisconnected(node);
                    }
                } finally {
                    connectionLock.release(node.id());
                }
            }
        }
    }

    /**
     * Disconnects from a node if a channel is found as part of that nodes channels.
     */
    private void disconnectFromNodeChannel(Channel channel, Throwable failure) {
        for (DiscoveryNode node : connectedNodes.keySet()) {
            NodeChannels nodeChannels = connectedNodes.get(node);
            if (nodeChannels != null && nodeChannels.hasChannel(channel)) {
                connectionLock.acquire(node.id());
                if (!nodeChannels.hasChannel(channel)) { //might have been removed in the meanwhile, safety check
                    assert !connectedNodes.containsKey(node);
                } else {
                    try {
                        connectedNodes.remove(node);
                        try {
                            nodeChannels.close();
                        } finally {
                            logger.debug("disconnected from [{}] on channel failure", failure, node);
                            transportServiceAdapter.raiseNodeDisconnected(node);
                        }
                    } finally {
                        connectionLock.release(node.id());
                    }
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

    private class ChannelCloseListener implements ChannelFutureListener {

        private final DiscoveryNode node;

        private ChannelCloseListener(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            disconnectFromNode(node, future.getChannel(), "channel closed event");
        }
    }

    public static class NodeChannels {

        private Channel[] recovery;
        private final AtomicInteger recoveryCounter = new AtomicInteger();
        private Channel[] bulk;
        private final AtomicInteger bulkCounter = new AtomicInteger();
        private Channel[] reg;
        private final AtomicInteger regCounter = new AtomicInteger();
        private Channel[] state;
        private final AtomicInteger stateCounter = new AtomicInteger();
        private Channel[] ping;
        private final AtomicInteger pingCounter = new AtomicInteger();

        public NodeChannels(Channel[] recovery, Channel[] bulk, Channel[] reg, Channel[] state, Channel[] ping) {
            this.recovery = recovery;
            this.bulk = bulk;
            this.reg = reg;
            this.state = state;
            this.ping = ping;
        }

        public boolean hasChannel(Channel channel) {
            return hasChannel(channel, recovery) || hasChannel(channel, bulk) || hasChannel(channel, reg) || hasChannel(channel, state) || hasChannel(channel, ping);
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
            if (type == TransportRequestOptions.Type.REG) {
                return reg[MathUtils.mod(regCounter.incrementAndGet(), reg.length)];
            } else if (type == TransportRequestOptions.Type.STATE) {
                return state[MathUtils.mod(stateCounter.incrementAndGet(), state.length)];
            } else if (type == TransportRequestOptions.Type.PING) {
                return ping[MathUtils.mod(pingCounter.incrementAndGet(), ping.length)];
            } else if (type == TransportRequestOptions.Type.BULK) {
                return bulk[MathUtils.mod(bulkCounter.incrementAndGet(), bulk.length)];
            } else if (type == TransportRequestOptions.Type.RECOVERY) {
                return recovery[MathUtils.mod(recoveryCounter.incrementAndGet(), recovery.length)];
            } else {
                throw new ElasticsearchIllegalArgumentException("no type channel for [" + type + "]");
            }
        }

        public synchronized void close() {
            List<ChannelFuture> futures = new ArrayList<>();
            closeChannelsAndWait(recovery, futures);
            closeChannelsAndWait(bulk, futures);
            closeChannelsAndWait(reg, futures);
            closeChannelsAndWait(state, futures);
            closeChannelsAndWait(ping, futures);
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
}
