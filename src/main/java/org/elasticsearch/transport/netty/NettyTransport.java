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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.netty.NettyUtils;
import org.elasticsearch.common.netty.OpenChannelsHandler;
import org.elasticsearch.common.netty.ReleaseChannelFutureListener;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
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
import org.jboss.netty.buffer.ChannelBuffers;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.common.network.NetworkService.TcpSettings.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
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

    public static final String HTTP_SERVER_WORKER_THREAD_NAME_PREFIX = "http_server_worker";
    public static final String HTTP_SERVER_BOSS_THREAD_NAME_PREFIX = "http_server_boss";
    public static final String TRANSPORT_CLIENT_WORKER_THREAD_NAME_PREFIX = "transport_client_worker";
    public static final String TRANSPORT_CLIENT_BOSS_THREAD_NAME_PREFIX = "transport_client_boss";

    public static final String WORKER_COUNT = "transport.netty.worker_count";
    public static final String CONNECTIONS_PER_NODE_RECOVERY = "transport.connections_per_node.recovery";
    public static final String CONNECTIONS_PER_NODE_BULK = "transport.connections_per_node.bulk";
    public static final String CONNECTIONS_PER_NODE_REG = "transport.connections_per_node.reg";
    public static final String CONNECTIONS_PER_NODE_STATE = "transport.connections_per_node.state";
    public static final String CONNECTIONS_PER_NODE_PING = "transport.connections_per_node.ping";
    public static final String PING_SCHEDULE = "transport.ping_schedule"; // the scheduled internal ping interval setting
    public static final TimeValue DEFAULT_PING_SCHEDULE = TimeValue.timeValueMillis(-1); // the default ping schedule, defaults to disabled (-1)
    public static final String DEFAULT_PORT_RANGE = "9300-9400";
    public static final String DEFAULT_PROFILE = "default";

    protected final NetworkService networkService;
    protected final Version version;

    protected final boolean blockingClient;
    protected final TimeValue connectTimeout;
    protected final ByteSizeValue maxCumulationBufferCapacity;
    protected final int maxCompositeBufferComponents;
    protected final boolean compress;
    protected final ReceiveBufferSizePredictorFactory receiveBufferSizePredictorFactory;
    protected final int workerCount;
    protected final ByteSizeValue receivePredictorMin;
    protected final ByteSizeValue receivePredictorMax;

    protected final int connectionsPerNodeRecovery;
    protected final int connectionsPerNodeBulk;
    protected final int connectionsPerNodeReg;
    protected final int connectionsPerNodeState;
    protected final int connectionsPerNodePing;

    private final TimeValue pingSchedule;

    protected final BigArrays bigArrays;
    protected final ThreadPool threadPool;
    protected volatile OpenChannelsHandler serverOpenChannels;
    protected volatile ClientBootstrap clientBootstrap;
    // node id to actual channel
    protected final ConcurrentMap<DiscoveryNode, NodeChannels> connectedNodes = newConcurrentMap();
    protected final Map<String, ServerBootstrap> serverBootstraps = newConcurrentMap();
    protected final Map<String, Channel> serverChannels = newConcurrentMap();
    protected final Map<String, BoundTransportAddress> profileBoundAddresses = newConcurrentMap();
    protected volatile TransportServiceAdapter transportServiceAdapter;
    protected volatile BoundTransportAddress boundAddress;
    protected final KeyedLock<String> connectionLock = new KeyedLock<>();

    // this lock is here to make sure we close this transport and disconnect all the client nodes
    // connections while no connect operations is going on... (this might help with 100% CPU when stopping the transport?)
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();

    // package visibility for tests
    final ScheduledPing scheduledPing;

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
        this.blockingClient = settings.getAsBoolean("transport.netty.transport.tcp.blocking_client", settings.getAsBoolean(TCP_BLOCKING_CLIENT, settings.getAsBoolean(TCP_BLOCKING, false)));
        this.connectTimeout = this.settings.getAsTime("transport.netty.connect_timeout", settings.getAsTime("transport.tcp.connect_timeout", settings.getAsTime(TCP_CONNECT_TIMEOUT, TCP_DEFAULT_CONNECT_TIMEOUT)));
        this.maxCumulationBufferCapacity = this.settings.getAsBytesSize("transport.netty.max_cumulation_buffer_capacity", null);
        this.maxCompositeBufferComponents = this.settings.getAsInt("transport.netty.max_composite_buffer_components", -1);
        this.compress = settings.getAsBoolean(TransportSettings.TRANSPORT_TCP_COMPRESS, false);

        this.connectionsPerNodeRecovery = this.settings.getAsInt("transport.netty.connections_per_node.recovery", settings.getAsInt(CONNECTIONS_PER_NODE_RECOVERY, 2));
        this.connectionsPerNodeBulk = this.settings.getAsInt("transport.netty.connections_per_node.bulk", settings.getAsInt(CONNECTIONS_PER_NODE_BULK, 3));
        this.connectionsPerNodeReg = this.settings.getAsInt("transport.netty.connections_per_node.reg", settings.getAsInt(CONNECTIONS_PER_NODE_REG, 6));
        this.connectionsPerNodeState = this.settings.getAsInt("transport.netty.connections_per_node.high", settings.getAsInt(CONNECTIONS_PER_NODE_STATE, 1));
        this.connectionsPerNodePing = this.settings.getAsInt("transport.netty.connections_per_node.ping", settings.getAsInt(CONNECTIONS_PER_NODE_PING, 1));

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

        long defaultReceiverPredictor = 512 * 1024;
        if (JvmInfo.jvmInfo().getMem().getDirectMemoryMax().bytes() > 0) {
            // we can guess a better default...
            long l = (long) ((0.3 * JvmInfo.jvmInfo().getMem().getDirectMemoryMax().bytes()) / workerCount);
            defaultReceiverPredictor = Math.min(defaultReceiverPredictor, Math.max(l, 64 * 1024));
        }

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        this.receivePredictorMin = this.settings.getAsBytesSize("transport.netty.receive_predictor_min", this.settings.getAsBytesSize("transport.netty.receive_predictor_size", new ByteSizeValue(defaultReceiverPredictor)));
        this.receivePredictorMax = this.settings.getAsBytesSize("transport.netty.receive_predictor_max", this.settings.getAsBytesSize("transport.netty.receive_predictor_size", new ByteSizeValue(defaultReceiverPredictor)));
        if (receivePredictorMax.bytes() == receivePredictorMin.bytes()) {
            receiveBufferSizePredictorFactory = new FixedReceiveBufferSizePredictorFactory((int) receivePredictorMax.bytes());
        } else {
            receiveBufferSizePredictorFactory = new AdaptiveReceiveBufferSizePredictorFactory((int) receivePredictorMin.bytes(), (int) receivePredictorMin.bytes(), (int) receivePredictorMax.bytes());
        }

        this.scheduledPing = new ScheduledPing();
        this.pingSchedule = settings.getAsTime(PING_SCHEDULE, DEFAULT_PING_SCHEDULE);
        if (pingSchedule.millis() > 0) {
            threadPool.schedule(pingSchedule, ThreadPool.Names.GENERIC, scheduledPing);
        }
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
        boolean success = false;
        try {
            clientBootstrap = createClientBootstrap();
            if (settings.getAsBoolean("network.server", true)) {
                final OpenChannelsHandler openChannels = new OpenChannelsHandler(logger);
                this.serverOpenChannels = openChannels;

                // extract default profile first and create standard bootstrap
                Map<String, Settings> profiles = settings.getGroups("transport.profiles", true);
                if (!profiles.containsKey(DEFAULT_PROFILE)) {
                    profiles = Maps.newHashMap(profiles);
                    profiles.put(DEFAULT_PROFILE, ImmutableSettings.EMPTY);
                }

                Settings fallbackSettings = createFallbackSettings();
                Settings defaultSettings = profiles.get(DEFAULT_PROFILE);

                // loop through all profiles and strart them app, special handling for default one
                for (Map.Entry<String, Settings> entry : profiles.entrySet()) {
                    Settings profileSettings = entry.getValue();
                    String name = entry.getKey();

                    if (DEFAULT_PROFILE.equals(name)) {
                        profileSettings = settingsBuilder()
                                .put(profileSettings)
                                .put("port", profileSettings.get("port", this.settings.get("transport.tcp.port", DEFAULT_PORT_RANGE)))
                                .build();
                    } else {
                        // if profile does not have a port, skip it
                        if (profileSettings.get("port") == null) {
                            logger.info("No port configured for profile [{}], not binding", name);
                            continue;
                        }
                    }

                    // merge fallback settings with default settings with profile settings so we have complete settings with default values
                    Settings mergedSettings = settingsBuilder()
                            .put(fallbackSettings)
                            .put(defaultSettings)
                            .put(profileSettings)
                            .build();

                    createServerBootstrap(name, mergedSettings);
                    bindServerBootstrap(name, mergedSettings);
                }

                InetSocketAddress boundAddress = (InetSocketAddress) serverChannels.get(DEFAULT_PROFILE).getLocalAddress();
                int publishPort = settings.getAsInt("transport.netty.publish_port", settings.getAsInt("transport.publish_port", boundAddress.getPort()));
                String publishHost = settings.get("transport.netty.publish_host", settings.get("transport.publish_host", settings.get("transport.host")));
                InetSocketAddress publishAddress = createPublishAddress(publishHost, publishPort);
                this.boundAddress = new BoundTransportAddress(new InetSocketTransportAddress(boundAddress), new InetSocketTransportAddress(publishAddress));
            }
            success = true;
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return ImmutableMap.copyOf(profileBoundAddresses);
    }

    private InetSocketAddress createPublishAddress(String publishHost, int publishPort) {
        try {
            return new InetSocketAddress(networkService.resolvePublishHostAddress(publishHost), publishPort);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
    }

    private ClientBootstrap createClientBootstrap() {

        if (blockingClient) {
            clientBootstrap = new ClientBootstrap(new OioClientSocketChannelFactory(Executors.newCachedThreadPool(daemonThreadFactory(settings, TRANSPORT_CLIENT_WORKER_THREAD_NAME_PREFIX))));
        } else {
            int bossCount = settings.getAsInt("transport.netty.boss_count", 1);
            clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, TRANSPORT_CLIENT_BOSS_THREAD_NAME_PREFIX)),
                    bossCount,
                    new NioWorkerPool(Executors.newCachedThreadPool(daemonThreadFactory(settings, TRANSPORT_CLIENT_WORKER_THREAD_NAME_PREFIX)), workerCount),
                    new HashedWheelTimer(daemonThreadFactory(settings, "transport_client_timer"))));
        }
        clientBootstrap.setPipelineFactory(configureClientChannelPipelineFactory());
        clientBootstrap.setOption("connectTimeoutMillis", connectTimeout.millis());

        String tcpNoDelay = settings.get("transport.netty.tcp_no_delay", settings.get(TCP_NO_DELAY, "true"));
        if (!"default".equals(tcpNoDelay)) {
            clientBootstrap.setOption("tcpNoDelay", Booleans.parseBoolean(tcpNoDelay, null));
        }

        String tcpKeepAlive = settings.get("transport.netty.tcp_keep_alive", settings.get(TCP_KEEP_ALIVE, "true"));
        if (!"default".equals(tcpKeepAlive)) {
            clientBootstrap.setOption("keepAlive", Booleans.parseBoolean(tcpKeepAlive, null));
        }

        ByteSizeValue tcpSendBufferSize = settings.getAsBytesSize("transport.netty.tcp_send_buffer_size", settings.getAsBytesSize(TCP_SEND_BUFFER_SIZE, TCP_DEFAULT_SEND_BUFFER_SIZE));
        if (tcpSendBufferSize != null && tcpSendBufferSize.bytes() > 0) {
            clientBootstrap.setOption("sendBufferSize", tcpSendBufferSize.bytes());
        }

        ByteSizeValue tcpReceiveBufferSize = settings.getAsBytesSize("transport.netty.tcp_receive_buffer_size", settings.getAsBytesSize(TCP_RECEIVE_BUFFER_SIZE, TCP_DEFAULT_RECEIVE_BUFFER_SIZE));
        if (tcpReceiveBufferSize != null && tcpReceiveBufferSize.bytes() > 0) {
            clientBootstrap.setOption("receiveBufferSize", tcpReceiveBufferSize.bytes());
        }

        clientBootstrap.setOption("receiveBufferSizePredictorFactory", receiveBufferSizePredictorFactory);

        Boolean reuseAddress = settings.getAsBoolean("transport.netty.reuse_address", settings.getAsBoolean(TCP_REUSE_ADDRESS, NetworkUtils.defaultReuseAddress()));
        if (reuseAddress != null) {
            clientBootstrap.setOption("reuseAddress", reuseAddress);
        }

        return clientBootstrap;
    }

    private Settings createFallbackSettings() {
        ImmutableSettings.Builder fallbackSettingsBuilder = settingsBuilder();

        String fallbackBindHost = settings.get("transport.netty.bind_host", settings.get("transport.bind_host", settings.get("transport.host")));
        if (fallbackBindHost != null) {
            fallbackSettingsBuilder.put("bind_host", fallbackBindHost);
        }

        String fallbackPublishHost = settings.get("transport.netty.publish_host", settings.get("transport.publish_host", settings.get("transport.host")));
        if (fallbackPublishHost != null) {
            fallbackSettingsBuilder.put("publish_host", fallbackPublishHost);
        }

        String fallbackTcpNoDelay = settings.get("transport.netty.tcp_no_delay", settings.get(TCP_NO_DELAY, "true"));
        if (fallbackTcpNoDelay != null) {
            fallbackSettingsBuilder.put("tcp_no_delay", fallbackTcpNoDelay);
        }

        String fallbackTcpKeepAlive = settings.get("transport.netty.tcp_keep_alive", settings.get(TCP_KEEP_ALIVE, "true"));
        if (fallbackTcpKeepAlive != null) {
            fallbackSettingsBuilder.put("tcp_keep_alive", fallbackTcpKeepAlive);
        }

        Boolean fallbackReuseAddress = settings.getAsBoolean("transport.netty.reuse_address", settings.getAsBoolean(TCP_REUSE_ADDRESS, NetworkUtils.defaultReuseAddress()));
        if (fallbackReuseAddress != null) {
            fallbackSettingsBuilder.put("reuse_address", fallbackReuseAddress);
        }

        ByteSizeValue fallbackTcpSendBufferSize = settings.getAsBytesSize("transport.netty.tcp_send_buffer_size", settings.getAsBytesSize(TCP_SEND_BUFFER_SIZE, TCP_DEFAULT_SEND_BUFFER_SIZE));
        if (fallbackTcpSendBufferSize != null) {
            fallbackSettingsBuilder.put("tcp_send_buffer_size", fallbackTcpSendBufferSize);
        }

        ByteSizeValue fallbackTcpBufferSize = settings.getAsBytesSize("transport.netty.tcp_receive_buffer_size", settings.getAsBytesSize(TCP_RECEIVE_BUFFER_SIZE, TCP_DEFAULT_RECEIVE_BUFFER_SIZE));
        if (fallbackTcpBufferSize != null) {
            fallbackSettingsBuilder.put("tcp_receive_buffer_size", fallbackTcpBufferSize);
        }

        return fallbackSettingsBuilder.build();
    }

    private void bindServerBootstrap(final String name, final Settings settings) {
        // Bind and start to accept incoming connections.
        InetAddress hostAddressX;
        String bindHost = settings.get("bind_host");
        try {
            hostAddressX = networkService.resolveBindHostAddress(bindHost);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host [" + bindHost + "]", e);
        }
        final InetAddress hostAddress = hostAddressX;

        String port = settings.get("port");
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override
            public boolean onPortNumber(int portNumber) {
                try {
                    serverChannels.put(name, serverBootstraps.get(name).bind(new InetSocketAddress(hostAddress, portNumber)));
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

        if (!DEFAULT_PROFILE.equals(name)) {
            InetSocketAddress boundAddress = (InetSocketAddress) serverChannels.get(name).getLocalAddress();
            int publishPort = settings.getAsInt("publish_port", boundAddress.getPort());
            String publishHost = settings.get("publish_host", boundAddress.getHostString());
            InetSocketAddress publishAddress = createPublishAddress(publishHost, publishPort);
            profileBoundAddresses.put(name, new BoundTransportAddress(new InetSocketTransportAddress(boundAddress), new InetSocketTransportAddress(publishAddress)));
        }

        logger.debug("Bound profile [{}] to address [{}]", name, serverChannels.get(name).getLocalAddress());
    }

    private void createServerBootstrap(String name, Settings settings) {
        boolean blockingServer = settings.getAsBoolean("transport.tcp.blocking_server", this.settings.getAsBoolean(TCP_BLOCKING_SERVER, this.settings.getAsBoolean(TCP_BLOCKING, false)));
        String port = settings.get("port");
        String bindHost = settings.get("bind_host");
        String publishHost = settings.get("publish_host");
        String tcpNoDelay = settings.get("tcp_no_delay");
        String tcpKeepAlive = settings.get("tcp_keep_alive");
        Boolean reuseAddress = settings.getAsBoolean("reuse_address", NetworkUtils.defaultReuseAddress());
        ByteSizeValue tcpSendBufferSize = settings.getAsBytesSize("tcp_send_buffer_size", TCP_DEFAULT_SEND_BUFFER_SIZE);
        ByteSizeValue tcpReceiveBufferSize = settings.getAsBytesSize("tcp_receive_buffer_size", TCP_DEFAULT_RECEIVE_BUFFER_SIZE);

        logger.debug("using profile[{}], worker_count[{}], port[{}], bind_host[{}], publish_host[{}], compress[{}], connect_timeout[{}], connections_per_node[{}/{}/{}/{}/{}], receive_predictor[{}->{}]",
                name, workerCount, port, bindHost, publishHost, compress, connectTimeout, connectionsPerNodeRecovery, connectionsPerNodeBulk, connectionsPerNodeReg, connectionsPerNodeState, connectionsPerNodePing, receivePredictorMin, receivePredictorMax);

        final ThreadFactory bossFactory = daemonThreadFactory(this.settings, HTTP_SERVER_BOSS_THREAD_NAME_PREFIX, name);
        final ThreadFactory workerFactory = daemonThreadFactory(this.settings, HTTP_SERVER_WORKER_THREAD_NAME_PREFIX, name);
        ServerBootstrap serverBootstrap;
        if (blockingServer) {
            serverBootstrap = new ServerBootstrap(new OioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(bossFactory),
                    Executors.newCachedThreadPool(workerFactory)
            ));
        } else {
            serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(bossFactory),
                    Executors.newCachedThreadPool(workerFactory),
                    workerCount));
        }
        serverBootstrap.setPipelineFactory(configureServerChannelPipelineFactory(name, settings));
        if (!"default".equals(tcpNoDelay)) {
            serverBootstrap.setOption("child.tcpNoDelay", Booleans.parseBoolean(tcpNoDelay, null));
        }
        if (!"default".equals(tcpKeepAlive)) {
            serverBootstrap.setOption("child.keepAlive", Booleans.parseBoolean(tcpKeepAlive, null));
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

        serverBootstraps.put(name, serverBootstrap);
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

                    Iterator<Map.Entry<String, Channel>> serverChannelIterator = serverChannels.entrySet().iterator();
                    while (serverChannelIterator.hasNext()) {
                        Map.Entry<String, Channel> serverChannelEntry = serverChannelIterator.next();
                        String name = serverChannelEntry.getKey();
                        Channel serverChannel = serverChannelEntry.getValue();
                        try {
                            serverChannel.close().awaitUninterruptibly();
                        } catch (Throwable t) {
                            logger.debug("Error closing serverChannel for profile [{}]", t, name);
                        }
                        serverChannelIterator.remove();
                    }

                    if (serverOpenChannels != null) {
                        serverOpenChannels.close();
                        serverOpenChannels = null;
                    }

                    Iterator<Map.Entry<String, ServerBootstrap>> serverBootstrapIterator = serverBootstraps.entrySet().iterator();
                    while (serverBootstrapIterator.hasNext()) {
                        Map.Entry<String, ServerBootstrap> serverBootstrapEntry = serverBootstrapIterator.next();
                        String name = serverBootstrapEntry.getKey();
                        ServerBootstrap serverBootstrap = serverBootstrapEntry.getValue();

                        try {
                            serverBootstrap.releaseExternalResources();
                        } catch (Throwable t) {
                            logger.debug("Error closing serverBootstrap for profile [{}]", t, name);
                        }

                        serverBootstrapIterator.remove();
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
                String defaultPort = settings.get("transport.profiles.default.port", settings.get("transport.netty.port", this.settings.get("transport.tcp.port", DEFAULT_PORT_RANGE)));
                int[] iPorts = new PortsRange(defaultPort).ports();
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

    protected void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if (!lifecycle.started()) {
            // ignore
            return;
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
        } else if (e.getCause() instanceof SizeHeaderFrameDecoder.HttpOnTransportException) {
            // in case we are able to return data, serialize the exception content and sent it back to the client
            if (ctx.getChannel().isOpen()) {
                ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(e.getCause().getMessage().getBytes(Charsets.UTF_8));
                ChannelFuture channelFuture = ctx.getChannel().write(buffer);
                channelFuture.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        future.getChannel().close();
                    }
                });
            }
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

            // we pick the smallest of the 2, to support both backward and forward compatibility
            // note, this is the only place we need to do this, since from here on, we use the serialized version
            // as the version to use also when the node receiving this request will send the response with
            Version version = Version.smallest(this.version, node.version());

            stream.setVersion(version);
            stream.writeString(action);

            ReleasablePagedBytesReference bytes;
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
                buffer = ChannelBuffers.wrappedBuffer(NettyUtils.DEFAULT_GATHERING, headerBuffer, contentBuffer);
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
            transportServiceAdapter.onRequestSent(node, requestId, action, request, options);
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
            connectionLock.acquire(node.id());
            try {
                if (!lifecycle.started()) {
                    throw new ElasticsearchIllegalStateException("can't add nodes to a stopped transport");
                }
                NodeChannels nodeChannels = connectedNodes.get(node);
                if (nodeChannels != null) {
                    return;
                }
                try {
                    if (light) {
                        nodeChannels = connectToChannelsLight(node);
                    } else {
                        nodeChannels = new NodeChannels(new Channel[connectionsPerNodeRecovery], new Channel[connectionsPerNodeBulk], new Channel[connectionsPerNodeReg], new Channel[connectionsPerNodeState], new Channel[connectionsPerNodePing]);
                        try {
                            connectToChannels(nodeChannels, node);
                        } catch (Throwable e) {
                            logger.trace("failed to connect to [{}], cleaning dangling connections", e, node);
                            nodeChannels.close();
                            throw e;
                        }
                    }
                    // we acquire a connection lock, so no way there is an existing connection
                    nodeChannels.start();
                    connectedNodes.put(node, nodeChannels);
                    if (logger.isDebugEnabled()) {
                        logger.debug("connected to node [{}]", node);
                    }
                    transportServiceAdapter.raiseNodeConnected(node);
                } catch (ConnectTransportException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ConnectTransportException(node, "general node connection failure", e);
                }
            } finally {
                connectionLock.release(node.id());
            }
        } finally {
            globalLock.readLock().unlock();
        }
    }

    protected NodeChannels connectToChannelsLight(DiscoveryNode node) {
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

    protected void connectToChannels(NodeChannels nodeChannels, DiscoveryNode node) {
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
        connectionLock.acquire(node.id());
        try {
            NodeChannels nodeChannels = connectedNodes.remove(node);
            if (nodeChannels != null) {
                try {
                    logger.debug("disconnecting from [{}] due to explicit disconnect call", node);
                    nodeChannels.close();
                } finally {
                    logger.trace("disconnected from [{}] due to explicit disconnect call", node);
                    transportServiceAdapter.raiseNodeDisconnected(node);
                }
            }
        } finally {
            connectionLock.release(node.id());
        }
    }

    /**
     * Disconnects from a node, only if the relevant channel is found to be part of the node channels.
     */
    protected boolean disconnectFromNode(DiscoveryNode node, Channel channel, String reason) {
        // this might be called multiple times from all the node channels, so do a lightweight
        // check outside of the lock
        NodeChannels nodeChannels = connectedNodes.get(node);
        if (nodeChannels != null && nodeChannels.hasChannel(channel)) {
            connectionLock.acquire(node.id());
            try {
                nodeChannels = connectedNodes.get(node);
                // check again within the connection lock, if its still applicable to remove it
                if (nodeChannels != null && nodeChannels.hasChannel(channel)) {
                    connectedNodes.remove(node);
                    try {
                        logger.debug("disconnecting from [{}], {}", node, reason);
                        nodeChannels.close();
                    } finally {
                        logger.trace("disconnected from [{}], {}", node, reason);
                        transportServiceAdapter.raiseNodeDisconnected(node);
                    }
                    return true;
                }
            } finally {
                connectionLock.release(node.id());
            }
        }
        return false;
    }

    /**
     * Disconnects from a node if a channel is found as part of that nodes channels.
     */
    protected void disconnectFromNodeChannel(final Channel channel, final Throwable failure) {
        threadPool().generic().execute(new Runnable() {

            @Override
            public void run() {
                for (DiscoveryNode node : connectedNodes.keySet()) {
                    if (disconnectFromNode(node, channel, ExceptionsHelper.detailedMessage(failure))) {
                        // if we managed to find this channel and disconnect from it, then break, no need to check on
                        // the rest of the nodes
                        break;
                    }
                }
            }
        });
    }

    protected Channel nodeChannel(DiscoveryNode node, TransportRequestOptions options) throws ConnectTransportException {
        NodeChannels nodeChannels = connectedNodes.get(node);
        if (nodeChannels == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        return nodeChannels.channel(options.type());
    }

    public ChannelPipelineFactory configureClientChannelPipelineFactory() {
        return new ClientChannelPipelineFactory(this);
    }

    protected static class ClientChannelPipelineFactory implements ChannelPipelineFactory {
        protected NettyTransport nettyTransport;

        public ClientChannelPipelineFactory(NettyTransport nettyTransport) {
            this.nettyTransport = nettyTransport;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline channelPipeline = Channels.pipeline();
            SizeHeaderFrameDecoder sizeHeader = new SizeHeaderFrameDecoder();
            if (nettyTransport.maxCumulationBufferCapacity != null) {
                if (nettyTransport.maxCumulationBufferCapacity.bytes() > Integer.MAX_VALUE) {
                    sizeHeader.setMaxCumulationBufferCapacity(Integer.MAX_VALUE);
                } else {
                    sizeHeader.setMaxCumulationBufferCapacity((int) nettyTransport.maxCumulationBufferCapacity.bytes());
                }
            }
            if (nettyTransport.maxCompositeBufferComponents != -1) {
                sizeHeader.setMaxCumulationBufferComponents(nettyTransport.maxCompositeBufferComponents);
            }
            channelPipeline.addLast("size", sizeHeader);
            // using a dot as a prefix means, this cannot come from any settings parsed
            channelPipeline.addLast("dispatcher", new MessageChannelHandler(nettyTransport, nettyTransport.logger, ".client"));
            return channelPipeline;
        }
    }

    public ChannelPipelineFactory configureServerChannelPipelineFactory(String name, Settings settings) {
        return new ServerChannelPipelineFactory(this, name, settings);
    }

    protected static class ServerChannelPipelineFactory implements ChannelPipelineFactory {

        protected final NettyTransport nettyTransport;
        protected final String name;
        protected final Settings settings;

        public ServerChannelPipelineFactory(NettyTransport nettyTransport, String name, Settings settings) {
            this.nettyTransport = nettyTransport;
            this.name = name;
            this.settings = settings;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline channelPipeline = Channels.pipeline();
            channelPipeline.addLast("openChannels", nettyTransport.serverOpenChannels);
            SizeHeaderFrameDecoder sizeHeader = new SizeHeaderFrameDecoder();
            if (nettyTransport.maxCumulationBufferCapacity != null) {
                if (nettyTransport.maxCumulationBufferCapacity.bytes() > Integer.MAX_VALUE) {
                    sizeHeader.setMaxCumulationBufferCapacity(Integer.MAX_VALUE);
                } else {
                    sizeHeader.setMaxCumulationBufferCapacity((int) nettyTransport.maxCumulationBufferCapacity.bytes());
                }
            }
            if (nettyTransport.maxCompositeBufferComponents != -1) {
                sizeHeader.setMaxCumulationBufferComponents(nettyTransport.maxCompositeBufferComponents);
            }
            channelPipeline.addLast("size", sizeHeader);
            channelPipeline.addLast("dispatcher", new MessageChannelHandler(nettyTransport, nettyTransport.logger, name));
            return channelPipeline;
        }
    }

    protected class ChannelCloseListener implements ChannelFutureListener {

        private final DiscoveryNode node;

        private ChannelCloseListener(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public void operationComplete(final ChannelFuture future) throws Exception {
            NodeChannels nodeChannels = connectedNodes.get(node);
            if (nodeChannels != null && nodeChannels.hasChannel(future.getChannel())) {
                threadPool().generic().execute(new Runnable() {
                    @Override
                    public void run() {
                        disconnectFromNode(node, future.getChannel(), "channel closed event");
                    }
                });
            }
        }
    }

    public static class NodeChannels {

        ImmutableList<Channel> allChannels = ImmutableList.of();
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

        public void start() {
            this.allChannels = ImmutableList.<Channel>builder().add(recovery).add(bulk).add(reg).add(state).add(ping).build();
        }

        public boolean hasChannel(Channel channel) {
            for (Channel channel1 : allChannels) {
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
            for (Channel channel : allChannels) {
                try {
                    if (channel != null && channel.isOpen()) {
                        futures.add(channel.close());
                    }
                } catch (Exception e) {
                    //ignore
                }
            }
            for (ChannelFuture future : futures) {
                future.awaitUninterruptibly();
            }
        }
    }

    class ScheduledPing implements Runnable {

        final CounterMetric successfulPings = new CounterMetric();
        final CounterMetric failedPings = new CounterMetric();

        @Override
        public void run() {
            if (lifecycle.stoppedOrClosed()) {
                return;
            }
            for (Map.Entry<DiscoveryNode, NodeChannels> entry : connectedNodes.entrySet()) {
                DiscoveryNode node = entry.getKey();
                NodeChannels channels = entry.getValue();
                for (Channel channel : channels.allChannels) {
                    try {
                        ChannelFuture future = channel.write(NettyHeader.pingHeader());
                        future.addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                successfulPings.inc();
                            }
                        });
                    } catch (Throwable t) {
                        if (channel.isOpen()) {
                            logger.debug("[{}] failed to send ping transport message", t, node);
                            failedPings.inc();
                        } else {
                            logger.trace("[{}] failed to send ping transport message (channel closed)", t, node);
                        }
                    }
                }
            }
            threadPool.schedule(pingSchedule, ThreadPool.Names.GENERIC, this);
        }
    }
}
