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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.netty.NettyUtils;
import org.elasticsearch.common.netty.OpenChannelsHandler;
import org.elasticsearch.common.netty.ReleaseChannelFutureListener;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkService.TcpSettings;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractLifecycleRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportServiceAdapter;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.support.TransportStatus;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.AdaptiveReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.ReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.channel.socket.oio.OioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.CancelledKeyException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;
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

    public static final Setting<Integer> WORKER_COUNT =
        new Setting<>("transport.netty.worker_count",
            (s) -> Integer.toString(EsExecutors.boundedNumberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.netty.worker_count"), Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_RECOVERY =
        intSetting("transport.connections_per_node.recovery", 2, 1, Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_BULK =
        intSetting("transport.connections_per_node.bulk", 3, 1, Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_REG =
        intSetting("transport.connections_per_node.reg", 6, 1, Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_STATE =
        intSetting("transport.connections_per_node.state", 1, 1, Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_PING =
        intSetting("transport.connections_per_node.ping", 1, 1, Property.NodeScope);
    // the scheduled internal ping interval setting, defaults to disabled (-1)
    public static final Setting<TimeValue> PING_SCHEDULE =
        timeSetting("transport.ping_schedule", TimeValue.timeValueSeconds(-1), Property.NodeScope);
    public static final Setting<Boolean> TCP_BLOCKING_CLIENT =
        boolSetting("transport.tcp.blocking_client", TcpSettings.TCP_BLOCKING_CLIENT, Property.NodeScope);
    public static final Setting<TimeValue> TCP_CONNECT_TIMEOUT =
        timeSetting("transport.tcp.connect_timeout", TcpSettings.TCP_CONNECT_TIMEOUT, Property.NodeScope);
    public static final Setting<Boolean> TCP_NO_DELAY =
        boolSetting("transport.tcp_no_delay", TcpSettings.TCP_NO_DELAY, Property.NodeScope);
    public static final Setting<Boolean> TCP_KEEP_ALIVE =
        boolSetting("transport.tcp.keep_alive", TcpSettings.TCP_KEEP_ALIVE, Property.NodeScope);
    public static final Setting<Boolean> TCP_BLOCKING_SERVER =
        boolSetting("transport.tcp.blocking_server", TcpSettings.TCP_BLOCKING_SERVER, Property.NodeScope);
    public static final Setting<Boolean> TCP_REUSE_ADDRESS =
        boolSetting("transport.tcp.reuse_address", TcpSettings.TCP_REUSE_ADDRESS, Property.NodeScope);

    public static final Setting<ByteSizeValue> TCP_SEND_BUFFER_SIZE =
        Setting.byteSizeSetting("transport.tcp.send_buffer_size", TcpSettings.TCP_SEND_BUFFER_SIZE, Property.NodeScope);
    public static final Setting<ByteSizeValue> TCP_RECEIVE_BUFFER_SIZE =
        Setting.byteSizeSetting("transport.tcp.receive_buffer_size", TcpSettings.TCP_RECEIVE_BUFFER_SIZE, Property.NodeScope);

    public static final Setting<ByteSizeValue> NETTY_MAX_CUMULATION_BUFFER_CAPACITY =
        Setting.byteSizeSetting("transport.netty.max_cumulation_buffer_capacity", new ByteSizeValue(-1), Property.NodeScope);
    public static final Setting<Integer> NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS =
        Setting.intSetting("transport.netty.max_composite_buffer_components", -1, -1, Property.NodeScope);

    // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_SIZE = Setting.byteSizeSetting(
            "transport.netty.receive_predictor_size",
            settings -> {
                long defaultReceiverPredictor = 512 * 1024;
                if (JvmInfo.jvmInfo().getMem().getDirectMemoryMax().bytes() > 0) {
                    // we can guess a better default...
                    long l = (long) ((0.3 * JvmInfo.jvmInfo().getMem().getDirectMemoryMax().bytes()) / WORKER_COUNT.get(settings));
                    defaultReceiverPredictor = Math.min(defaultReceiverPredictor, Math.max(l, 64 * 1024));
                }
                return new ByteSizeValue(defaultReceiverPredictor).toString();
            }, Property.NodeScope);
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MIN =
        byteSizeSetting("transport.netty.receive_predictor_min", NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope);
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MAX =
        byteSizeSetting("transport.netty.receive_predictor_max", NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope);
    public static final Setting<Integer> NETTY_BOSS_COUNT =
        intSetting("transport.netty.boss_count", 1, 1, Property.NodeScope);

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
    // package private for testing
    volatile OpenChannelsHandler serverOpenChannels;
    protected volatile ClientBootstrap clientBootstrap;
    // node id to actual channel
    protected final ConcurrentMap<DiscoveryNode, NodeChannels> connectedNodes = newConcurrentMap();
    protected final Map<String, ServerBootstrap> serverBootstraps = newConcurrentMap();
    protected final Map<String, List<Channel>> serverChannels = newConcurrentMap();
    protected final ConcurrentMap<String, BoundTransportAddress> profileBoundAddresses = newConcurrentMap();
    protected volatile TransportServiceAdapter transportServiceAdapter;
    protected volatile BoundTransportAddress boundAddress;
    protected final KeyedLock<String> connectionLock = new KeyedLock<>();
    protected final NamedWriteableRegistry namedWriteableRegistry;
    private final CircuitBreakerService circuitBreakerService;

    // this lock is here to make sure we close this transport and disconnect all the client nodes
    // connections while no connect operations is going on... (this might help with 100% CPU when stopping the transport?)
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();

    // package visibility for tests
    final ScheduledPing scheduledPing;

    @Inject
    public NettyTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays, Version version,
                          NamedWriteableRegistry namedWriteableRegistry, CircuitBreakerService circuitBreakerService) {
        super(settings);
        this.threadPool = threadPool;
        this.networkService = networkService;
        this.bigArrays = bigArrays;
        this.version = version;

        this.workerCount = WORKER_COUNT.get(settings);
        this.blockingClient = TCP_BLOCKING_CLIENT.get(settings);
        this.connectTimeout = TCP_CONNECT_TIMEOUT.get(settings);
        this.maxCumulationBufferCapacity = NETTY_MAX_CUMULATION_BUFFER_CAPACITY.get(settings);
        this.maxCompositeBufferComponents = NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS.get(settings);
        this.compress = Transport.TRANSPORT_TCP_COMPRESS.get(settings);

        this.connectionsPerNodeRecovery = CONNECTIONS_PER_NODE_RECOVERY.get(settings);
        this.connectionsPerNodeBulk = CONNECTIONS_PER_NODE_BULK.get(settings);
        this.connectionsPerNodeReg = CONNECTIONS_PER_NODE_REG.get(settings);
        this.connectionsPerNodeState = CONNECTIONS_PER_NODE_STATE.get(settings);
        this.connectionsPerNodePing = CONNECTIONS_PER_NODE_PING.get(settings);

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        this.receivePredictorMin = NETTY_RECEIVE_PREDICTOR_MIN.get(settings);
        this.receivePredictorMax = NETTY_RECEIVE_PREDICTOR_MAX.get(settings);
        if (receivePredictorMax.bytes() == receivePredictorMin.bytes()) {
            receiveBufferSizePredictorFactory = new FixedReceiveBufferSizePredictorFactory((int) receivePredictorMax.bytes());
        } else {
            receiveBufferSizePredictorFactory = new AdaptiveReceiveBufferSizePredictorFactory((int) receivePredictorMin.bytes(),
                    (int) receivePredictorMin.bytes(), (int) receivePredictorMax.bytes());
        }

        this.scheduledPing = new ScheduledPing();
        this.pingSchedule = PING_SCHEDULE.get(settings);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.circuitBreakerService = circuitBreakerService;
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

    CircuitBreaker inFlightRequestsBreaker() {
        // We always obtain a fresh breaker to reflect changes to the breaker configuration.
        return circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            clientBootstrap = createClientBootstrap();
            if (NetworkService.NETWORK_SERVER.get(settings)) {
                final OpenChannelsHandler openChannels = new OpenChannelsHandler(logger);
                this.serverOpenChannels = openChannels;

                // extract default profile first and create standard bootstrap
                Map<String, Settings> profiles = TransportSettings.TRANSPORT_PROFILES_SETTING.get(settings()).getAsGroups(true);
                if (!profiles.containsKey(TransportSettings.DEFAULT_PROFILE)) {
                    profiles = new HashMap<>(profiles);
                    profiles.put(TransportSettings.DEFAULT_PROFILE, Settings.EMPTY);
                }

                Settings fallbackSettings = createFallbackSettings();
                Settings defaultSettings = profiles.get(TransportSettings.DEFAULT_PROFILE);

                // loop through all profiles and start them up, special handling for default one
                for (Map.Entry<String, Settings> entry : profiles.entrySet()) {
                    Settings profileSettings = entry.getValue();
                    String name = entry.getKey();

                    if (!Strings.hasLength(name)) {
                        logger.info("transport profile configured without a name. skipping profile with settings [{}]",
                                profileSettings.toDelimitedString(','));
                        continue;
                    } else if (TransportSettings.DEFAULT_PROFILE.equals(name)) {
                        profileSettings = Settings.builder()
                                .put(profileSettings)
                                .put("port", profileSettings.get("port", TransportSettings.PORT.get(this.settings)))
                                .build();
                    } else if (profileSettings.get("port") == null) {
                        // if profile does not have a port, skip it
                        logger.info("No port configured for profile [{}], not binding", name);
                        continue;
                    }

                    // merge fallback settings with default settings with profile settings so we have complete settings with default values
                    Settings mergedSettings = Settings.builder()
                            .put(fallbackSettings)
                            .put(defaultSettings)
                            .put(profileSettings)
                            .build();

                    createServerBootstrap(name, mergedSettings);
                    bindServerBootstrap(name, mergedSettings);
                }
            }
            if (pingSchedule.millis() > 0) {
                threadPool.schedule(pingSchedule, ThreadPool.Names.GENERIC, scheduledPing);
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
        return unmodifiableMap(new HashMap<>(profileBoundAddresses));
    }

    private ClientBootstrap createClientBootstrap() {

        if (blockingClient) {
            clientBootstrap = new ClientBootstrap(new OioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(daemonThreadFactory(settings, TRANSPORT_CLIENT_WORKER_THREAD_NAME_PREFIX))));
        } else {
            int bossCount = NETTY_BOSS_COUNT.get(settings);
            clientBootstrap = new ClientBootstrap(
                    new NioClientSocketChannelFactory(
                            Executors.newCachedThreadPool(daemonThreadFactory(settings, TRANSPORT_CLIENT_BOSS_THREAD_NAME_PREFIX)),
                            bossCount,
                            new NioWorkerPool(Executors.newCachedThreadPool(
                                    daemonThreadFactory(settings, TRANSPORT_CLIENT_WORKER_THREAD_NAME_PREFIX)), workerCount),
                    new HashedWheelTimer(daemonThreadFactory(settings, "transport_client_timer"))));
        }
        clientBootstrap.setPipelineFactory(configureClientChannelPipelineFactory());
        clientBootstrap.setOption("connectTimeoutMillis", connectTimeout.millis());

        boolean tcpNoDelay = TCP_NO_DELAY.get(settings);
        clientBootstrap.setOption("tcpNoDelay", tcpNoDelay);

        boolean tcpKeepAlive = TCP_KEEP_ALIVE.get(settings);
        clientBootstrap.setOption("keepAlive", tcpKeepAlive);

        ByteSizeValue tcpSendBufferSize = TCP_SEND_BUFFER_SIZE.get(settings);
        if (tcpSendBufferSize.bytes() > 0) {
            clientBootstrap.setOption("sendBufferSize", tcpSendBufferSize.bytes());
        }

        ByteSizeValue tcpReceiveBufferSize = TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.bytes() > 0) {
            clientBootstrap.setOption("receiveBufferSize", tcpReceiveBufferSize.bytes());
        }

        clientBootstrap.setOption("receiveBufferSizePredictorFactory", receiveBufferSizePredictorFactory);

        boolean reuseAddress = TCP_REUSE_ADDRESS.get(settings);
        clientBootstrap.setOption("reuseAddress", reuseAddress);

        return clientBootstrap;
    }

    private Settings createFallbackSettings() {
        Settings.Builder fallbackSettingsBuilder = Settings.builder();

        List<String> fallbackBindHost = TransportSettings.BIND_HOST.get(settings);
        if (fallbackBindHost.isEmpty() == false) {
            fallbackSettingsBuilder.putArray("bind_host", fallbackBindHost);
        }

        List<String> fallbackPublishHost = TransportSettings.PUBLISH_HOST.get(settings);
        if (fallbackPublishHost.isEmpty() == false) {
            fallbackSettingsBuilder.putArray("publish_host", fallbackPublishHost);
        }

        boolean fallbackTcpNoDelay = settings.getAsBoolean("transport.netty.tcp_no_delay", TcpSettings.TCP_NO_DELAY.get(settings));
        fallbackSettingsBuilder.put("tcp_no_delay", fallbackTcpNoDelay);

        boolean fallbackTcpKeepAlive = settings.getAsBoolean("transport.netty.tcp_keep_alive", TcpSettings.TCP_KEEP_ALIVE.get(settings));
            fallbackSettingsBuilder.put("tcp_keep_alive", fallbackTcpKeepAlive);

        boolean fallbackReuseAddress = settings.getAsBoolean("transport.netty.reuse_address", TcpSettings.TCP_REUSE_ADDRESS.get(settings));
        fallbackSettingsBuilder.put("reuse_address", fallbackReuseAddress);

        ByteSizeValue fallbackTcpSendBufferSize = settings.getAsBytesSize("transport.netty.tcp_send_buffer_size",
                TCP_SEND_BUFFER_SIZE.get(settings));
        if (fallbackTcpSendBufferSize.bytes() >= 0) {
            fallbackSettingsBuilder.put("tcp_send_buffer_size", fallbackTcpSendBufferSize);
        }

        ByteSizeValue fallbackTcpBufferSize = settings.getAsBytesSize("transport.netty.tcp_receive_buffer_size",
                TCP_RECEIVE_BUFFER_SIZE.get(settings));
        if (fallbackTcpBufferSize.bytes() >= 0) {
            fallbackSettingsBuilder.put("tcp_receive_buffer_size", fallbackTcpBufferSize);
        }

        return fallbackSettingsBuilder.build();
    }

    private void bindServerBootstrap(final String name, final Settings settings) {
        // Bind and start to accept incoming connections.
        InetAddress hostAddresses[];
        String bindHosts[] = settings.getAsArray("bind_host", null);
        try {
            hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host " + Arrays.toString(bindHosts) + "", e);
        }
        if (logger.isDebugEnabled()) {
            String[] addresses = new String[hostAddresses.length];
            for (int i = 0; i < hostAddresses.length; i++) {
                addresses[i] = NetworkAddress.format(hostAddresses[i]);
            }
            logger.debug("binding server bootstrap to: {}", (Object)addresses);
        }

        assert hostAddresses.length > 0;

        List<InetSocketAddress> boundAddresses = new ArrayList<>();
        for (InetAddress hostAddress : hostAddresses) {
            boundAddresses.add(bindToPort(name, hostAddress, settings.get("port")));
        }

        final BoundTransportAddress boundTransportAddress = createBoundTransportAddress(name, settings, boundAddresses);

        if (TransportSettings.DEFAULT_PROFILE.equals(name)) {
            this.boundAddress = boundTransportAddress;
        } else {
            profileBoundAddresses.put(name, boundTransportAddress);
        }
    }

    private InetSocketAddress bindToPort(final String name, final InetAddress hostAddress, String port) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override
            public boolean onPortNumber(int portNumber) {
                try {
                    Channel channel = serverBootstraps.get(name).bind(new InetSocketAddress(hostAddress, portNumber));
                    synchronized (serverChannels) {
                        List<Channel> list = serverChannels.get(name);
                        if (list == null) {
                            list = new ArrayList<>();
                            serverChannels.put(name, list);
                        }
                        list.add(channel);
                        boundSocket.set((InetSocketAddress) channel.getLocalAddress());
                    }
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

        if (logger.isDebugEnabled()) {
            logger.debug("Bound profile [{}] to address {{}}", name, NetworkAddress.format(boundSocket.get()));
        }

        return boundSocket.get();
    }

    private BoundTransportAddress createBoundTransportAddress(String name, Settings profileSettings,
            List<InetSocketAddress> boundAddresses) {
        String[] boundAddressesHostStrings = new String[boundAddresses.size()];
        TransportAddress[] transportBoundAddresses = new TransportAddress[boundAddresses.size()];
        for (int i = 0; i < boundAddresses.size(); i++) {
            InetSocketAddress boundAddress = boundAddresses.get(i);
            boundAddressesHostStrings[i] = boundAddress.getHostString();
            transportBoundAddresses[i] = new InetSocketTransportAddress(boundAddress);
        }

        final String[] publishHosts;
        if (TransportSettings.DEFAULT_PROFILE.equals(name)) {
            publishHosts = TransportSettings.PUBLISH_HOST.get(settings).toArray(Strings.EMPTY_ARRAY);
        } else {
            publishHosts = profileSettings.getAsArray("publish_host", boundAddressesHostStrings);
        }

        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        final int publishPort = resolvePublishPort(name, settings, profileSettings, boundAddresses, publishInetAddress);
        final TransportAddress publishAddress = new InetSocketTransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        return new BoundTransportAddress(transportBoundAddresses, publishAddress);
    }

    // package private for tests
    static int resolvePublishPort(String profileName, Settings settings, Settings profileSettings, List<InetSocketAddress> boundAddresses,
                               InetAddress publishInetAddress) {
        int publishPort;
        if (TransportSettings.DEFAULT_PROFILE.equals(profileName)) {
            publishPort = TransportSettings.PUBLISH_PORT.get(settings);
        } else {
            publishPort = profileSettings.getAsInt("publish_port", -1);
        }

        // if port not explicitly provided, search for port of address in boundAddresses that matches publishInetAddress
        if (publishPort < 0) {
            for (InetSocketAddress boundAddress : boundAddresses) {
                InetAddress boundInetAddress = boundAddress.getAddress();
                if (boundInetAddress.isAnyLocalAddress() || boundInetAddress.equals(publishInetAddress)) {
                    publishPort = boundAddress.getPort();
                    break;
                }
            }
        }

        // if no matching boundAddress found, check if there is a unique port for all bound addresses
        if (publishPort < 0) {
            final IntSet ports = new IntHashSet();
            for (InetSocketAddress boundAddress : boundAddresses) {
                ports.add(boundAddress.getPort());
            }
            if (ports.size() == 1) {
                publishPort = ports.iterator().next().value;
            }
        }

        if (publishPort < 0) {
            String profileExplanation = TransportSettings.DEFAULT_PROFILE.equals(profileName) ? "" : " for profile " + profileName;
            throw new BindTransportException("Failed to auto-resolve publish port" + profileExplanation + ", multiple bound addresses " +
                boundAddresses + " with distinct ports and none of them matched the publish address (" + publishInetAddress + "). " +
                "Please specify a unique port by setting " + TransportSettings.PORT.getKey() + " or " +
                TransportSettings.PUBLISH_PORT.getKey());
        }
        return publishPort;
    }

    private void createServerBootstrap(String name, Settings settings) {
        boolean blockingServer = TCP_BLOCKING_SERVER.get(settings);
        String port = settings.get("port");
        String bindHost = settings.get("bind_host");
        String publishHost = settings.get("publish_host");
        String tcpNoDelay = settings.get("tcp_no_delay");
        String tcpKeepAlive = settings.get("tcp_keep_alive");
        boolean reuseAddress = settings.getAsBoolean("reuse_address", NetworkUtils.defaultReuseAddress());
        ByteSizeValue tcpSendBufferSize = TCP_SEND_BUFFER_SIZE.getDefault(settings);
        ByteSizeValue tcpReceiveBufferSize = TCP_RECEIVE_BUFFER_SIZE.getDefault(settings);

        if (logger.isDebugEnabled()) {
            logger.debug("using profile[{}], worker_count[{}], port[{}], bind_host[{}], publish_host[{}], compress[{}], "
                            + "connect_timeout[{}], connections_per_node[{}/{}/{}/{}/{}], receive_predictor[{}->{}]",
                    name, workerCount, port, bindHost, publishHost, compress, connectTimeout, connectionsPerNodeRecovery,
                    connectionsPerNodeBulk, connectionsPerNodeReg, connectionsPerNodeState, connectionsPerNodePing, receivePredictorMin,
                    receivePredictorMax);
        }

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
        serverBootstrap.setOption("reuseAddress", reuseAddress);
        serverBootstrap.setOption("child.reuseAddress", reuseAddress);
        serverBootstraps.put(name, serverBootstrap);
    }

    @Override
    protected void doStop() {
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

                    Iterator<Map.Entry<String, List<Channel>>> serverChannelIterator = serverChannels.entrySet().iterator();
                    while (serverChannelIterator.hasNext()) {
                        Map.Entry<String, List<Channel>> serverChannelEntry = serverChannelIterator.next();
                        String name = serverChannelEntry.getKey();
                        List<Channel> serverChannels = serverChannelEntry.getValue();
                        for (Channel serverChannel : serverChannels) {
                            try {
                                serverChannel.close().awaitUninterruptibly();
                            } catch (Throwable t) {
                                logger.debug("Error closing serverChannel for profile [{}]", t, name);
                            }
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
    protected void doClose() {
    }

    @Override
    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws Exception {
        return parse(address, settings.get("transport.profiles.default.port", TransportSettings.PORT.get(settings)), perAddressLimit);
    }

    // this code is a take on guava's HostAndPort, like a HostAndPortRange

    // pattern for validating ipv6 bracket addresses.
    // not perfect, but PortsRange should take care of any port range validation, not a regex
    private static final Pattern BRACKET_PATTERN = Pattern.compile("^\\[(.*:.*)\\](?::([\\d\\-]*))?$");

    /** parse a hostname+port range spec into its equivalent addresses */
    static TransportAddress[] parse(String hostPortString, String defaultPortRange, int perAddressLimit) throws UnknownHostException {
        Objects.requireNonNull(hostPortString);
        String host;
        String portString = null;

        if (hostPortString.startsWith("[")) {
          // Parse a bracketed host, typically an IPv6 literal.
          Matcher matcher = BRACKET_PATTERN.matcher(hostPortString);
          if (!matcher.matches()) {
              throw new IllegalArgumentException("Invalid bracketed host/port range: " + hostPortString);
          }
          host = matcher.group(1);
          portString = matcher.group(2);  // could be null
        } else {
          int colonPos = hostPortString.indexOf(':');
          if (colonPos >= 0 && hostPortString.indexOf(':', colonPos + 1) == -1) {
            // Exactly 1 colon.  Split into host:port.
            host = hostPortString.substring(0, colonPos);
            portString = hostPortString.substring(colonPos + 1);
          } else {
            // 0 or 2+ colons.  Bare hostname or IPv6 literal.
            host = hostPortString;
            // 2+ colons and not bracketed: exception
            if (colonPos >= 0) {
                throw new IllegalArgumentException("IPv6 addresses must be bracketed: " + hostPortString);
            }
          }
        }

        // if port isn't specified, fill with the default
        if (portString == null || portString.isEmpty()) {
            portString = defaultPortRange;
        }

        // generate address for each port in the range
        Set<InetAddress> addresses = new HashSet<>(Arrays.asList(InetAddress.getAllByName(host)));
        List<TransportAddress> transportAddresses = new ArrayList<>();
        int[] ports = new PortsRange(portString).ports();
        int limit = Math.min(ports.length, perAddressLimit);
        for (int i = 0; i < limit; i++) {
            for (InetAddress address : addresses) {
                transportAddresses.add(new InetSocketTransportAddress(address, ports[i]));
            }
        }
        return transportAddresses.toArray(new TransportAddress[transportAddresses.size()]);
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
            logger.trace("close connection exception caught on transport layer [{}], disconnecting from relevant node", e.getCause(),
                    ctx.getChannel());
            // close the channel, which will cause a node to be disconnected if relevant
            ctx.getChannel().close();
            disconnectFromNodeChannel(ctx.getChannel(), e.getCause());
        } else if (isConnectException(e.getCause())) {
            logger.trace("connect exception caught on transport layer [{}]", e.getCause(), ctx.getChannel());
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            ctx.getChannel().close();
            disconnectFromNodeChannel(ctx.getChannel(), e.getCause());
        } else if (e.getCause() instanceof BindException) {
            logger.trace("bind exception caught on transport layer [{}]", e.getCause(), ctx.getChannel());
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            ctx.getChannel().close();
            disconnectFromNodeChannel(ctx.getChannel(), e.getCause());
        } else if (e.getCause() instanceof CancelledKeyException) {
            logger.trace("cancelled key exception caught on transport layer [{}], disconnecting from relevant node", e.getCause(),
                    ctx.getChannel());
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            ctx.getChannel().close();
            disconnectFromNodeChannel(ctx.getChannel(), e.getCause());
        } else if (e.getCause() instanceof SizeHeaderFrameDecoder.HttpOnTransportException) {
            // in case we are able to return data, serialize the exception content and sent it back to the client
            if (ctx.getChannel().isOpen()) {
                ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(e.getCause().getMessage().getBytes(StandardCharsets.UTF_8));
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
    public List<String> getLocalAddresses() {
        List<String> local = new ArrayList<>();
        local.add("127.0.0.1");
        // check if v6 is supported, if so, v4 will also work via mapped addresses.
        if (NetworkUtils.SUPPORTS_V6) {
            local.add("[::1]"); // may get ports appended!
        }
        return local;
    }

    @Override
    public void sendRequest(final DiscoveryNode node, final long requestId, final String action, final TransportRequest request,
            TransportRequestOptions options) throws IOException, TransportException {

        Channel targetChannel = nodeChannel(node, options);

        if (compress) {
            options = TransportRequestOptions.builder(options).withCompress(true).build();
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
            Version version = Version.smallest(this.version, node.getVersion());

            stream.setVersion(version);
            threadPool.getThreadContext().writeTo(stream);
            stream.writeString(action);

            ReleasablePagedBytesReference bytes;
            ChannelBuffer buffer;
            // it might be nice to somehow generalize this optimization, maybe a smart "paged" bytes output
            // that create paged channel buffers, but its tricky to know when to do it (where this option is
            // more explicit).
            if (request instanceof BytesTransportRequest) {
                BytesTransportRequest bRequest = (BytesTransportRequest) request;
                assert node.getVersion().equals(bRequest.version());
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
            final TransportRequestOptions finalOptions = options;
            ChannelFutureListener channelFutureListener =
                f -> transportServiceAdapter.onRequestSent(node, requestId, action, request, finalOptions);
            future.addListener(channelFutureListener);
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
            throw new IllegalStateException("can't add nodes to a stopped transport");
        }
        if (node == null) {
            throw new ConnectTransportException(null, "can't connect to a null node");
        }
        globalLock.readLock().lock();
        try {

            try (Releasable ignored = connectionLock.acquire(node.getId())) {
                if (!lifecycle.started()) {
                    throw new IllegalStateException("can't add nodes to a stopped transport");
                }
                NodeChannels nodeChannels = connectedNodes.get(node);
                if (nodeChannels != null) {
                    return;
                }
                try {
                    if (light) {
                        nodeChannels = connectToChannelsLight(node);
                    } else {
                        nodeChannels = new NodeChannels(new Channel[connectionsPerNodeRecovery], new Channel[connectionsPerNodeBulk],
                                new Channel[connectionsPerNodeReg], new Channel[connectionsPerNodeState],
                                new Channel[connectionsPerNodePing]);
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
            }
        } finally {
            globalLock.readLock().unlock();
        }
    }

    protected NodeChannels connectToChannelsLight(DiscoveryNode node) {
        InetSocketAddress address = ((InetSocketTransportAddress) node.getAddress()).address();
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
        InetSocketAddress address = ((InetSocketTransportAddress) node.getAddress()).address();
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
            List<ChannelFuture> futures = new ArrayList<>();
            futures.addAll(Arrays.asList(connectRecovery));
            futures.addAll(Arrays.asList(connectBulk));
            futures.addAll(Arrays.asList(connectReg));
            futures.addAll(Arrays.asList(connectState));
            futures.addAll(Arrays.asList(connectPing));
            for (ChannelFuture future : Collections.unmodifiableList(futures)) {
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

        try (Releasable ignored = connectionLock.acquire(node.getId())) {
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
            try (Releasable ignored = connectionLock.acquire(node.getId())) {
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
        protected final NettyTransport nettyTransport;

        public ClientChannelPipelineFactory(NettyTransport nettyTransport) {
            this.nettyTransport = nettyTransport;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline channelPipeline = Channels.pipeline();
            SizeHeaderFrameDecoder sizeHeader = new SizeHeaderFrameDecoder();
            if (nettyTransport.maxCumulationBufferCapacity.bytes() >= 0) {
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
            if (nettyTransport.maxCumulationBufferCapacity.bytes() > 0) {
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

        List<Channel> allChannels = Collections.emptyList();
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
            List<Channel> newAllChannels = new ArrayList<>();
            newAllChannels.addAll(Arrays.asList(recovery));
            newAllChannels.addAll(Arrays.asList(bulk));
            newAllChannels.addAll(Arrays.asList(reg));
            newAllChannels.addAll(Arrays.asList(state));
            newAllChannels.addAll(Arrays.asList(ping));
            this.allChannels = Collections.unmodifiableList(newAllChannels);
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
                return reg[Math.floorMod(regCounter.incrementAndGet(), reg.length)];
            } else if (type == TransportRequestOptions.Type.STATE) {
                return state[Math.floorMod(stateCounter.incrementAndGet(), state.length)];
            } else if (type == TransportRequestOptions.Type.PING) {
                return ping[Math.floorMod(pingCounter.incrementAndGet(), ping.length)];
            } else if (type == TransportRequestOptions.Type.BULK) {
                return bulk[Math.floorMod(bulkCounter.incrementAndGet(), bulk.length)];
            } else if (type == TransportRequestOptions.Type.RECOVERY) {
                return recovery[Math.floorMod(recoveryCounter.incrementAndGet(), recovery.length)];
            } else {
                throw new IllegalArgumentException("no type channel for [" + type + "]");
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

    class ScheduledPing extends AbstractLifecycleRunnable {

        final CounterMetric successfulPings = new CounterMetric();
        final CounterMetric failedPings = new CounterMetric();

        public ScheduledPing() {
            super(lifecycle, logger);
        }

        @Override
        protected void doRunInLifecycle() throws Exception {
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
        }

        @Override
        protected void onAfterInLifecycle() {
            threadPool.schedule(pingSchedule, ThreadPool.Names.GENERIC, this);
        }

        @Override
        public void onFailure(Throwable t) {
            if (lifecycle.stoppedOrClosed()) {
                logger.trace("failed to send ping transport message", t);
            } else {
                logger.warn("failed to send ping transport message", t);
            }
        }
    }
}
