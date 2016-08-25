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
package org.elasticsearch.transport;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.compress.NotCompressedException;
import org.elasticsearch.common.io.ReleasableBytesStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractLifecycleRunnable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.support.TransportStatus;

import java.io.Closeable;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isCloseConnectionException;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isConnectException;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public abstract class TcpTransport<Channel> extends AbstractLifecycleComponent implements Transport {

    public static final String HTTP_SERVER_WORKER_THREAD_NAME_PREFIX = "http_server_worker";
    public static final String HTTP_SERVER_BOSS_THREAD_NAME_PREFIX = "http_server_boss";
    public static final String TRANSPORT_CLIENT_WORKER_THREAD_NAME_PREFIX = "transport_client_worker";
    public static final String TRANSPORT_CLIENT_BOSS_THREAD_NAME_PREFIX = "transport_client_boss";

    // the scheduled internal ping interval setting, defaults to disabled (-1)
    public static final Setting<TimeValue> PING_SCHEDULE =
        timeSetting("transport.ping_schedule", TimeValue.timeValueSeconds(-1), Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_RECOVERY =
        intSetting("transport.connections_per_node.recovery", 2, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_BULK =
        intSetting("transport.connections_per_node.bulk", 3, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_REG =
        intSetting("transport.connections_per_node.reg", 6, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_STATE =
        intSetting("transport.connections_per_node.state", 1, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_PING =
        intSetting("transport.connections_per_node.ping", 1, 1, Setting.Property.NodeScope);
    public static final Setting<TimeValue> TCP_CONNECT_TIMEOUT =
        timeSetting("transport.tcp.connect_timeout", NetworkService.TcpSettings.TCP_CONNECT_TIMEOUT, Setting.Property.NodeScope);
    public static final Setting<Boolean> TCP_NO_DELAY =
        boolSetting("transport.tcp_no_delay", NetworkService.TcpSettings.TCP_NO_DELAY, Setting.Property.NodeScope);
    public static final Setting<Boolean> TCP_KEEP_ALIVE =
        boolSetting("transport.tcp.keep_alive", NetworkService.TcpSettings.TCP_KEEP_ALIVE, Setting.Property.NodeScope);
    public static final Setting<Boolean> TCP_REUSE_ADDRESS =
        boolSetting("transport.tcp.reuse_address", NetworkService.TcpSettings.TCP_REUSE_ADDRESS, Setting.Property.NodeScope);
    public static final Setting<Boolean> TCP_BLOCKING_CLIENT =
        boolSetting("transport.tcp.blocking_client", NetworkService.TcpSettings.TCP_BLOCKING_CLIENT, Setting.Property.NodeScope);
    public static final Setting<Boolean> TCP_BLOCKING_SERVER =
        boolSetting("transport.tcp.blocking_server", NetworkService.TcpSettings.TCP_BLOCKING_SERVER, Setting.Property.NodeScope);
    public static final Setting<ByteSizeValue> TCP_SEND_BUFFER_SIZE =
        Setting.byteSizeSetting("transport.tcp.send_buffer_size", NetworkService.TcpSettings.TCP_SEND_BUFFER_SIZE,
            Setting.Property.NodeScope);
    public static final Setting<ByteSizeValue> TCP_RECEIVE_BUFFER_SIZE =
        Setting.byteSizeSetting("transport.tcp.receive_buffer_size", NetworkService.TcpSettings.TCP_RECEIVE_BUFFER_SIZE,
            Setting.Property.NodeScope);

    private static final long NINETY_PER_HEAP_SIZE = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().bytes() * 0.9);
    private static final int PING_DATA_SIZE = -1;

    protected final int connectionsPerNodeRecovery;
    protected final int connectionsPerNodeBulk;
    protected final int connectionsPerNodeReg;
    protected final int connectionsPerNodeState;
    protected final int connectionsPerNodePing;
    protected final TimeValue connectTimeout;
    protected final boolean blockingClient;
    private final CircuitBreakerService circuitBreakerService;
    // package visibility for tests
    protected final ScheduledPing scheduledPing;
    private final TimeValue pingSchedule;
    protected final ThreadPool threadPool;
    private final BigArrays bigArrays;
    protected final NetworkService networkService;

    protected volatile TransportServiceAdapter transportServiceAdapter;
    // node id to actual channel
    protected final ConcurrentMap<DiscoveryNode, NodeChannels> connectedNodes = newConcurrentMap();
    protected final Map<String, List<Channel>> serverChannels = newConcurrentMap();
    protected final ConcurrentMap<String, BoundTransportAddress> profileBoundAddresses = newConcurrentMap();

    protected final KeyedLock<String> connectionLock = new KeyedLock<>();
    private final NamedWriteableRegistry namedWriteableRegistry;

    // this lock is here to make sure we close this transport and disconnect all the client nodes
    // connections while no connect operations is going on... (this might help with 100% CPU when stopping the transport?)
    protected final ReadWriteLock globalLock = new ReentrantReadWriteLock();
    protected final boolean compress;
    protected volatile BoundTransportAddress boundAddress;
    private final String transportName;

    public TcpTransport(String transportName, Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                        CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                        NetworkService networkService) {
        super(settings);
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.circuitBreakerService = circuitBreakerService;
        this.scheduledPing = new ScheduledPing();
        this.pingSchedule = PING_SCHEDULE.get(settings);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.compress = Transport.TRANSPORT_TCP_COMPRESS.get(settings);
        this.networkService = networkService;
        this.transportName = transportName;

        this.connectionsPerNodeRecovery = CONNECTIONS_PER_NODE_RECOVERY.get(settings);
        this.connectionsPerNodeBulk = CONNECTIONS_PER_NODE_BULK.get(settings);
        this.connectionsPerNodeReg = CONNECTIONS_PER_NODE_REG.get(settings);
        this.connectionsPerNodeState = CONNECTIONS_PER_NODE_STATE.get(settings);
        this.connectionsPerNodePing = CONNECTIONS_PER_NODE_PING.get(settings);
        this.connectTimeout = TCP_CONNECT_TIMEOUT.get(settings);
        this.blockingClient = TCP_BLOCKING_CLIENT.get(settings);
    }

    @Override
    protected void doStart() {
        if (pingSchedule.millis() > 0) {
            threadPool.schedule(pingSchedule, ThreadPool.Names.GENERIC, scheduledPing);
        }
    }

    @Override
    public CircuitBreaker getInFlightRequestBreaker() {
        // We always obtain a fresh breaker to reflect changes to the breaker configuration.
        return circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }

    @Override
    public void transportServiceAdapter(TransportServiceAdapter service) {
        this.transportServiceAdapter = service;
    }

    public Settings settings() {
        return this.settings;
    }

    public boolean isCompressed() {
        return compress;
    }

    public class ScheduledPing extends AbstractLifecycleRunnable {

        /**
         * The magic number (must be lower than 0) for a ping message. This is handled
         * specifically in {@link TcpTransport#validateMessageHeader}.
         */
        private final BytesReference pingHeader;
        final CounterMetric successfulPings = new CounterMetric();
        final CounterMetric failedPings = new CounterMetric();

        public ScheduledPing() {
            super(lifecycle, logger);
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeByte((byte) 'E');
                out.writeByte((byte) 'S');
                out.writeInt(PING_DATA_SIZE);
                pingHeader = out.bytes();
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage(), e); // won't happen
            }
        }

        @Override
        protected void doRunInLifecycle() throws Exception {
            for (Map.Entry<DiscoveryNode, NodeChannels> entry : connectedNodes.entrySet()) {
                DiscoveryNode node = entry.getKey();
                NodeChannels channels = entry.getValue();
                for (Channel channel : channels.allChannels) {
                    try {
                        sendMessage(channel, pingHeader, successfulPings::inc, false);
                    } catch (Exception e) {
                        if (isOpen(channel)) {
                            logger.debug("[{}] failed to send ping transport message", e, node);
                            failedPings.inc();
                        } else {
                            logger.trace("[{}] failed to send ping transport message (channel closed)", e, node);
                        }
                    }
                }
            }
        }

        public long getSuccessfulPings() {
            return successfulPings.count();
        }

        public long getFailedPings() {
            return failedPings.count();
        }

        @Override
        protected void onAfterInLifecycle() {
            threadPool.schedule(pingSchedule, ThreadPool.Names.GENERIC, this);
        }

        @Override
        public void onFailure(Exception e) {
            if (lifecycle.stoppedOrClosed()) {
                logger.trace("failed to send ping transport message", e);
            } else {
                logger.warn("failed to send ping transport message", e);
            }
        }
    }

    public class NodeChannels implements Closeable {

        public List<Channel> allChannels = Collections.emptyList();
        public Channel[] recovery;
        public final AtomicInteger recoveryCounter = new AtomicInteger();
        public Channel[] bulk;
        public final AtomicInteger bulkCounter = new AtomicInteger();
        public Channel[] reg;
        public final AtomicInteger regCounter = new AtomicInteger();
        public Channel[] state;
        public final AtomicInteger stateCounter = new AtomicInteger();
        public Channel[] ping;
        public final AtomicInteger pingCounter = new AtomicInteger();

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

        public List<Channel[]> getChannelArrays() {
            return Arrays.asList(recovery, bulk, reg, state, ping);
        }

        public synchronized void close() throws IOException {
            closeChannels(allChannels);
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
                        try {
                            nodeChannels = connectToChannels(node);
                        } catch (Exception e) {
                            logger.trace("failed to connect to [{}], cleaning dangling connections", e, node);
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
                        IOUtils.closeWhileHandlingException(nodeChannels);
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
    protected final void disconnectFromNodeChannel(final Channel channel, final Exception failure) {
        threadPool.generic().execute(() -> {
            try {
                try {
                    closeChannels(Collections.singletonList(channel));
                } finally {
                    for (DiscoveryNode node : connectedNodes.keySet()) {
                        if (disconnectFromNode(node, channel, ExceptionsHelper.detailedMessage(failure))) {
                            // if we managed to find this channel and disconnect from it, then break, no need to check on
                            // the rest of the nodes
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                logger.warn("failed to close channel", e);
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

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        try (Releasable ignored = connectionLock.acquire(node.getId())) {
            NodeChannels nodeChannels = connectedNodes.remove(node);
            if (nodeChannels != null) {
                try {
                    logger.debug("disconnecting from [{}] due to explicit disconnect call", node);
                    IOUtils.closeWhileHandlingException(nodeChannels);
                } finally {
                    logger.trace("disconnected from [{}] due to explicit disconnect call", node);
                    transportServiceAdapter.raiseNodeDisconnected(node);
                }
            }
        }
    }

    protected Version getCurrentVersion() {
        // this is just for tests to mock stuff like the nodes version - tests can override this internally
        return Version.CURRENT;
    }

    @Override
    public boolean addressSupported(Class<? extends TransportAddress> address) {
        return InetSocketTransportAddress.class.equals(address);
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return unmodifiableMap(new HashMap<>(profileBoundAddresses));
    }

    protected Map<String, Settings> buildProfileSettings() {
        // extract default profile first and create standard bootstrap
        Map<String, Settings> profiles = TransportSettings.TRANSPORT_PROFILES_SETTING.get(settings()).getAsGroups(true);
        if (!profiles.containsKey(TransportSettings.DEFAULT_PROFILE)) {
            profiles = new HashMap<>(profiles);
            profiles.put(TransportSettings.DEFAULT_PROFILE, Settings.EMPTY);
        }
        Settings defaultSettings = profiles.get(TransportSettings.DEFAULT_PROFILE);
        Map<String, Settings> result = new HashMap<>();
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
            Settings mergedSettings = Settings.builder()
                .put(defaultSettings)
                .put(profileSettings)
                .build();
            result.put(name, mergedSettings);
        }
        return result;
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

    protected void bindServer(final String name, final Settings settings) {
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

    protected InetSocketAddress bindToPort(final String name, final InetAddress hostAddress, String port) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = portsRange.iterate(portNumber -> {
            try {
                Channel channel = bind(name, new InetSocketAddress(hostAddress, portNumber));
                synchronized (serverChannels) {
                    List<Channel> list = serverChannels.get(name);
                    if (list == null) {
                        list = new ArrayList<>();
                        serverChannels.put(name, list);
                    }
                    list.add(channel);
                    boundSocket.set(getLocalAddress(channel));
                }
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
            return true;
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
    public static int resolvePublishPort(String profileName, Settings settings, Settings profileSettings,
                                         List<InetSocketAddress> boundAddresses, InetAddress publishInetAddress) {
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
    protected final void doClose() {
    }

    @Override
    protected final void doStop() {
        final CountDownLatch latch = new CountDownLatch(1);
        // make sure we run it on another thread than a possible IO handler thread
        threadPool.generic().execute(() -> {
            globalLock.writeLock().lock();
            try {
                // first stop to accept any incoming connections so nobody can connect to this transport
                for (Map.Entry<String, List<Channel>> entry : serverChannels.entrySet()) {
                    try {
                        closeChannels(entry.getValue());
                    } catch (Exception e) {
                        logger.debug("Error closing serverChannel for profile [{}]", e, entry.getKey());
                    }
                }

                for (Iterator<NodeChannels> it = connectedNodes.values().iterator(); it.hasNext(); ) {
                    NodeChannels nodeChannels = it.next();
                    it.remove();
                    IOUtils.closeWhileHandlingException(nodeChannels);
                }
                stopInternal();
            } finally {
                globalLock.writeLock().unlock();
                latch.countDown();
            }
        });

        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // ignore
        }
    }

    protected void onException(Channel channel, Exception e) throws IOException {
        if (!lifecycle.started()) {
            // ignore
            return;
        }
        if (isCloseConnectionException(e)) {
            logger.trace("close connection exception caught on transport layer [{}], disconnecting from relevant node", e,
                channel);
            // close the channel, which will cause a node to be disconnected if relevant
            disconnectFromNodeChannel(channel, e);
        } else if (isConnectException(e)) {
            logger.trace("connect exception caught on transport layer [{}]", e, channel);
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            disconnectFromNodeChannel(channel, e);
        } else if (e instanceof BindException) {
            logger.trace("bind exception caught on transport layer [{}]", e, channel);
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            disconnectFromNodeChannel(channel, e);
        } else if (e instanceof CancelledKeyException) {
            logger.trace("cancelled key exception caught on transport layer [{}], disconnecting from relevant node", e,
                channel);
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            disconnectFromNodeChannel(channel, e);
        } else if (e instanceof TcpTransport.HttpOnTransportException) {
            // in case we are able to return data, serialize the exception content and sent it back to the client
            if (isOpen(channel)) {
                sendMessage(channel, new BytesArray(e.getMessage().getBytes(StandardCharsets.UTF_8)), () -> {}, true);
            }
        } else {
            logger.warn("exception caught on transport layer [{}], closing connection", e, channel);
            // close the channel, which will cause a node to be disconnected if relevant
            disconnectFromNodeChannel(channel, e);
        }
    }

    /**
     * Returns the channels local address
     */
    protected abstract InetSocketAddress getLocalAddress(Channel channel);

    /**
     * Binds to the given {@link InetSocketAddress}
     * @param name the profile name
     * @param address the address to bind to
     */
    protected abstract Channel bind(String name, InetSocketAddress address) throws IOException;

    /**
     * Closes all channels in this list
     */
    protected abstract void closeChannels(List<Channel> channel) throws IOException;

    /**
     * Connects to the given node in a light way. This means we are not creating multiple connections like we do
     * for production connections. This connection is for pings or handshakes
     */
    protected abstract NodeChannels connectToChannelsLight(DiscoveryNode node) throws IOException;


    protected abstract void sendMessage(Channel channel, BytesReference reference, Runnable sendListener, boolean close) throws IOException;

    /**
     * Connects to the node in a <tt>heavy</tt> way.
     *
     * @see #connectToChannelsLight(DiscoveryNode)
     */
    protected abstract NodeChannels connectToChannels(DiscoveryNode node) throws IOException;

    /**
     * Called to tear down internal resources
     */
    protected void stopInternal() {}

    public boolean canCompress(TransportRequest request) {
        return compress && (!(request instanceof BytesTransportRequest));
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
        StreamOutput stream = bStream;
        try {
            // only compress if asked, and, the request is not bytes, since then only
            // the header part is compressed, and the "body" can't be extracted as compressed
            if (options.compress() && canCompress(request)) {
                status = TransportStatus.setCompress(status);
                stream = CompressorFactory.COMPRESSOR.streamOutput(stream);
            }

            // we pick the smallest of the 2, to support both backward and forward compatibility
            // note, this is the only place we need to do this, since from here on, we use the serialized version
            // as the version to use also when the node receiving this request will send the response with
            Version version = Version.smallest(getCurrentVersion(), node.getVersion());

            stream.setVersion(version);
            threadPool.getThreadContext().writeTo(stream);
            stream.writeString(action);
            BytesReference message = buildMessage(requestId, status, node.getVersion(), request, stream, bStream);
            final TransportRequestOptions finalOptions = options;
            Runnable onRequestSent = () -> {
                try {
                    Releasables.close(bStream.bytes());
                } finally {
                    transportServiceAdapter.onRequestSent(node, requestId, action, request, finalOptions);
                }
            };
            addedReleaseListener = internalSendMessage(targetChannel, message, onRequestSent);
        } finally {
            IOUtils.close(stream);
            if (!addedReleaseListener) {
                Releasables.close(bStream.bytes());
            }
        }
    }

    /**
     * sends a message view the given channel, using the given callbacks.
     *
     * @return true if the message was successfully sent or false when an error occurred and the error hanlding logic was activated
     *
     */
    private boolean internalSendMessage(Channel targetChannel, BytesReference message, Runnable onRequestSent) throws IOException {
        boolean success;
        try {
            sendMessage(targetChannel, message, onRequestSent, false);
            success = true;
        } catch (IOException ex) {
            // passing exception handling to deal with this and raise disconnect events and decide the right logging level
            onException(targetChannel, ex);
            success = false;
        }
        return success;
    }

    /**
     * Sends back an error response to the caller via the given channel
     * @param nodeVersion the caller node version
     * @param channel the channel to send the response to
     * @param error the error to return
     * @param requestId the request ID this response replies to
     * @param action the action this response replies to
     */
    public void sendErrorResponse(Version nodeVersion, Channel channel, final Exception error, final long requestId,
                                  final String action) throws IOException {
        try (BytesStreamOutput stream = new BytesStreamOutput()) {
            stream.setVersion(nodeVersion);
            RemoteTransportException tx = new RemoteTransportException(
                nodeName(), new InetSocketTransportAddress(getLocalAddress(channel)), action, error);
            threadPool.getThreadContext().writeTo(stream);
            stream.writeException(tx);
            byte status = 0;
            status = TransportStatus.setResponse(status);
            status = TransportStatus.setError(status);
            final BytesReference bytes = stream.bytes();
            final BytesReference header = buildHeader(requestId, status, nodeVersion, bytes.length());
            Runnable onRequestSent = () -> transportServiceAdapter.onResponseSent(requestId, action, error);
            sendMessage(channel, new CompositeBytesReference(header, bytes), onRequestSent, false);
        }
    }

    /**
     * Sends the response to the given channel. This method should be used to send {@link TransportResponse} objects back to the caller.
     *
     * @see #sendErrorResponse(Version, Object, Exception, long, String) for sending back errors to the caller
     */
    public void sendResponse(Version nodeVersion, Channel channel, final TransportResponse response, final long requestId,
                             final String action, TransportResponseOptions options) throws IOException {
        if (compress) {
            options = TransportResponseOptions.builder(options).withCompress(true).build();
        }
        byte status = 0;
        status = TransportStatus.setResponse(status); // TODO share some code with sendRequest
        ReleasableBytesStreamOutput bStream = new ReleasableBytesStreamOutput(bigArrays);
        boolean addedReleaseListener = false;
        StreamOutput stream = bStream;
        try {
            if (options.compress()) {
                status = TransportStatus.setCompress(status);
                stream = CompressorFactory.COMPRESSOR.streamOutput(stream);
            }
            threadPool.getThreadContext().writeTo(stream);
            stream.setVersion(nodeVersion);
            BytesReference reference = buildMessage(requestId, status, nodeVersion, response, stream, bStream);

            final TransportResponseOptions finalOptions = options;
            Runnable onRequestSent = () -> {
                try {
                    Releasables.close(bStream.bytes());
                } finally {
                    transportServiceAdapter.onResponseSent(requestId, action, response, finalOptions);
                }
            };
            addedReleaseListener = internalSendMessage(channel, reference, onRequestSent);
        } finally {
            IOUtils.close(stream);
            if (!addedReleaseListener) {
                Releasables.close(bStream.bytes());
            }
        }
    }

    /**
     * Writes the Tcp message header into a bytes reference.
     *
     * @param requestId       the request ID
     * @param status          the request status
     * @param protocolVersion the protocol version used to serialize the data in the message
     * @param length          the payload length in bytes
     * @see TcpHeader
     */
    private BytesReference buildHeader(long requestId, byte status, Version protocolVersion, int length) throws IOException {
        try (BytesStreamOutput headerOutput = new BytesStreamOutput(TcpHeader.HEADER_SIZE)) {
            headerOutput.setVersion(protocolVersion);
            TcpHeader.writeHeader(headerOutput, requestId, status, protocolVersion, length);
            final BytesReference bytes = headerOutput.bytes();
            assert bytes.length() == TcpHeader.HEADER_SIZE : "header size mismatch expected: " + TcpHeader.HEADER_SIZE + " but was: "
                + bytes.length();
            return bytes;
        }
    }

    /**
     * Serializes the given message into a bytes representation
     */
    private BytesReference buildMessage(long requestId, byte status, Version nodeVersion, TransportMessage message, StreamOutput stream,
                                        ReleasableBytesStream writtenBytes) throws IOException {
        final BytesReference zeroCopyBuffer;
        if (message instanceof BytesTransportRequest) { // what a shitty optimization - we should use a direct send method instead
            BytesTransportRequest bRequest = (BytesTransportRequest) message;
            assert nodeVersion.equals(bRequest.version());
            bRequest.writeThin(stream);
            zeroCopyBuffer = bRequest.bytes;
        } else {
            message.writeTo(stream);
            zeroCopyBuffer = BytesArray.EMPTY;
        }
        // we have to close the stream here - flush is not enough since we might be compressing the content
        // and if we do that the close method will write some marker bytes (EOS marker) and otherwise
        // we barf on the decompressing end when we read past EOF on purpose in the #validateRequest method.
        // this might be a problem in deflate after all but it's important to close it for now.
        stream.close();
        final BytesReference messageBody = writtenBytes.bytes();
        final BytesReference header = buildHeader(requestId, status, stream.getVersion(), messageBody.length() + zeroCopyBuffer.length());
        return new CompositeBytesReference(header, messageBody, zeroCopyBuffer);
    }

    /**
     * Validates the first N bytes of the message header and returns <code>false</code> if the message is
     * a ping message and has no payload ie. isn't a real user level message.
     *
     * @throws IllegalStateException if the message is too short, less than the header or less that the header plus the message size
     * @throws HttpOnTransportException if the message has no valid header and appears to be a HTTP message
     * @throws IllegalArgumentException if the message is greater that the maximum allowed frame size. This is dependent on the available
     * memory.
     */
    public static boolean validateMessageHeader(BytesReference buffer) throws IOException {
        final int sizeHeaderLength = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
        if (buffer.length() < sizeHeaderLength) {
            throw new IllegalStateException("message size must be >= to the header size");
        }
        int offset = 0;
        if (buffer.get(offset) != 'E' || buffer.get(offset + 1) != 'S') {
            // special handling for what is probably HTTP
            if (bufferStartsWith(buffer, offset, "GET ") ||
                    bufferStartsWith(buffer, offset, "POST ") ||
                    bufferStartsWith(buffer, offset, "PUT ") ||
                    bufferStartsWith(buffer, offset, "HEAD ") ||
                    bufferStartsWith(buffer, offset, "DELETE ") ||
                    bufferStartsWith(buffer, offset, "OPTIONS ") ||
                    bufferStartsWith(buffer, offset, "PATCH ") ||
                    bufferStartsWith(buffer, offset, "TRACE ")) {

                throw new HttpOnTransportException("This is not a HTTP port");
            }

            // we have 6 readable bytes, show 4 (should be enough)
            throw new StreamCorruptedException("invalid internal transport message format, got ("
                    + Integer.toHexString(buffer.get(offset) & 0xFF) + ","
                    + Integer.toHexString(buffer.get(offset + 1) & 0xFF) + ","
                    + Integer.toHexString(buffer.get(offset + 2) & 0xFF) + ","
                    + Integer.toHexString(buffer.get(offset + 3) & 0xFF) + ")");
        }

        final int dataLen;
        try (StreamInput input = buffer.streamInput()) {
            input.skip(TcpHeader.MARKER_BYTES_SIZE);
            dataLen = input.readInt();
            if (dataLen == PING_DATA_SIZE) {
                // discard the messages we read and continue, this is achieved by skipping the bytes
                // and returning null
                return false;
            }
        }
        if (dataLen <= 0) {
            throw new StreamCorruptedException("invalid data length: " + dataLen);
        }
        // safety against too large frames being sent
        if (dataLen > NINETY_PER_HEAP_SIZE) {
            throw new IllegalArgumentException("transport content length received [" + new ByteSizeValue(dataLen) + "] exceeded ["
                    + new ByteSizeValue(NINETY_PER_HEAP_SIZE) + "]");
        }

        if (buffer.length() < dataLen + sizeHeaderLength) {
            throw new IllegalStateException("buffer must be >= to the message size but wasn't");
        }
        return true;
    }

    private static boolean bufferStartsWith(BytesReference buffer, int offset, String method) {
        char[] chars = method.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (buffer.get(offset+ i) != chars[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * A helper exception to mark an incoming connection as potentially being HTTP
     * so an appropriate error code can be returned
     */
    public static class HttpOnTransportException extends ElasticsearchException {

        public HttpOnTransportException(String msg) {
            super(msg);
        }

        @Override
        public RestStatus status() {
            return RestStatus.BAD_REQUEST;
        }

        public HttpOnTransportException(StreamInput in) throws IOException{
            super(in);
        }
    }

    protected abstract boolean isOpen(Channel channel);

    /**
     * This method handles the message receive part for both request and responses
     */
    public final void messageReceived(BytesReference reference, Channel channel, String profileName,
                                      InetSocketAddress remoteAddress, int messageLengthBytes) throws IOException {
        final int totalMessageSize = messageLengthBytes + TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
        transportServiceAdapter.addBytesReceived(totalMessageSize);
        // we have additional bytes to read, outside of the header
        boolean hasMessageBytesToRead = (totalMessageSize - TcpHeader.HEADER_SIZE) > 0;
        StreamInput streamIn = reference.streamInput();
        boolean success = false;
        try (ThreadContext.StoredContext tCtx = threadPool.getThreadContext().stashContext()) {
            long requestId = streamIn.readLong();
            byte status = streamIn.readByte();
            Version version = Version.fromId(streamIn.readInt());
            if (TransportStatus.isCompress(status) && hasMessageBytesToRead && streamIn.available() > 0) {
                Compressor compressor;
                try {
                    final int bytesConsumed = TcpHeader.REQUEST_ID_SIZE + TcpHeader.STATUS_SIZE + TcpHeader.VERSION_ID_SIZE;
                    compressor = CompressorFactory.compressor(reference.slice(bytesConsumed, reference.length() - bytesConsumed));
                } catch (NotCompressedException ex) {
                    int maxToRead = Math.min(reference.length(), 10);
                    StringBuilder sb = new StringBuilder("stream marked as compressed, but no compressor found, first [").append(maxToRead)
                        .append("] content bytes out of [").append(reference.length())
                        .append("] readable bytes with message size [").append(messageLengthBytes).append("] ").append("] are [");
                    for (int i = 0; i < maxToRead; i++) {
                        sb.append(reference.get(i)).append(",");
                    }
                    sb.append("]");
                    throw new IllegalStateException(sb.toString());
                }
                streamIn = compressor.streamInput(streamIn);
            }
            if (version.onOrAfter(Version.CURRENT.minimumCompatibilityVersion()) == false || version.major != Version.CURRENT.major) {
                throw new IllegalStateException("Received message from unsupported version: [" + version
                    + "] minimal compatible version is: [" +Version.CURRENT.minimumCompatibilityVersion() + "]");
            }
            streamIn = new NamedWriteableAwareStreamInput(streamIn, namedWriteableRegistry);
            streamIn.setVersion(version);
            threadPool.getThreadContext().readHeaders(streamIn);
            if (TransportStatus.isRequest(status)) {
                handleRequest(channel, profileName, streamIn, requestId, messageLengthBytes, version, remoteAddress);
            } else {
                final TransportResponseHandler<?> handler = transportServiceAdapter.onResponseReceived(requestId);
                // ignore if its null, the adapter logs it
                if (handler != null) {
                    if (TransportStatus.isError(status)) {
                        handlerResponseError(streamIn, handler);
                    } else {
                        handleResponse(remoteAddress, streamIn, handler);
                    }
                    // Check the entire message has been read
                    final int nextByte = streamIn.read();
                    // calling read() is useful to make sure the message is fully read, even if there is an EOS marker
                    if (nextByte != -1) {
                        throw new IllegalStateException("Message not fully read (response) for requestId [" + requestId + "], handler ["
                            + handler + "], error [" + TransportStatus.isError(status) + "]; resetting");
                    }
                }
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(streamIn);
            } else {
                IOUtils.closeWhileHandlingException(streamIn);
            }
        }
    }

    private void handleResponse(InetSocketAddress remoteAddress, final StreamInput stream, final TransportResponseHandler handler) {
        final TransportResponse response = handler.newInstance();
        response.remoteAddress(new InetSocketTransportAddress(remoteAddress));
        try {
            response.readFrom(stream);
        } catch (Exception e) {
            handleException(handler, new TransportSerializationException(
                "Failed to deserialize response of type [" + response.getClass().getName() + "]", e));
            return;
        }
        threadPool.executor(handler.executor()).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                handleException(handler, new ResponseHandlerFailureTransportException(e));
            }

            @Override
            protected void doRun() throws Exception {
                handler.handleResponse(response);
            }});

    }

    /**
     * Executed for a received response error
     */
    private void handlerResponseError(StreamInput stream, final TransportResponseHandler handler) {
        Exception error;
        try {
            error = stream.readException();
        } catch (Exception e) {
            error = new TransportSerializationException("Failed to deserialize exception response from stream", e);
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException(error.getMessage(), error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        threadPool.executor(handler.executor()).execute(() -> {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error("failed to handle exception response [{}]", e, handler);
            }
        });
    }

    protected String handleRequest(Channel channel, String profileName, final StreamInput stream, long requestId,
                                   int messageLengthBytes, Version version, InetSocketAddress remoteAddress) throws IOException {
        final String action = stream.readString();
        transportServiceAdapter.onRequestReceived(requestId, action);
        TransportChannel transportChannel = null;
        try {
            final RequestHandlerRegistry reg = transportServiceAdapter.getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException(action);
            }
            if (reg.canTripCircuitBreaker()) {
                getInFlightRequestBreaker().addEstimateBytesAndMaybeBreak(messageLengthBytes, "<transport_request>");
            } else {
                getInFlightRequestBreaker().addWithoutBreaking(messageLengthBytes);
            }
            transportChannel = new TcpTransportChannel<>(this, channel, transportName, action, requestId, version, profileName,
                messageLengthBytes);
            final TransportRequest request = reg.newRequest();
            request.remoteAddress(new InetSocketTransportAddress(remoteAddress));
            request.readFrom(stream);
            // in case we throw an exception, i.e. when the limit is hit, we don't want to verify
            validateRequest(stream, requestId, action);
            threadPool.executor(reg.getExecutor()).execute(new RequestHandler(reg, request, transportChannel));
        } catch (Exception e) {
            // the circuit breaker tripped
            if (transportChannel == null) {
                transportChannel = new TcpTransportChannel<>(this, channel, transportName, action, requestId, version, profileName, 0);
            }
            try {
                transportChannel.sendResponse(e);
            } catch (IOException inner) {
                inner.addSuppressed(e);
                logger.warn("Failed to send error message back to client for action [{}]", inner, action);
            }
        }
        return action;
    }

    // This template method is needed to inject custom error checking logic in tests.
    protected void validateRequest(StreamInput stream, long requestId, String action) throws IOException {
        final int nextByte = stream.read();
        // calling read() is useful to make sure the message is fully read, even if there some kind of EOS marker
        if (nextByte != -1) {
            throw new IllegalStateException("Message not fully read (request) for requestId [" + requestId + "], action [" + action
                + "], available [" + stream.available() + "]; resetting");
        }
    }

    class RequestHandler extends AbstractRunnable {
        private final RequestHandlerRegistry reg;
        private final TransportRequest request;
        private final TransportChannel transportChannel;

        public RequestHandler(RequestHandlerRegistry reg, TransportRequest request, TransportChannel transportChannel) {
            this.reg = reg;
            this.request = request;
            this.transportChannel = transportChannel;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        protected void doRun() throws Exception {
            reg.processMessageReceived(request, transportChannel);
        }

        @Override
        public boolean isForceExecution() {
            return reg.isForceExecution();
        }

        @Override
        public void onFailure(Exception e) {
            if (lifecycleState() == Lifecycle.State.STARTED) {
                // we can only send a response transport is started....
                try {
                    transportChannel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.warn("Failed to send error message back to client for action [{}]", inner, reg.getAction());
                }
            }
        }
    }
}
