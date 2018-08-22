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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.compress.NotCompressedException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.CancelledKeyException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.settings.Setting.affixKeySetting;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.settings.Setting.listSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isCloseConnectionException;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isConnectException;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public abstract class TcpTransport extends AbstractLifecycleComponent implements Transport {

    public static final String TRANSPORT_SERVER_WORKER_THREAD_NAME_PREFIX = "transport_server_worker";
    public static final String TRANSPORT_CLIENT_BOSS_THREAD_NAME_PREFIX = "transport_client_boss";

    public static final Setting<List<String>> HOST =
        listSetting("transport.host", emptyList(), Function.identity(), Setting.Property.NodeScope);
    public static final Setting<List<String>> BIND_HOST =
        listSetting("transport.bind_host", HOST, Function.identity(), Setting.Property.NodeScope);
    public static final Setting<List<String>> PUBLISH_HOST =
        listSetting("transport.publish_host", HOST, Function.identity(), Setting.Property.NodeScope);
    public static final Setting<String> PORT =
        new Setting<>("transport.tcp.port", "9300-9400", Function.identity(), Setting.Property.NodeScope);
    public static final Setting<Integer> PUBLISH_PORT =
        intSetting("transport.publish_port", -1, -1, Setting.Property.NodeScope);
    public static final String DEFAULT_PROFILE = "default";
    // the scheduled internal ping interval setting, defaults to disabled (-1)
    public static final Setting<TimeValue> PING_SCHEDULE =
        timeSetting("transport.ping_schedule", TimeValue.timeValueSeconds(-1), Setting.Property.NodeScope);
    public static final Setting<Boolean> TCP_NO_DELAY =
        boolSetting("transport.tcp_no_delay", NetworkService.TCP_NO_DELAY, Setting.Property.NodeScope);
    public static final Setting<Boolean> TCP_KEEP_ALIVE =
        boolSetting("transport.tcp.keep_alive", NetworkService.TCP_KEEP_ALIVE, Setting.Property.NodeScope);
    public static final Setting<Boolean> TCP_REUSE_ADDRESS =
        boolSetting("transport.tcp.reuse_address", NetworkService.TCP_REUSE_ADDRESS, Setting.Property.NodeScope);
    public static final Setting<ByteSizeValue> TCP_SEND_BUFFER_SIZE =
        Setting.byteSizeSetting("transport.tcp.send_buffer_size", NetworkService.TCP_SEND_BUFFER_SIZE, Setting.Property.NodeScope);
    public static final Setting<ByteSizeValue> TCP_RECEIVE_BUFFER_SIZE =
        Setting.byteSizeSetting("transport.tcp.receive_buffer_size", NetworkService.TCP_RECEIVE_BUFFER_SIZE, Setting.Property.NodeScope);


    public static final Setting.AffixSetting<Boolean> TCP_NO_DELAY_PROFILE = affixKeySetting("transport.profiles.", "tcp_no_delay",
        key -> boolSetting(key, TcpTransport.TCP_NO_DELAY, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<Boolean> TCP_KEEP_ALIVE_PROFILE = affixKeySetting("transport.profiles.", "tcp_keep_alive",
        key -> boolSetting(key, TcpTransport.TCP_KEEP_ALIVE, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<Boolean> TCP_REUSE_ADDRESS_PROFILE = affixKeySetting("transport.profiles.", "reuse_address",
        key -> boolSetting(key, TcpTransport.TCP_REUSE_ADDRESS, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<ByteSizeValue> TCP_SEND_BUFFER_SIZE_PROFILE = affixKeySetting("transport.profiles.",
        "send_buffer_size", key -> Setting.byteSizeSetting(key, TcpTransport.TCP_SEND_BUFFER_SIZE, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<ByteSizeValue> TCP_RECEIVE_BUFFER_SIZE_PROFILE = affixKeySetting("transport.profiles.",
        "receive_buffer_size", key -> Setting.byteSizeSetting(key, TcpTransport.TCP_RECEIVE_BUFFER_SIZE, Setting.Property.NodeScope));

    public static final Setting.AffixSetting<List<String>> BIND_HOST_PROFILE = affixKeySetting("transport.profiles.", "bind_host",
        key -> listSetting(key, BIND_HOST, Function.identity(), Setting.Property.NodeScope));
    public static final Setting.AffixSetting<List<String>> PUBLISH_HOST_PROFILE = affixKeySetting("transport.profiles.", "publish_host",
        key -> listSetting(key, PUBLISH_HOST, Function.identity(), Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> PORT_PROFILE = affixKeySetting("transport.profiles.", "port",
        key -> new Setting<>(key, PORT, Function.identity(), Setting.Property.NodeScope));
    public static final Setting.AffixSetting<Integer> PUBLISH_PORT_PROFILE = affixKeySetting("transport.profiles.", "publish_port",
        key -> intSetting(key, -1, -1, Setting.Property.NodeScope));

    // This is the number of bytes necessary to read the message size
    public static final int BYTES_NEEDED_FOR_MESSAGE_SIZE = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
    public static final int PING_DATA_SIZE = -1;
    protected final CounterMetric successfulPings = new CounterMetric();
    protected final CounterMetric failedPings = new CounterMetric();
    private static final long NINETY_PER_HEAP_SIZE = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.9);
    private static final BytesReference EMPTY_BYTES_REFERENCE = new BytesArray(new byte[0]);

    public static final String FEATURE_PREFIX = "transport.features";
    public static final Setting<Settings> DEFAULT_FEATURES_SETTING = Setting.groupSetting(FEATURE_PREFIX + ".", Setting.Property.NodeScope);
    private final String[] features;

    private final CircuitBreakerService circuitBreakerService;
    protected final ThreadPool threadPool;
    private final BigArrays bigArrays;
    protected final NetworkService networkService;
    protected final Set<ProfileSettings> profileSettings;

    private final DelegatingTransportMessageListener messageListener = new DelegatingTransportMessageListener();

    private final ConcurrentMap<String, BoundTransportAddress> profileBoundAddresses = newConcurrentMap();
    private final Map<String, List<TcpServerChannel>> serverChannels = newConcurrentMap();
    private final Set<TcpChannel> acceptedChannels = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final NamedWriteableRegistry namedWriteableRegistry;

    // this lock is here to make sure we close this transport and disconnect all the client nodes
    // connections while no connect operations is going on
    private final ReadWriteLock closeLock = new ReentrantReadWriteLock();
    protected final boolean compress;
    private volatile BoundTransportAddress boundAddress;
    private final String transportName;

    private final ConcurrentMap<Long, HandshakeResponseHandler> pendingHandshakes = new ConcurrentHashMap<>();
    private final CounterMetric numHandshakes = new CounterMetric();
    private static final String HANDSHAKE_ACTION_NAME = "internal:tcp/handshake";

    private final MeanMetric readBytesMetric = new MeanMetric();
    private final MeanMetric transmittedBytesMetric = new MeanMetric();
    private volatile Map<String, RequestHandlerRegistry> requestHandlers = Collections.emptyMap();
    private final ResponseHandlers responseHandlers = new ResponseHandlers();
    private final BytesReference pingMessage;

    public TcpTransport(String transportName, Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                        CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                        NetworkService networkService) {
        super(settings);
        this.profileSettings = getProfileSettings(settings);
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.circuitBreakerService = circuitBreakerService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.compress = Transport.TRANSPORT_TCP_COMPRESS.get(settings);
        this.networkService = networkService;
        this.transportName = transportName;
        final Settings defaultFeatures = DEFAULT_FEATURES_SETTING.get(settings);
        if (defaultFeatures == null) {
            this.features = new String[0];
        } else {
            defaultFeatures.names().forEach(key -> {
                if (Booleans.parseBoolean(defaultFeatures.get(key)) == false) {
                    throw new IllegalArgumentException("feature settings must have default [true] value");
                }
            });
            // use a sorted set to present the features in a consistent order
            this.features = new TreeSet<>(defaultFeatures.names()).toArray(new String[defaultFeatures.names().size()]);
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeByte((byte) 'E');
            out.writeByte((byte) 'S');
            out.writeInt(TcpTransport.PING_DATA_SIZE);
            pingMessage = out.bytes();
        } catch (IOException e) {
            throw new AssertionError(e.getMessage(), e); // won't happen
        }
    }

    @Override
    protected void doStart() {
    }

    public void addMessageListener(TransportMessageListener listener) {
        messageListener.listeners.add(listener);
    }

    public boolean removeMessageListener(TransportMessageListener listener) {
        return messageListener.listeners.remove(listener);
    }

    @Override
    public CircuitBreaker getInFlightRequestBreaker() {
        // We always obtain a fresh breaker to reflect changes to the breaker configuration.
        return circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }

    @Override
    public synchronized <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        if (requestHandlers.containsKey(reg.getAction())) {
            throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
        }
        requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
    }

    private static class HandshakeResponseHandler implements TransportResponseHandler<VersionHandshakeResponse> {
        final AtomicReference<Version> versionRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        final TcpChannel channel;

        HandshakeResponseHandler(TcpChannel channel) {
            this.channel = channel;
        }

        @Override
        public VersionHandshakeResponse newInstance() {
            return new VersionHandshakeResponse();
        }

        @Override
        public void handleResponse(VersionHandshakeResponse response) {
            final boolean success = versionRef.compareAndSet(null, response.version);
            latch.countDown();
            assert success;
        }

        @Override
        public void handleException(TransportException exp) {
            final boolean success = exceptionRef.compareAndSet(null, exp);
            latch.countDown();
            assert success;
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    public final class NodeChannels extends CloseableConnection {
        private final Map<TransportRequestOptions.Type, ConnectionProfile.ConnectionTypeHandle> typeMapping;
        private final List<TcpChannel> channels;
        private final DiscoveryNode node;
        private final Version version;
        private final AtomicBoolean isClosing = new AtomicBoolean(false);

        NodeChannels(DiscoveryNode node, List<TcpChannel> channels, ConnectionProfile connectionProfile, Version handshakeVersion) {
            this.node = node;
            this.channels = Collections.unmodifiableList(channels);
            assert channels.size() == connectionProfile.getNumConnections() : "expected channels size to be == "
                + connectionProfile.getNumConnections() + " but was: [" + channels.size() + "]";
            typeMapping = new EnumMap<>(TransportRequestOptions.Type.class);
            for (ConnectionProfile.ConnectionTypeHandle handle : connectionProfile.getHandles()) {
                for (TransportRequestOptions.Type type : handle.getTypes())
                    typeMapping.put(type, handle);
            }
            version = handshakeVersion;
        }

        @Override
        public Version getVersion() {
            return version;
        }

        public List<TcpChannel> getChannels() {
            return channels;
        }

        public TcpChannel channel(TransportRequestOptions.Type type) {
            ConnectionProfile.ConnectionTypeHandle connectionTypeHandle = typeMapping.get(type);
            if (connectionTypeHandle == null) {
                throw new IllegalArgumentException("no type channel for [" + type + "]");
            }
            return connectionTypeHandle.getChannel(channels);
        }

        @Override
        public boolean sendPing() {
            for (TcpChannel channel : channels) {
                internalSendMessage(channel, pingMessage, new SendMetricListener(pingMessage.length()) {
                    @Override
                    protected void innerInnerOnResponse(Void v) {
                        successfulPings.inc();
                    }

                    @Override
                    protected void innerOnFailure(Exception e) {
                        if (channel.isOpen()) {
                            logger.debug(() -> new ParameterizedMessage("[{}] failed to send ping transport message", node), e);
                            failedPings.inc();
                        } else {
                            logger.trace(() ->
                                new ParameterizedMessage("[{}] failed to send ping transport message (channel closed)", node), e);
                        }

                    }
                });
            }
            return true;
        }

        @Override
        public void close() {
            if (isClosing.compareAndSet(false, true)) {
                try {
                    if (lifecycle.stopped()) {
                        /* We set SO_LINGER timeout to 0 to ensure that when we shutdown the node we don't
                         * have a gazillion connections sitting in TIME_WAIT to free up resources quickly.
                         * This is really the only part where we close the connection from the server side
                         * otherwise the client (node) initiates the TCP closing sequence which doesn't cause
                         * these issues. Setting this by default from the beginning can have unexpected
                         * side-effects an should be avoided, our protocol is designed in a way that clients
                         * close connection which is how it should be*/

                        channels.forEach(c -> {
                            try {
                                c.setSoLinger(0);
                            } catch (IOException e) {
                                logger.warn(new ParameterizedMessage("unexpected exception when setting SO_LINGER on channel {}", c), e);
                            }
                        });
                    }

                    boolean block = lifecycle.stopped() && Transports.isTransportThread(Thread.currentThread()) == false;
                    CloseableChannel.closeChannels(channels, block);
                } finally {
                    // Call the super method to trigger listeners
                    super.close();
                }
            }
        }

        @Override
        public DiscoveryNode getNode() {
            return this.node;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            if (isClosing.get()) {
                throw new NodeNotConnectedException(node, "connection already closed");
            }
            TcpChannel channel = channel(options.type());
            sendRequestToChannel(this.node, channel, requestId, action, request, options, getVersion(), (byte) 0);
        }
    }

    // This allows transport implementations to potentially override specific connection profiles. This
    // primarily exists for the test implementations.
    protected ConnectionProfile maybeOverrideConnectionProfile(ConnectionProfile connectionProfile) {
        return connectionProfile;
    }

    @Override
    public NodeChannels openConnection(DiscoveryNode node, ConnectionProfile connectionProfile) {
        Objects.requireNonNull(connectionProfile, "connection profile cannot be null");
        if (node == null) {
            throw new ConnectTransportException(null, "can't open connection to a null node");
        }
        boolean success = false;
        NodeChannels nodeChannels = null;
        connectionProfile = maybeOverrideConnectionProfile(connectionProfile);
        closeLock.readLock().lock(); // ensure we don't open connections while we are closing
        try {
            ensureOpen();
            try {
                int numConnections = connectionProfile.getNumConnections();
                assert numConnections > 0 : "A connection profile must be configured with at least one connection";
                List<TcpChannel> channels = new ArrayList<>(numConnections);
                List<ActionFuture<Void>> connectionFutures = new ArrayList<>(numConnections);
                for (int i = 0; i < numConnections; ++i) {
                    try {
                        PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
                        connectionFutures.add(connectFuture);
                        TcpChannel channel = initiateChannel(node, connectFuture);
                        logger.trace(() -> new ParameterizedMessage("Tcp transport client channel opened: {}", channel));
                        channels.add(channel);
                    } catch (Exception e) {
                        // If there was an exception when attempting to instantiate the raw channels, we close all of the channels
                        CloseableChannel.closeChannels(channels, false);
                        throw e;
                    }
                }

                // If we make it past the block above, we successfully instantiated all of the channels
                try {
                    TcpChannel.awaitConnected(node, connectionFutures, connectionProfile.getConnectTimeout());
                } catch (Exception ex) {
                    CloseableChannel.closeChannels(channels, false);
                    throw ex;
                }

                // If we make it past the block above, we have successfully established connections for all of the channels
                final TcpChannel handshakeChannel = channels.get(0); // one channel is guaranteed by the connection profile
                handshakeChannel.addCloseListener(ActionListener.wrap(() -> cancelHandshakeForChannel(handshakeChannel)));
                Version version;
                try {
                    version = executeHandshake(node, handshakeChannel, connectionProfile.getHandshakeTimeout());
                } catch (Exception ex) {
                    CloseableChannel.closeChannels(channels, false);
                    throw ex;
                }

                // If we make it past the block above, we have successfully completed the handshake and the connection is now open.
                // At this point we should construct the connection, notify the transport service, and attach close listeners to the
                // underlying channels.
                nodeChannels = new NodeChannels(node, channels, connectionProfile, version);
                final NodeChannels finalNodeChannels = nodeChannels;

                Consumer<TcpChannel> onClose = c -> {
                    assert c.isOpen() == false : "channel is still open when onClose is called";
                    finalNodeChannels.close();
                };

                nodeChannels.channels.forEach(ch -> ch.addCloseListener(ActionListener.wrap(() -> onClose.accept(ch))));
                success = true;
                return nodeChannels;
            } catch (ConnectTransportException e) {
                throw e;
            } catch (Exception e) {
                // ConnectTransportExceptions are handled specifically on the caller end - we wrap the actual exception to ensure
                // only relevant exceptions are logged on the caller end.. this is the same as in connectToNode
                throw new ConnectTransportException(node, "general node connection failure", e);
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(nodeChannels);
                }
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    protected Version getCurrentVersion() {
        // this is just for tests to mock stuff like the nodes version - tests can override this internally
        return Version.CURRENT;
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return unmodifiableMap(new HashMap<>(profileBoundAddresses));
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

    protected void bindServer(ProfileSettings profileSettings) {
        // Bind and start to accept incoming connections.
        InetAddress hostAddresses[];
        List<String> profileBindHosts = profileSettings.bindHosts;
        try {
            hostAddresses = networkService.resolveBindHostAddresses(profileBindHosts.toArray(Strings.EMPTY_ARRAY));
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host " + profileBindHosts, e);
        }
        if (logger.isDebugEnabled()) {
            String[] addresses = new String[hostAddresses.length];
            for (int i = 0; i < hostAddresses.length; i++) {
                addresses[i] = NetworkAddress.format(hostAddresses[i]);
            }
            logger.debug("binding server bootstrap to: {}", (Object) addresses);
        }

        assert hostAddresses.length > 0;

        List<InetSocketAddress> boundAddresses = new ArrayList<>();
        for (InetAddress hostAddress : hostAddresses) {
            boundAddresses.add(bindToPort(profileSettings.profileName, hostAddress, profileSettings.portOrRange));
        }

        final BoundTransportAddress boundTransportAddress = createBoundTransportAddress(profileSettings, boundAddresses);

        if (profileSettings.isDefaultProfile) {
            this.boundAddress = boundTransportAddress;
        } else {
            profileBoundAddresses.put(profileSettings.profileName, boundTransportAddress);
        }
    }

    protected InetSocketAddress bindToPort(final String name, final InetAddress hostAddress, String port) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = portsRange.iterate(portNumber -> {
            try {
                TcpServerChannel channel = bind(name, new InetSocketAddress(hostAddress, portNumber));
                synchronized (serverChannels) {
                    List<TcpServerChannel> list = serverChannels.get(name);
                    if (list == null) {
                        list = new ArrayList<>();
                        serverChannels.put(name, list);
                    }
                    list.add(channel);
                    boundSocket.set(channel.getLocalAddress());
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

    private BoundTransportAddress createBoundTransportAddress(ProfileSettings profileSettings,
                                                              List<InetSocketAddress> boundAddresses) {
        String[] boundAddressesHostStrings = new String[boundAddresses.size()];
        TransportAddress[] transportBoundAddresses = new TransportAddress[boundAddresses.size()];
        for (int i = 0; i < boundAddresses.size(); i++) {
            InetSocketAddress boundAddress = boundAddresses.get(i);
            boundAddressesHostStrings[i] = boundAddress.getHostString();
            transportBoundAddresses[i] = new TransportAddress(boundAddress);
        }

        List<String> publishHosts = profileSettings.publishHosts;
        if (profileSettings.isDefaultProfile == false && publishHosts.isEmpty()) {
            publishHosts = Arrays.asList(boundAddressesHostStrings);
        }
        if (publishHosts.isEmpty()) {
            publishHosts = NetworkService.GLOBAL_NETWORK_PUBLISHHOST_SETTING.get(settings);
        }

        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts.toArray(Strings.EMPTY_ARRAY));
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        final int publishPort = resolvePublishPort(profileSettings, boundAddresses, publishInetAddress);
        final TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        return new BoundTransportAddress(transportBoundAddresses, publishAddress);
    }

    // package private for tests
    public static int resolvePublishPort(ProfileSettings profileSettings, List<InetSocketAddress> boundAddresses,
                                         InetAddress publishInetAddress) {
        int publishPort = profileSettings.publishPort;

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
            String profileExplanation = profileSettings.isDefaultProfile ? "" : " for profile " + profileSettings.profileName;
            throw new BindTransportException("Failed to auto-resolve publish port" + profileExplanation + ", multiple bound addresses " +
                boundAddresses + " with distinct ports and none of them matched the publish address (" + publishInetAddress + "). " +
                "Please specify a unique port by setting " + PORT.getKey() + " or " +
                PUBLISH_PORT.getKey());
        }
        return publishPort;
    }

    @Override
    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
        return parse(address, settings.get("transport.profiles.default.port", PORT.get(settings)), perAddressLimit);
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
                transportAddresses.add(new TransportAddress(address, ports[i]));
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
            closeLock.writeLock().lock();
            try {
                // first stop to accept any incoming connections so nobody can connect to this transport
                for (Map.Entry<String, List<TcpServerChannel>> entry : serverChannels.entrySet()) {
                    String profile = entry.getKey();
                    List<TcpServerChannel> channels = entry.getValue();
                    ActionListener<Void> closeFailLogger = ActionListener.wrap(c -> {},
                        e -> logger.warn(() -> new ParameterizedMessage("Error closing serverChannel for profile [{}]", profile), e));
                    channels.forEach(c -> c.addCloseListener(closeFailLogger));
                    CloseableChannel.closeChannels(channels, true);
                }
                serverChannels.clear();

                // close all of the incoming channels. The closeChannels method takes a list so we must convert the set.
                CloseableChannel.closeChannels(new ArrayList<>(acceptedChannels), true);
                acceptedChannels.clear();

                stopInternal();
            } finally {
                closeLock.writeLock().unlock();
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

    public void onException(TcpChannel channel, Exception e) {
        if (!lifecycle.started()) {
            // just close and ignore - we are already stopped and just need to make sure we release all resources
            CloseableChannel.closeChannel(channel);
            return;
        }

        if (isCloseConnectionException(e)) {
            logger.trace(() -> new ParameterizedMessage(
                    "close connection exception caught on transport layer [{}], disconnecting from relevant node", channel), e);
            // close the channel, which will cause a node to be disconnected if relevant
            CloseableChannel.closeChannel(channel);
        } else if (isConnectException(e)) {
            logger.trace(() -> new ParameterizedMessage("connect exception caught on transport layer [{}]", channel), e);
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            CloseableChannel.closeChannel(channel);
        } else if (e instanceof BindException) {
            logger.trace(() -> new ParameterizedMessage("bind exception caught on transport layer [{}]", channel), e);
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            CloseableChannel.closeChannel(channel);
        } else if (e instanceof CancelledKeyException) {
            logger.trace(() -> new ParameterizedMessage(
                    "cancelled key exception caught on transport layer [{}], disconnecting from relevant node", channel), e);
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            CloseableChannel.closeChannel(channel);
        } else if (e instanceof TcpTransport.HttpOnTransportException) {
            // in case we are able to return data, serialize the exception content and sent it back to the client
            if (channel.isOpen()) {
                BytesArray message = new BytesArray(e.getMessage().getBytes(StandardCharsets.UTF_8));
                final SendMetricListener closeChannel = new SendMetricListener(message.length()) {
                    @Override
                    protected void innerInnerOnResponse(Void v) {
                        CloseableChannel.closeChannel(channel);
                    }

                    @Override
                    protected void innerOnFailure(Exception e) {
                        logger.debug("failed to send message to httpOnTransport channel", e);
                        CloseableChannel.closeChannel(channel);
                    }
                };
                internalSendMessage(channel, message, closeChannel);
            }
        } else {
            logger.warn(() -> new ParameterizedMessage("exception caught on transport layer [{}], closing connection", channel), e);
            // close the channel, which will cause a node to be disconnected if relevant
            CloseableChannel.closeChannel(channel);
        }
    }

    protected void onServerException(TcpServerChannel channel, Exception e) {
        logger.error(new ParameterizedMessage("exception from server channel caught on transport layer [channel={}]", channel), e);
    }

    /**
     * Exception handler for exceptions that are not associated with a specific channel.
     *
     * @param exception the exception
     */
    protected void onNonChannelException(Exception exception) {
        logger.warn(new ParameterizedMessage("exception caught on transport layer [thread={}]", Thread.currentThread().getName()),
            exception);
    }

    protected void serverAcceptedChannel(TcpChannel channel) {
        boolean addedOnThisCall = acceptedChannels.add(channel);
        assert addedOnThisCall : "Channel should only be added to accepted channel set once";
        channel.addCloseListener(ActionListener.wrap(() -> acceptedChannels.remove(channel)));
        logger.trace(() -> new ParameterizedMessage("Tcp transport channel accepted: {}", channel));
    }

    /**
     * Binds to the given {@link InetSocketAddress}
     *
     * @param name    the profile name
     * @param address the address to bind to
     */
    protected abstract TcpServerChannel bind(String name, InetSocketAddress address) throws IOException;

    /**
     * Initiate a single tcp socket channel.
     *
     * @param node for the initiated connection
     * @param connectListener listener to be called when connection complete
     * @return the pending connection
     * @throws IOException if an I/O exception occurs while opening the channel
     */
    protected abstract TcpChannel initiateChannel(DiscoveryNode node, ActionListener<Void> connectListener) throws IOException;

    /**
     * Called to tear down internal resources
     */
    protected abstract void stopInternal();

    public boolean canCompress(TransportRequest request) {
        return compress && (!(request instanceof BytesTransportRequest));
    }

    private void sendRequestToChannel(final DiscoveryNode node, final TcpChannel channel, final long requestId, final String action,
                                      final TransportRequest request, TransportRequestOptions options, Version channelVersion,
                                      byte status) throws IOException, TransportException {
        if (compress) {
            options = TransportRequestOptions.builder(options).withCompress(true).build();
        }

        // only compress if asked and the request is not bytes. Otherwise only
        // the header part is compressed, and the "body" can't be extracted as compressed
        final boolean compressMessage = options.compress() && canCompress(request);

        status = TransportStatus.setRequest(status);
        ReleasableBytesStreamOutput bStream = new ReleasableBytesStreamOutput(bigArrays);
        final CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, compressMessage);
        boolean addedReleaseListener = false;
        try {
            if (compressMessage) {
                status = TransportStatus.setCompress(status);
            }

            // we pick the smallest of the 2, to support both backward and forward compatibility
            // note, this is the only place we need to do this, since from here on, we use the serialized version
            // as the version to use also when the node receiving this request will send the response with
            Version version = Version.min(getCurrentVersion(), channelVersion);

            stream.setVersion(version);
            threadPool.getThreadContext().writeTo(stream);
            if (version.onOrAfter(Version.V_6_3_0)) {
                stream.writeStringArray(features);
            }
            stream.writeString(action);
            BytesReference message = buildMessage(requestId, status, node.getVersion(), request, stream);
            final TransportRequestOptions finalOptions = options;
            // this might be called in a different thread
            SendListener onRequestSent = new SendListener(channel, stream,
                () -> messageListener.onRequestSent(node, requestId, action, request, finalOptions), message.length());
            internalSendMessage(channel, message, onRequestSent);
            addedReleaseListener = true;
        } finally {
            if (!addedReleaseListener) {
                IOUtils.close(stream);
            }
        }
    }

    /**
     * sends a message to the given channel, using the given callbacks.
     */
    private void internalSendMessage(TcpChannel channel, BytesReference message, SendMetricListener listener) {
        try {
            channel.sendMessage(message, listener);
        } catch (Exception ex) {
            // call listener to ensure that any resources are released
            listener.onFailure(ex);
            onException(channel, ex);
        }
    }

    /**
     * Sends back an error response to the caller via the given channel
     *
     * @param nodeVersion the caller node version
     * @param features    the caller features
     * @param channel     the channel to send the response to
     * @param error       the error to return
     * @param requestId   the request ID this response replies to
     * @param action      the action this response replies to
     */
    public void sendErrorResponse(
            final Version nodeVersion,
            final Set<String> features,
            final TcpChannel channel,
            final Exception error,
            final long requestId,
            final String action) throws IOException {
        try (BytesStreamOutput stream = new BytesStreamOutput()) {
            stream.setVersion(nodeVersion);
            stream.setFeatures(features);
            RemoteTransportException tx = new RemoteTransportException(
                nodeName(), new TransportAddress(channel.getLocalAddress()), action, error);
            threadPool.getThreadContext().writeTo(stream);
            stream.writeException(tx);
            byte status = 0;
            status = TransportStatus.setResponse(status);
            status = TransportStatus.setError(status);
            final BytesReference bytes = stream.bytes();
            final BytesReference header = buildHeader(requestId, status, nodeVersion, bytes.length());
            CompositeBytesReference message = new CompositeBytesReference(header, bytes);
            SendListener onResponseSent = new SendListener(channel, null,
                () -> messageListener.onResponseSent(requestId, action, error), message.length());
            internalSendMessage(channel, message, onResponseSent);
        }
    }

    /**
     * Sends the response to the given channel. This method should be used to send {@link TransportResponse} objects back to the caller.
     *
     * @see #sendErrorResponse(Version, Set, TcpChannel, Exception, long, String) for sending back errors to the caller
     */
    public void sendResponse(
            final Version nodeVersion,
            final Set<String> features,
            final TcpChannel channel,
            final TransportResponse response,
            final long requestId,
            final String action,
            final TransportResponseOptions options) throws IOException {
        sendResponse(nodeVersion, features, channel, response, requestId, action, options, (byte) 0);
    }

    private void sendResponse(
            final Version nodeVersion,
            final Set<String> features,
            final TcpChannel channel,
            final TransportResponse response,
            final long requestId,
            final String action,
            TransportResponseOptions options,
            byte status) throws IOException {
        if (compress) {
            options = TransportResponseOptions.builder(options).withCompress(true).build();
        }
        status = TransportStatus.setResponse(status); // TODO share some code with sendRequest
        ReleasableBytesStreamOutput bStream = new ReleasableBytesStreamOutput(bigArrays);
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, options.compress());
        boolean addedReleaseListener = false;
        try {
            if (options.compress()) {
                status = TransportStatus.setCompress(status);
            }
            threadPool.getThreadContext().writeTo(stream);
            stream.setVersion(nodeVersion);
            stream.setFeatures(features);
            BytesReference message = buildMessage(requestId, status, nodeVersion, response, stream);

            final TransportResponseOptions finalOptions = options;
            // this might be called in a different thread
            SendListener listener = new SendListener(channel, stream,
                () -> messageListener.onResponseSent(requestId, action, response, finalOptions), message.length());
            internalSendMessage(channel, message, listener);
            addedReleaseListener = true;
        } finally {
            if (!addedReleaseListener) {
                IOUtils.close(stream);
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
    private BytesReference buildMessage(long requestId, byte status, Version nodeVersion, TransportMessage message,
                                        CompressibleBytesOutputStream stream) throws IOException {
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
        // we have to call materializeBytes() here before accessing the bytes. A CompressibleBytesOutputStream
        // might be implementing compression. And materializeBytes() ensures that some marker bytes (EOS marker)
        // are written. Otherwise we barf on the decompressing end when we read past EOF on purpose in the
        // #validateRequest method. this might be a problem in deflate after all but it's important to write
        // the marker bytes.
        final BytesReference messageBody = stream.materializeBytes();
        final BytesReference header = buildHeader(requestId, status, stream.getVersion(), messageBody.length() + zeroCopyBuffer.length());
        return new CompositeBytesReference(header, messageBody, zeroCopyBuffer);
    }

    /**
     * Consumes bytes that are available from network reads. This method returns the number of bytes consumed
     * in this call.
     *
     * @param channel the channel read from
     * @param bytesReference the bytes available to consume
     * @return the number of bytes consumed
     * @throws StreamCorruptedException if the message header format is not recognized
     * @throws TcpTransport.HttpOnTransportException if the message header appears to be a HTTP message
     * @throws IllegalArgumentException if the message length is greater that the maximum allowed frame size.
     *                                  This is dependent on the available memory.
     */
    public int consumeNetworkReads(TcpChannel channel, BytesReference bytesReference) throws IOException {
        BytesReference message = decodeFrame(bytesReference);

        if (message == null) {
            return 0;
        } else if (message.length() == 0) {
            // This is a ping and should not be handled.
            return BYTES_NEEDED_FOR_MESSAGE_SIZE;
        } else {
            try {
                messageReceived(message, channel);
            } catch (Exception e) {
                onException(channel, e);
            }
            return message.length() + BYTES_NEEDED_FOR_MESSAGE_SIZE;
        }
    }

    /**
     * Attempts to a decode a message from the provided bytes. If a full message is not available, null is
     * returned. If the message is a ping, an empty {@link BytesReference} will be returned.
     *
     * @param networkBytes the will be read
     * @return the message decoded
     * @throws StreamCorruptedException if the message header format is not recognized
     * @throws TcpTransport.HttpOnTransportException if the message header appears to be a HTTP message
     * @throws IllegalArgumentException if the message length is greater that the maximum allowed frame size.
     *                                  This is dependent on the available memory.
     */
    public static BytesReference decodeFrame(BytesReference networkBytes) throws IOException {
        int messageLength = readMessageLength(networkBytes);
        if (messageLength == -1) {
            return null;
        } else {
            int totalLength = messageLength + BYTES_NEEDED_FOR_MESSAGE_SIZE;
            if (totalLength > networkBytes.length()) {
                return null;
            } else if (totalLength == 6) {
                return EMPTY_BYTES_REFERENCE;
            } else {
                return networkBytes.slice(BYTES_NEEDED_FOR_MESSAGE_SIZE, messageLength);
            }
        }
    }

    /**
     * Validates the first 6 bytes of the message header and returns the length of the message. If 6 bytes
     * are not available, it returns -1.
     *
     * @param networkBytes the will be read
     * @return the length of the message
     * @throws StreamCorruptedException if the message header format is not recognized
     * @throws TcpTransport.HttpOnTransportException if the message header appears to be a HTTP message
     * @throws IllegalArgumentException if the message length is greater that the maximum allowed frame size.
     *                                  This is dependent on the available memory.
     */
    public static int readMessageLength(BytesReference networkBytes) throws IOException {
        if (networkBytes.length() < BYTES_NEEDED_FOR_MESSAGE_SIZE) {
            return -1;
        } else {
            return readHeaderBuffer(networkBytes);
        }
    }

    private static int readHeaderBuffer(BytesReference headerBuffer) throws IOException {
        if (headerBuffer.get(0) != 'E' || headerBuffer.get(1) != 'S') {
            if (appearsToBeHTTP(headerBuffer)) {
                throw new TcpTransport.HttpOnTransportException("This is not a HTTP port");
            }

            throw new StreamCorruptedException("invalid internal transport message format, got ("
                + Integer.toHexString(headerBuffer.get(0) & 0xFF) + ","
                + Integer.toHexString(headerBuffer.get(1) & 0xFF) + ","
                + Integer.toHexString(headerBuffer.get(2) & 0xFF) + ","
                + Integer.toHexString(headerBuffer.get(3) & 0xFF) + ")");
        }
        final int messageLength;
        try (StreamInput input = headerBuffer.streamInput()) {
            input.skip(TcpHeader.MARKER_BYTES_SIZE);
            messageLength = input.readInt();
        }

        if (messageLength == TcpTransport.PING_DATA_SIZE) {
            // This is a ping
            return 0;
        }

        if (messageLength <= 0) {
            throw new StreamCorruptedException("invalid data length: " + messageLength);
        }

        if (messageLength > NINETY_PER_HEAP_SIZE) {
            throw new IllegalArgumentException("transport content length received [" + new ByteSizeValue(messageLength) + "] exceeded ["
                + new ByteSizeValue(NINETY_PER_HEAP_SIZE) + "]");
        }

        return messageLength;
    }

    private static boolean appearsToBeHTTP(BytesReference headerBuffer) {
        return bufferStartsWith(headerBuffer, "GET") ||
            bufferStartsWith(headerBuffer, "POST") ||
            bufferStartsWith(headerBuffer, "PUT") ||
            bufferStartsWith(headerBuffer, "HEAD") ||
            bufferStartsWith(headerBuffer, "DELETE") ||
            // Actually 'OPTIONS'. But we are only guaranteed to have read six bytes at this point.
            bufferStartsWith(headerBuffer, "OPTION") ||
            bufferStartsWith(headerBuffer, "PATCH") ||
            bufferStartsWith(headerBuffer, "TRACE");
    }

    private static boolean bufferStartsWith(BytesReference buffer, String method) {
        char[] chars = method.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (buffer.get(i) != chars[i]) {
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

        public HttpOnTransportException(StreamInput in) throws IOException {
            super(in);
        }
    }

    /**
     * This method handles the message receive part for both request and responses
     */
    public final void messageReceived(BytesReference reference, TcpChannel channel) throws IOException {
        String profileName = channel.getProfile();
        InetSocketAddress remoteAddress = channel.getRemoteAddress();
        int messageLengthBytes = reference.length();
        final int totalMessageSize = messageLengthBytes + TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
        readBytesMetric.inc(totalMessageSize);
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
            final boolean isHandshake = TransportStatus.isHandshake(status);
            ensureVersionCompatibility(version, getCurrentVersion(), isHandshake);
            streamIn = new NamedWriteableAwareStreamInput(streamIn, namedWriteableRegistry);
            streamIn.setVersion(version);
            threadPool.getThreadContext().readHeaders(streamIn);
            threadPool.getThreadContext().putTransient("_remote_address", remoteAddress);
            if (TransportStatus.isRequest(status)) {
                handleRequest(channel, profileName, streamIn, requestId, messageLengthBytes, version, remoteAddress, status);
            } else {
                final TransportResponseHandler<?> handler;
                if (isHandshake) {
                    handler = pendingHandshakes.remove(requestId);
                } else {
                    TransportResponseHandler theHandler = responseHandlers.onResponseReceived(requestId, messageListener);
                    if (theHandler == null && TransportStatus.isError(status)) {
                        handler = pendingHandshakes.remove(requestId);
                    } else {
                        handler = theHandler;
                    }
                }
                // ignore if its null, the service logs it
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

    static void ensureVersionCompatibility(Version version, Version currentVersion, boolean isHandshake) {
        // for handshakes we are compatible with N-2 since otherwise we can't figure out our initial version
        // since we are compatible with N-1 and N+1 so we always send our minCompatVersion as the initial version in the
        // handshake. This looks odd but it's required to establish the connection correctly we check for real compatibility
        // once the connection is established
        final Version compatibilityVersion = isHandshake ? currentVersion.minimumCompatibilityVersion() : currentVersion;
        if (version.isCompatible(compatibilityVersion) == false) {
            final Version minCompatibilityVersion = isHandshake ? compatibilityVersion : compatibilityVersion.minimumCompatibilityVersion();
            String msg = "Received " + (isHandshake ? "handshake " : "") + "message from unsupported version: [";
            throw new IllegalStateException(msg + version + "] minimal compatible version is: [" + minCompatibilityVersion + "]");
        }
    }

    private void handleResponse(InetSocketAddress remoteAddress, final StreamInput stream, final TransportResponseHandler handler) {
        final TransportResponse response;
        try {
            response = handler.read(stream);
            response.remoteAddress(new TransportAddress(remoteAddress));
        } catch (Exception e) {
            handleException(handler, new TransportSerializationException(
                "Failed to deserialize response from handler [" + handler.getClass().getName() + "]", e));
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
            }
        });

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
                logger.error(() -> new ParameterizedMessage("failed to handle exception response [{}]", handler), e);
            }
        });
    }

    protected String handleRequest(TcpChannel channel, String profileName, final StreamInput stream, long requestId,
                                   int messageLengthBytes, Version version, InetSocketAddress remoteAddress, byte status)
        throws IOException {
        final Set<String> features;
        if (version.onOrAfter(Version.V_6_3_0)) {
            features = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(stream.readStringArray())));
        } else {
            features = Collections.emptySet();
        }
        final String action = stream.readString();
        messageListener.onRequestReceived(requestId, action);
        TransportChannel transportChannel = null;
        try {
            if (TransportStatus.isHandshake(status)) {
                final VersionHandshakeResponse response = new VersionHandshakeResponse(getCurrentVersion());
                sendResponse(version, features, channel, response, requestId, HANDSHAKE_ACTION_NAME, TransportResponseOptions.EMPTY,
                    TransportStatus.setHandshake((byte) 0));
            } else {
                final RequestHandlerRegistry reg = getRequestHandler(action);
                if (reg == null) {
                    throw new ActionNotFoundTransportException(action);
                }
                if (reg.canTripCircuitBreaker()) {
                    getInFlightRequestBreaker().addEstimateBytesAndMaybeBreak(messageLengthBytes, "<transport_request>");
                } else {
                    getInFlightRequestBreaker().addWithoutBreaking(messageLengthBytes);
                }
                transportChannel = new TcpTransportChannel(this, channel, transportName, action, requestId, version, features, profileName,
                    messageLengthBytes);
                final TransportRequest request = reg.newRequest(stream);
                request.remoteAddress(new TransportAddress(remoteAddress));
                // in case we throw an exception, i.e. when the limit is hit, we don't want to verify
                validateRequest(stream, requestId, action);
                threadPool.executor(reg.getExecutor()).execute(new RequestHandler(reg, request, transportChannel));
            }
        } catch (Exception e) {
            // the circuit breaker tripped
            if (transportChannel == null) {
                transportChannel =
                        new TcpTransportChannel(this, channel, transportName, action, requestId, version, features, profileName, 0);
            }
            try {
                transportChannel.sendResponse(e);
            } catch (IOException inner) {
                inner.addSuppressed(e);
                logger.warn(() -> new ParameterizedMessage("Failed to send error message back to client for action [{}]", action), inner);
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

        RequestHandler(RequestHandlerRegistry reg, TransportRequest request, TransportChannel transportChannel) {
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
                    logger.warn(() -> new ParameterizedMessage(
                            "Failed to send error message back to client for action [{}]", reg.getAction()), inner);
                }
            }
        }
    }

    private static final class VersionHandshakeResponse extends TransportResponse {
        private Version version;

        private VersionHandshakeResponse(Version version) {
            this.version = version;
        }

        private VersionHandshakeResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            version = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            assert version != null;
            Version.writeVersion(version, out);
        }
    }

    protected Version executeHandshake(DiscoveryNode node, TcpChannel channel, TimeValue timeout)
        throws IOException, InterruptedException {
        numHandshakes.inc();
        final long requestId = responseHandlers.newRequestId();
        final HandshakeResponseHandler handler = new HandshakeResponseHandler(channel);
        AtomicReference<Version> versionRef = handler.versionRef;
        AtomicReference<Exception> exceptionRef = handler.exceptionRef;
        pendingHandshakes.put(requestId, handler);
        boolean success = false;
        try {
            if (channel.isOpen() == false) {
                // we have to protect us here since sendRequestToChannel won't barf if the channel is closed.
                // it's weird but to change it will cause a lot of impact on the exception handling code all over the codebase.
                // yet, if we don't check the state here we might have registered a pending handshake handler but the close
                // listener calling #onChannelClosed might have already run and we are waiting on the latch below unitl we time out.
                throw new IllegalStateException("handshake failed, channel already closed");
            }
            // for the request we use the minCompatVersion since we don't know what's the version of the node we talk to
            // we also have no payload on the request but the response will contain the actual version of the node we talk
            // to as the payload.
            final Version minCompatVersion = getCurrentVersion().minimumCompatibilityVersion();
            sendRequestToChannel(node, channel, requestId, HANDSHAKE_ACTION_NAME, TransportRequest.Empty.INSTANCE,
                TransportRequestOptions.EMPTY, minCompatVersion, TransportStatus.setHandshake((byte) 0));
            if (handler.latch.await(timeout.millis(), TimeUnit.MILLISECONDS) == false) {
                throw new ConnectTransportException(node, "handshake_timeout[" + timeout + "]");
            }
            success = true;
            if (exceptionRef.get() != null) {
                throw new IllegalStateException("handshake failed", exceptionRef.get());
            } else {
                Version version = versionRef.get();
                if (getCurrentVersion().isCompatible(version) == false) {
                    throw new IllegalStateException("Received message from unsupported version: [" + version
                        + "] minimal compatible version is: [" + getCurrentVersion().minimumCompatibilityVersion() + "]");
                }
                return version;
            }
        } finally {
            final TransportResponseHandler<?> removedHandler = pendingHandshakes.remove(requestId);
            // in the case of a timeout or an exception on the send part the handshake has not been removed yet.
            // but the timeout is tricky since it's basically a race condition so we only assert on the success case.
            assert success && removedHandler == null || success == false : "handler for requestId [" + requestId + "] is not been removed";
        }
    }

    final int getNumPendingHandshakes() { // for testing
        return pendingHandshakes.size();
    }

    final long getNumHandshakes() {
        return numHandshakes.count(); // for testing
    }

    /**
     * Called once the channel is closed for instance due to a disconnect or a closed socket etc.
     */
    private void cancelHandshakeForChannel(TcpChannel channel) {
        final Optional<Long> first = pendingHandshakes.entrySet().stream()
            .filter((entry) -> entry.getValue().channel == channel).map(Map.Entry::getKey).findFirst();
        if (first.isPresent()) {
            final Long requestId = first.get();
            final HandshakeResponseHandler handler = pendingHandshakes.remove(requestId);
            if (handler != null) {
                // there might be a race removing this or this method might be called twice concurrently depending on how
                // the channel is closed ie. due to connection reset or broken pipes
                handler.handleException(new TransportException("connection reset"));
            }
        }
    }

    /**
     * Ensures this transport is still started / open
     *
     * @throws IllegalStateException if the transport is not started / open
     */
    protected final void ensureOpen() {
        if (lifecycle.started() == false) {
            throw new IllegalStateException("transport has been stopped");
        }
    }

    /**
     * This listener increments the transmitted bytes metric on success.
     */
    private abstract class SendMetricListener extends NotifyOnceListener<Void> {
        private final long messageSize;

        private SendMetricListener(long messageSize) {
            this.messageSize = messageSize;
        }

        @Override
        protected final void innerOnResponse(Void object) {
            transmittedBytesMetric.inc(messageSize);
            innerInnerOnResponse(object);
        }

        protected abstract void innerInnerOnResponse(Void object);
    }

    private final class SendListener extends SendMetricListener {
        private final TcpChannel channel;
        private final Closeable optionalCloseable;
        private final Runnable transportAdaptorCallback;

        private SendListener(TcpChannel channel, Closeable optionalCloseable, Runnable transportAdaptorCallback, long messageLength) {
            super(messageLength);
            this.channel = channel;
            this.optionalCloseable = optionalCloseable;
            this.transportAdaptorCallback = transportAdaptorCallback;
        }

        @Override
        protected void innerInnerOnResponse(Void v) {
            closeAndCallback(null);
        }

        @Override
        protected void innerOnFailure(Exception e) {
            logger.warn(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            closeAndCallback(e);
        }

        private void closeAndCallback(final Exception e) {
            try {
                IOUtils.close(optionalCloseable, transportAdaptorCallback::run);
            } catch (final IOException inner) {
                if (e != null) {
                    inner.addSuppressed(e);
                }
                throw new UncheckedIOException(inner);
            }
        }
    }

    @Override
    public final TransportStats getStats() {
        return new TransportStats(acceptedChannels.size(), readBytesMetric.count(), readBytesMetric.sum(), transmittedBytesMetric.count(),
            transmittedBytesMetric.sum());
    }

    /**
     * Returns all profile settings for the given settings object
     */
    public static Set<ProfileSettings> getProfileSettings(Settings settings) {
        HashSet<ProfileSettings> profiles = new HashSet<>();
        boolean isDefaultSet = false;
        for (String profile : settings.getGroups("transport.profiles.", true).keySet()) {
            profiles.add(new ProfileSettings(settings, profile));
            if (DEFAULT_PROFILE.equals(profile)) {
                isDefaultSet = true;
            }
        }
        if (isDefaultSet == false) {
            profiles.add(new ProfileSettings(settings, DEFAULT_PROFILE));
        }
        return Collections.unmodifiableSet(profiles);
    }

    /**
     * Representation of a transport profile settings for a {@code transport.profiles.$profilename.*}
     */
    public static final class ProfileSettings {
        public final String profileName;
        public final boolean tcpNoDelay;
        public final boolean tcpKeepAlive;
        public final boolean reuseAddress;
        public final ByteSizeValue sendBufferSize;
        public final ByteSizeValue receiveBufferSize;
        public final List<String> bindHosts;
        public final List<String> publishHosts;
        public final String portOrRange;
        public final int publishPort;
        public final boolean isDefaultProfile;

        public ProfileSettings(Settings settings, String profileName) {
            this.profileName = profileName;
            isDefaultProfile = DEFAULT_PROFILE.equals(profileName);
            tcpKeepAlive = TCP_KEEP_ALIVE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            tcpNoDelay = TCP_NO_DELAY_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            reuseAddress = TCP_REUSE_ADDRESS_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            sendBufferSize = TCP_SEND_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            receiveBufferSize = TCP_RECEIVE_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            List<String> profileBindHosts = BIND_HOST_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            bindHosts = (profileBindHosts.isEmpty() ? NetworkService.GLOBAL_NETWORK_BINDHOST_SETTING.get(settings)
                : profileBindHosts);
            publishHosts = PUBLISH_HOST_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            Setting<String> concretePort = PORT_PROFILE.getConcreteSettingForNamespace(profileName);
            if (concretePort.exists(settings) == false && isDefaultProfile == false) {
                throw new IllegalStateException("profile [" + profileName + "] has no port configured");
            }
            portOrRange = PORT_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            publishPort = isDefaultProfile ? PUBLISH_PORT.get(settings) :
                PUBLISH_PORT_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
        }
    }

    private static final class DelegatingTransportMessageListener implements TransportMessageListener {

        private final List<TransportMessageListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onRequestReceived(long requestId, String action) {
            for (TransportMessageListener listener : listeners) {
                listener.onRequestReceived(requestId, action);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, TransportResponse response, TransportResponseOptions finalOptions) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseSent(requestId, action, response, finalOptions);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, Exception error) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseSent(requestId, action, error);
            }
        }

        @Override
        public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                                  TransportRequestOptions finalOptions) {
            for (TransportMessageListener listener : listeners) {
                listener.onRequestSent(node, requestId, action, request, finalOptions);
            }
        }

        @Override
        public void onResponseReceived(long requestId, ResponseContext holder) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseReceived(requestId, holder);
            }
        }
    }

    @Override
    public final ResponseHandlers getResponseHandlers() {
        return responseHandlers;
    }

    @Override
    public final RequestHandlerRegistry getRequestHandler(String action) {
        return requestHandlers.get(action);
    }
}
