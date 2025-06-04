/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isConnectException;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;

public abstract class TcpTransport extends AbstractLifecycleComponent implements Transport {
    private static final Logger logger = LogManager.getLogger(TcpTransport.class);

    public static final String TRANSPORT_WORKER_THREAD_NAME_PREFIX = "transport_worker";

    // This is the number of bytes necessary to read the message size
    private static final int BYTES_NEEDED_FOR_MESSAGE_SIZE = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
    private static final long THIRTY_PER_HEAP_SIZE = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.3);

    final StatsTracker statsTracker = new StatsTracker();

    // this limit is per-address
    private static final int LIMIT_LOCAL_PORTS_COUNT = 6;

    // Deliberately unregistered, only used in unit tests. Copied to AbstractSimpleTransportTestCase#IGNORE_DESERIALIZATION_ERRORS_SETTING
    // so that tests in other packages can see it too.
    static final Setting<Boolean> IGNORE_DESERIALIZATION_ERRORS_SETTING = Setting.boolSetting(
        "transport.ignore_deserialization_errors",
        false,
        Setting.Property.NodeScope
    );

    private final boolean ignoreDeserializationErrors;

    protected final Settings settings;
    protected final ThreadPool threadPool;
    protected final Recycler<BytesRef> recycler;
    protected final NetworkService networkService;
    protected final Set<ProfileSettings> profileSettingsSet;
    protected final boolean rstOnClose;
    private final CircuitBreakerService circuitBreakerService;

    private final ConcurrentMap<String, BoundTransportAddress> profileBoundAddresses = newConcurrentMap();
    private final Map<String, List<TcpServerChannel>> serverChannels = newConcurrentMap();
    private final Set<TcpChannel> acceptedChannels = ConcurrentCollections.newConcurrentSet();

    // this lock is here to make sure we close this transport and disconnect all the client nodes
    // connections while no connect operations is going on
    private final ReadWriteLock closeLock = new ReentrantReadWriteLock();
    private volatile BoundTransportAddress boundAddress;

    private final TransportHandshaker handshaker;
    private final TransportKeepAlive keepAlive;
    private final HandlingTimeTracker outboundHandlingTimeTracker = new HandlingTimeTracker();
    private final OutboundHandler outboundHandler;
    private final InboundHandler inboundHandler;
    private final ResponseHandlers responseHandlers = new ResponseHandlers();
    private final RequestHandlers requestHandlers = new RequestHandlers();

    private final AtomicLong outboundConnectionCount = new AtomicLong(); // also used as a correlation ID for open/close logs

    @SuppressWarnings("this-escape")
    public TcpTransport(
        Settings settings,
        TransportVersion version,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService
    ) {
        this.settings = settings;
        this.profileSettingsSet = getProfileSettings(settings);
        this.threadPool = threadPool;
        this.circuitBreakerService = circuitBreakerService;
        this.networkService = networkService;
        String nodeName = Node.NODE_NAME_SETTING.get(settings);

        this.rstOnClose = TransportSettings.RST_ON_CLOSE.get(settings);

        this.recycler = createRecycler(settings, pageCacheRecycler);
        this.outboundHandler = new OutboundHandler(
            nodeName,
            version,
            statsTracker,
            threadPool,
            recycler,
            outboundHandlingTimeTracker,
            rstOnClose
        );

        ignoreDeserializationErrors = IGNORE_DESERIALIZATION_ERRORS_SETTING.get(settings);

        this.handshaker = new TransportHandshaker(
            version,
            threadPool,
            (node, channel, requestId, v) -> outboundHandler.sendRequest(
                node,
                channel,
                requestId,
                TransportHandshaker.HANDSHAKE_ACTION_NAME,
                new TransportHandshaker.HandshakeRequest(version, Build.current().version()),
                TransportRequestOptions.EMPTY,
                v,
                null,
                true
            ),
            ignoreDeserializationErrors
        );
        this.keepAlive = new TransportKeepAlive(threadPool, this.outboundHandler::sendBytes);
        this.inboundHandler = new InboundHandler(
            threadPool,
            outboundHandler,
            namedWriteableRegistry,
            handshaker,
            keepAlive,
            requestHandlers,
            responseHandlers,
            networkService.getHandlingTimeTracker(),
            ignoreDeserializationErrors
        );
    }

    public StatsTracker getStatsTracker() {
        return statsTracker;
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public Supplier<CircuitBreaker> getInflightBreaker() {
        return () -> circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }

    @Override
    public synchronized void setMessageListener(TransportMessageListener listener) {
        outboundHandler.setMessageListener(listener);
        inboundHandler.setMessageListener(listener);
    }

    @Override
    public void setSlowLogThreshold(TimeValue slowLogThreshold) {
        inboundHandler.setSlowLogThreshold(slowLogThreshold);
        outboundHandler.setSlowLogThreshold(slowLogThreshold);
    }

    /**
     * Only used in tests, see {@link #IGNORE_DESERIALIZATION_ERRORS_SETTING}.
     */
    public boolean ignoreDeserializationErrors() {
        return ignoreDeserializationErrors;
    }

    public final class NodeChannels extends CloseableConnection {
        private final Map<TransportRequestOptions.Type, ConnectionProfile.ConnectionTypeHandle> typeMapping;
        private final List<TcpChannel> channels;
        private final DiscoveryNode node;
        private final TransportVersion version;
        private final Compression.Enabled compress;
        private final Compression.Scheme compressionScheme;
        private final AtomicBoolean isClosing = new AtomicBoolean(false);

        NodeChannels(
            DiscoveryNode node,
            List<TcpChannel> channels,
            ConnectionProfile connectionProfile,
            TransportVersion handshakeVersion
        ) {
            this.node = node;
            this.channels = Collections.unmodifiableList(channels);
            assert channels.size() == connectionProfile.getNumConnections()
                : "expected channels size to be == " + connectionProfile.getNumConnections() + " but was: [" + channels.size() + "]";
            typeMapping = new EnumMap<>(TransportRequestOptions.Type.class);
            for (ConnectionProfile.ConnectionTypeHandle handle : connectionProfile.getHandles()) {
                for (TransportRequestOptions.Type type : handle.getTypes()) {
                    typeMapping.put(type, handle);
                }
            }
            version = handshakeVersion;
            compress = connectionProfile.getCompressionEnabled();
            compressionScheme = connectionProfile.getCompressionScheme();
        }

        @Override
        public TransportVersion getTransportVersion() {
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
        public void close() {
            handleClose(null);
        }

        @Override
        public void closeAndFail(Exception e) {
            handleClose(e);
        }

        private void handleClose(Exception e) {
            if (isClosing.compareAndSet(false, true)) {
                try {
                    boolean block = lifecycle.stopped() && Transports.isTransportThread(Thread.currentThread()) == false;
                    CloseableChannel.closeChannels(channels, block);
                } finally {
                    // Call the super method to trigger listeners
                    if (e == null) {
                        super.close();
                    } else {
                        super.closeAndFail(e);
                    }
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

            // In this is a proxy request, determine whether we want to compress based on the wrapped request
            final TransportRequest wrapped;
            if (request instanceof TransportActionProxy.ProxyRequest) {
                wrapped = ((TransportActionProxy.ProxyRequest<?>) request).wrapped;
            } else {
                wrapped = request;
            }

            final Compression.Scheme schemeToUse = getCompressionScheme(wrapped);
            assert false == RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME.equals(action)
                || REMOTE_CLUSTER_PROFILE.equals(channel.getProfile())
                : "remote cluster handshake can only be sent through RCS remote cluster connection";
            outboundHandler.sendRequest(node, channel, requestId, action, request, options, getTransportVersion(), schemeToUse, false);
        }

        private Compression.Scheme getCompressionScheme(TransportRequest request) {
            // We compress if total transport compression is enabled or if indexing_data transport compression
            // is enabled and the request is a RawIndexingDataTransportRequest which indicates it should be
            // compressed.
            final boolean shouldCompress = compress == Compression.Enabled.TRUE
                || (compress == Compression.Enabled.INDEXING_DATA
                    && request instanceof RawIndexingDataTransportRequest
                    && ((RawIndexingDataTransportRequest) request).isRawIndexingData());
            return shouldCompress ? compressionScheme : null;
        }

        @Override
        public String toString() {
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("NodeChannels[");
            node.appendDescriptionWithoutAttributes(stringBuilder);
            stringBuilder.append("]");
            return stringBuilder.toString();
        }
    }

    // This allows transport implementations to potentially override specific connection profiles. This
    // primarily exists for the test implementations.
    protected static ConnectionProfile maybeOverrideConnectionProfile(ConnectionProfile connectionProfile) {
        return connectionProfile;
    }

    protected Recycler<BytesRef> createRecycler(Settings settings, PageCacheRecycler pageCacheRecycler) {
        return new BytesRefRecycler(pageCacheRecycler);
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Transport.Connection> listener) {
        ActionListener.run(listener, l -> {
            Objects.requireNonNull(profile, "connection profile cannot be null");
            if (node == null) {
                throw new ConnectTransportException(null, "can't open connection to a null node");
            }
            final var finalProfile = maybeOverrideConnectionProfile(profile);
            if (closeLock.readLock().tryLock() == false) {
                ensureOpen();
                assert false : "should not get here ever because close-write-lock should only be held on shutdown";
                throw new ConnectTransportException(node, "failed to acquire close-read-lock");
            }
            try {
                ensureOpen();
                initiateConnection(node, finalProfile, l);
            } finally {
                closeLock.readLock().unlock();
            }
        });
    }

    private void initiateConnection(DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Connection> listener) {
        int numConnections = connectionProfile.getNumConnections();
        assert numConnections > 0 : "A connection profile must be configured with at least one connection";

        final List<TcpChannel> channels = new ArrayList<>(numConnections);

        for (int i = 0; i < numConnections; ++i) {
            try {
                final TcpChannel channel = initiateChannel(node, connectionProfile);
                if (logger.isTraceEnabled()) {
                    channel.addConnectListener(new ChannelOpenTraceLogger(channel));
                }
                channels.add(channel);
            } catch (ConnectTransportException e) {
                CloseableChannel.closeChannels(channels, false);
                listener.onFailure(e);
                return;
            } catch (Exception e) {
                CloseableChannel.closeChannels(channels, false);
                listener.onFailure(new ConnectTransportException(node, "general node connection failure", e));
                return;
            }
        }

        ChannelsConnectedListener channelsConnectedListener = new ChannelsConnectedListener(
            node,
            connectionProfile,
            channels,
            new ThreadedActionListener<>(threadPool.generic(), listener)
        );

        for (TcpChannel channel : channels) {
            channel.addConnectListener(channelsConnectedListener);
        }

        TimeValue connectTimeout = connectionProfile.getConnectTimeout();
        threadPool.schedule(channelsConnectedListener::onTimeout, connectTimeout, threadPool.generic());
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    @Override
    public BoundTransportAddress boundRemoteIngressAddress() {
        return profileBoundAddresses().get(REMOTE_CLUSTER_PROFILE);
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return unmodifiableMap(new HashMap<>(profileBoundAddresses));
    }

    @Override
    public List<String> getDefaultSeedAddresses() {
        List<String> local = new ArrayList<>();
        local.add("127.0.0.1");
        // check if v6 is supported, if so, v4 will also work via mapped addresses.
        if (NetworkUtils.SUPPORTS_V6) {
            local.add("[::1]"); // may get ports appended!
        }
        return local.stream()
            .flatMap(address -> Arrays.stream(defaultPortRange()).limit(LIMIT_LOCAL_PORTS_COUNT).mapToObj(port -> address + ":" + port))
            .toList();
    }

    protected void bindServer(ProfileSettings profileSettings) {
        // Bind and start to accept incoming connections.
        InetAddress[] hostAddresses;
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

    private InetSocketAddress bindToPort(final String name, final InetAddress hostAddress, String port) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        if (closeLock.writeLock().tryLock() == false) {
            assert false; // can't be concurrently stopping and mustn't be opening any connections yet
            throw new IllegalStateException("failed to acquire close-write-lock");
        }
        try {
            // No need for locking here since Lifecycle objects can't move from STARTED to INITIALIZED
            if (lifecycle.initialized() == false && lifecycle.started() == false) {
                throw new IllegalStateException("transport has been stopped");
            }
            boolean success = portsRange.iterate(portNumber -> {
                try {
                    TcpServerChannel channel = bind(name, new InetSocketAddress(hostAddress, portNumber));
                    serverChannels.computeIfAbsent(name, k -> new ArrayList<>()).add(channel);
                    boundSocket.set(channel.getLocalAddress());
                } catch (Exception e) {
                    lastException.set(e);
                    return false;
                }
                return true;
            });
            if (success == false) {
                throw new BindTransportException(
                    "Failed to bind to " + NetworkAddress.format(hostAddress, portsRange),
                    lastException.get()
                );
            }
        } finally {
            closeLock.writeLock().unlock();
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Bound profile [{}] to address {{}}", name, NetworkAddress.format(boundSocket.get()));
        }

        return boundSocket.get();
    }

    private BoundTransportAddress createBoundTransportAddress(ProfileSettings profileSettings, List<InetSocketAddress> boundAddresses) {
        String[] boundAddressesHostStrings = new String[boundAddresses.size()];
        TransportAddress[] transportBoundAddresses = new TransportAddress[boundAddresses.size()];
        for (int i = 0; i < boundAddresses.size(); i++) {
            InetSocketAddress nextAddress = boundAddresses.get(i);
            boundAddressesHostStrings[i] = nextAddress.getHostString();
            transportBoundAddresses[i] = new TransportAddress(nextAddress);
        }

        List<String> publishHosts = profileSettings.publishHosts;
        if (profileSettings.isDefaultProfile == false && publishHosts.isEmpty()) {
            publishHosts = Arrays.asList(boundAddressesHostStrings);
        }
        if (publishHosts.isEmpty()) {
            publishHosts = NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings);
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
    static int resolvePublishPort(ProfileSettings profileSettings, List<InetSocketAddress> boundAddresses, InetAddress publishInetAddress) {
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
            final Set<Integer> ports = new HashSet<>();
            for (InetSocketAddress boundAddress : boundAddresses) {
                ports.add(boundAddress.getPort());
            }
            if (ports.size() == 1) {
                publishPort = ports.iterator().next();
            }
        }

        if (publishPort < 0) {
            String profileExplanation = profileSettings.isDefaultProfile ? "" : " for profile " + profileSettings.profileName;
            throw new BindTransportException(
                "Failed to auto-resolve publish port"
                    + profileExplanation
                    + ", multiple bound addresses "
                    + boundAddresses
                    + " with distinct ports and none of them matched the publish address ("
                    + publishInetAddress
                    + "). "
                    + "Please specify a unique port by setting "
                    + TransportSettings.PORT.getKey()
                    + " or "
                    + TransportSettings.PUBLISH_PORT.getKey()
            );
        }
        return publishPort;
    }

    @Override
    public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
        return parse(address, defaultPortRange()[0]);
    }

    private int[] defaultPortRange() {
        return new PortsRange(
            settings.get(
                TransportSettings.PORT_PROFILE.getConcreteSettingForNamespace(TransportSettings.DEFAULT_PROFILE).getKey(),
                TransportSettings.PORT.get(settings)
            )
        ).ports();
    }

    // this code is a take on guava's HostAndPort, like a HostAndPortRange

    // pattern for validating ipv6 bracket addresses.
    // not perfect, but PortsRange should take care of any port range validation, not a regex
    private static final Pattern BRACKET_PATTERN = Pattern.compile("^\\[(.*:.*)\\](?::([\\d\\-]*))?$");

    /**
     * parse a hostname+port spec into its equivalent addresses
     */
    static TransportAddress[] parse(String hostPortString, int defaultPort) throws UnknownHostException {
        Objects.requireNonNull(hostPortString);
        String host;
        String portString = null;

        if (hostPortString.startsWith("[")) {
            // Parse a bracketed host, typically an IPv6 literal.
            Matcher matcher = BRACKET_PATTERN.matcher(hostPortString);
            if (matcher.matches() == false) {
                throw new IllegalArgumentException("Invalid bracketed host/port range: " + hostPortString);
            }
            host = matcher.group(1);
            portString = matcher.group(2);  // could be null
        } else {
            int colonPos = hostPortString.indexOf(':');
            if (colonPos >= 0 && hostPortString.indexOf(':', colonPos + 1) == -1) {
                // Exactly 1 colon. Split into host:port.
                host = hostPortString.substring(0, colonPos);
                portString = hostPortString.substring(colonPos + 1);
            } else {
                // 0 or 2+ colons. Bare hostname or IPv6 literal.
                host = hostPortString;
                // 2+ colons and not bracketed: exception
                if (colonPos >= 0) {
                    throw new IllegalArgumentException("IPv6 addresses must be bracketed: " + hostPortString);
                }
            }
        }

        int port;
        // if port isn't specified, fill with the default
        if (portString == null || portString.isEmpty()) {
            port = defaultPort;
        } else {
            port = Integer.parseInt(portString);
        }

        return Arrays.stream(InetAddress.getAllByName(host))
            .distinct()
            .map(address -> new TransportAddress(address, port))
            .toArray(TransportAddress[]::new);
    }

    @Override
    protected final void doClose() {}

    @Override
    protected final void doStop() {
        assert Transports.assertNotTransportThread("Must not block transport thread that might be needed for closing channels below");
        assert threadPool.generic().isShutdown() == false : "Must stop transport before terminating underlying threadpool";
        closeLock.writeLock().lock();
        try {
            keepAlive.close();

            // first stop to accept any incoming connections so nobody can connect to this transport
            for (Map.Entry<String, List<TcpServerChannel>> entry : serverChannels.entrySet()) {
                String profile = entry.getKey();
                List<TcpServerChannel> channels = entry.getValue();
                ActionListener<Void> closeFailLogger = ActionListener.wrap(
                    c -> {},
                    e -> logger.warn(() -> "Error closing serverChannel for profile [" + profile + "]", e)
                );
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
        }
    }

    public void onException(TcpChannel channel, Exception e) {
        handleException(channel, e, lifecycle, outboundHandler);
    }

    // exposed for tests
    static void handleException(TcpChannel channel, Exception e, Lifecycle lifecycle, OutboundHandler outboundHandler) {

        // these exceptions mean the channel cannot be salvaged: disconnect the node so that we reset everything back to a known-good state
        boolean closeChannel = true;

        try {
            if (lifecycle.started() == false) {
                return;
            }

            final Level closeConnectionExceptionLevel = NetworkExceptionHelper.getCloseConnectionExceptionLevel(
                e,
                outboundHandler.rstOnClose()
            );
            if (closeConnectionExceptionLevel != Level.OFF) {
                if (closeConnectionExceptionLevel == Level.INFO && logger.isDebugEnabled() == false) {
                    logger.info(
                        "close connection exception caught on transport layer [{}], disconnecting from relevant node: {}",
                        channel,
                        e.getMessage()
                    );
                } else {
                    logger.log(
                        closeConnectionExceptionLevel,
                        () -> format(
                            "close connection exception caught on transport layer [%s], disconnecting from relevant node",
                            channel
                        ),
                        e
                    );
                }
            } else if (isConnectException(e)) {
                logger.debug(() -> "connect exception caught on transport layer [" + channel + "]", e);
            } else if (e instanceof BindException) {
                logger.debug(() -> "bind exception caught on transport layer [" + channel + "]", e);
            } else if (e instanceof CancelledKeyException) {
                logger.debug(
                    () -> format("cancelled key exception caught on transport layer [%s], disconnecting from relevant node", channel),
                    e
                );
            } else if (e instanceof HttpRequestOnTransportException) {
                // in this case the client is speaking HTTP so we can do something a bit more useful than simply closing the channel
                if (channel.isOpen()) {
                    outboundHandler.sendBytes(
                        channel,
                        new BytesArray(e.getMessage().getBytes(StandardCharsets.UTF_8)),
                        ActionListener.running(() -> CloseableChannel.closeChannel(channel))
                    );
                    closeChannel = false;
                }
            } else if (e instanceof StreamCorruptedException) {
                logger.warn(() -> format("%s, [%s], closing connection", e.getMessage(), channel));
            } else if (e instanceof TransportNotReadyException) {
                logger.debug(() -> format("%s on [%s], closing connection", e.getMessage(), channel));
            } else if (e instanceof HeaderValidationException headerValidationException) {
                Header header = headerValidationException.header;
                if (channel.isOpen()) {
                    outboundHandler.sendErrorResponse(
                        header.getVersion(),
                        channel,
                        header.getRequestId(),
                        header.getActionName(),
                        ResponseStatsConsumer.NONE,
                        headerValidationException.validationException
                    );
                }
            } else {
                logger.warn(() -> "exception caught on transport layer [" + channel + "], closing connection", e);
            }
        } finally {
            if (closeChannel) {
                channel.setCloseException(e);
                CloseableChannel.closeChannel(channel);
            }
        }
    }

    protected static void onServerException(TcpServerChannel channel, Exception e) {
        if (e instanceof BindException) {
            logger.debug(() -> "bind exception from server channel caught on transport layer [" + channel + "]", e);
        } else {
            logger.error(() -> "exception from server channel caught on transport layer [" + channel + "]", e);
        }
    }

    protected void serverAcceptedChannel(TcpChannel channel) {
        boolean addedOnThisCall = acceptedChannels.add(channel);
        assert addedOnThisCall : "Channel should only be added to accepted channel set once";
        // Mark the channel init time
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        channel.addCloseListener(ActionListener.running(() -> acceptedChannels.remove(channel)));
        logger.trace(() -> format("Tcp transport channel accepted: %s", channel));
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
     * @param connectionProfile the connection profile to use when connecting to the node
     * @return the pending connection
     * @throws IOException if an I/O exception occurs while opening the channel
     */
    protected abstract TcpChannel initiateChannel(DiscoveryNode node, ConnectionProfile connectionProfile) throws IOException;

    /**
     * Called to tear down internal resources
     */
    protected abstract void stopInternal();

    /**
     * Handles inbound message that has been decoded.
     *
     * @param channel the channel the message is from
     * @param message the message
     */
    public void inboundMessage(TcpChannel channel, InboundMessage message) {
        try {
            inboundHandler.inboundMessage(channel, /* autocloses absent exception */ message);
            message = null;
        } catch (Exception e) {
            onException(channel, e);
        } finally {
            if (message != null) {
                message.close();
            }
        }
    }

    /**
     * Validates the first 6 bytes of the message header and returns the length of the message. If 6 bytes
     * are not available, it returns -1.
     *
     * @param networkBytes the will be read
     * @return the length of the message
     * @throws StreamCorruptedException              if the message header format is not recognized
     * @throws HttpRequestOnTransportException       if the message header appears to be an HTTP message
     * @throws IllegalArgumentException              if the message length is greater that the maximum allowed frame size.
     *                                               This is dependent on the available memory.
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
            if (appearsToBeHTTPRequest(headerBuffer)) {
                throw new HttpRequestOnTransportException("This is not an HTTP port");
            }

            if (appearsToBeHTTPResponse(headerBuffer)) {
                throw new StreamCorruptedException(
                    "received HTTP response on transport port, ensure that transport port (not "
                        + "HTTP port) of a remote node is specified in the configuration"
                );
            }

            String firstBytes = "("
                + Integer.toHexString(headerBuffer.get(0) & 0xFF)
                + ","
                + Integer.toHexString(headerBuffer.get(1) & 0xFF)
                + ","
                + Integer.toHexString(headerBuffer.get(2) & 0xFF)
                + ","
                + Integer.toHexString(headerBuffer.get(3) & 0xFF)
                + ")";

            if (appearsToBeTLS(headerBuffer)) {
                throw new StreamCorruptedException("SSL/TLS request received but SSL/TLS is not enabled on this node, got " + firstBytes);
            }

            throw new StreamCorruptedException("invalid internal transport message format, got " + firstBytes);
        }
        final int messageLength = headerBuffer.getInt(TcpHeader.MARKER_BYTES_SIZE);

        if (messageLength == TransportKeepAlive.PING_DATA_SIZE) {
            // This is a ping
            return 0;
        }

        if (messageLength <= 0) {
            throw new StreamCorruptedException("invalid data length: " + messageLength);
        }

        if (messageLength > THIRTY_PER_HEAP_SIZE) {
            throw new IllegalArgumentException(
                "illegal transport message of size ["
                    + ByteSizeValue.ofBytes(messageLength)
                    + "] which exceeds 30% of this node's heap size ["
                    + ByteSizeValue.ofBytes(THIRTY_PER_HEAP_SIZE)
                    + "], closing connection"
            );
        }

        return messageLength;
    }

    private static boolean appearsToBeHTTPRequest(BytesReference headerBuffer) {
        return bufferStartsWith(headerBuffer, "GET")
            || bufferStartsWith(headerBuffer, "POST")
            || bufferStartsWith(headerBuffer, "PUT")
            || bufferStartsWith(headerBuffer, "HEAD")
            || bufferStartsWith(headerBuffer, "DELETE")
            // Actually 'OPTIONS'. But we are only guaranteed to have read six bytes at this point.
            || bufferStartsWith(headerBuffer, "OPTION")
            || bufferStartsWith(headerBuffer, "PATCH")
            || bufferStartsWith(headerBuffer, "TRACE");
    }

    private static boolean appearsToBeHTTPResponse(BytesReference headerBuffer) {
        return bufferStartsWith(headerBuffer, "HTTP");
    }

    private static boolean appearsToBeTLS(BytesReference headerBuffer) {
        return headerBuffer.get(0) == 0x16 && headerBuffer.get(1) == 0x03;
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
    public static class HttpRequestOnTransportException extends ElasticsearchException {

        HttpRequestOnTransportException(String msg) {
            super(msg);
        }

        @Override
        public RestStatus status() {
            return RestStatus.BAD_REQUEST;
        }

        public HttpRequestOnTransportException(StreamInput in) throws IOException {
            super(in);
        }
    }

    public void executeHandshake(
        DiscoveryNode node,
        TcpChannel channel,
        ConnectionProfile profile,
        ActionListener<TransportVersion> listener
    ) {
        long requestId = responseHandlers.newRequestId();
        handshaker.sendHandshake(requestId, node, channel, profile.getHandshakeTimeout(), listener);
    }

    final TransportKeepAlive getKeepAlive() {
        return keepAlive;
    }

    final int getNumPendingHandshakes() {
        return handshaker.getNumPendingHandshakes();
    }

    final long getNumHandshakes() {
        return handshaker.getNumHandshakes();
    }

    final Set<TcpChannel> getAcceptedChannels() {
        return Collections.unmodifiableSet(acceptedChannels);
    }

    @Override
    public RecyclerBytesStreamOutput newNetworkBytesStream() {
        return new RecyclerBytesStreamOutput(recycler);
    }

    /**
     * Ensures this transport is still started / open
     *
     * @throws IllegalStateException if the transport is not started / open
     */
    private void ensureOpen() {
        if (lifecycle.started() == false) {
            throw new IllegalStateException("transport has been stopped");
        }
    }

    @Override
    public final TransportStats getStats() {
        final long bytesWritten = statsTracker.getBytesWritten();
        final long messagesSent = statsTracker.getMessagesSent();
        final long messagesReceived = statsTracker.getMessagesReceived();
        final long bytesRead = statsTracker.getBytesRead();
        return new TransportStats(
            acceptedChannels.size(),
            outboundConnectionCount.get(),
            messagesReceived,
            bytesRead,
            messagesSent,
            bytesWritten,
            networkService.getHandlingTimeTracker().getSnapshot(),
            outboundHandlingTimeTracker.getSnapshot(),
            requestHandlers.getStats()
        );
    }

    /**
     * Returns all profile settings for the given settings object
     */
    public static Set<ProfileSettings> getProfileSettings(Settings settings) {
        HashSet<ProfileSettings> profiles = new HashSet<>();
        // Process remote cluster port first so that errors are consistently reported if there
        // is direct usage of the _remote_cluster profile
        if (REMOTE_CLUSTER_SERVER_ENABLED.get(settings)) {
            profiles.add(RemoteClusterPortSettings.buildRemoteAccessProfileSettings(settings));
        }
        boolean isDefaultSet = false;
        for (String profile : settings.getGroups("transport.profiles.", true).keySet()) {
            profiles.add(new ProfileSettings(settings, profile));
            if (TransportSettings.DEFAULT_PROFILE.equals(profile)) {
                isDefaultSet = true;
            }
        }
        if (isDefaultSet == false) {
            profiles.add(new ProfileSettings(settings, TransportSettings.DEFAULT_PROFILE));
        }
        // Add the remote access profile
        return Collections.unmodifiableSet(profiles);
    }

    /**
     * Representation of a transport profile settings for a {@code transport.profiles.$profilename.*}
     */
    public static final class ProfileSettings {
        public final String profileName;
        public final boolean tcpNoDelay;
        public final boolean tcpKeepAlive;
        public final int tcpKeepIdle;
        public final int tcpKeepInterval;
        public final int tcpKeepCount;
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
            isDefaultProfile = TransportSettings.DEFAULT_PROFILE.equals(profileName);
            tcpKeepAlive = TransportSettings.TCP_KEEP_ALIVE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            tcpKeepIdle = TransportSettings.TCP_KEEP_IDLE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            tcpKeepInterval = TransportSettings.TCP_KEEP_INTERVAL_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            tcpKeepCount = TransportSettings.TCP_KEEP_COUNT_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            tcpNoDelay = TransportSettings.TCP_NO_DELAY_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            reuseAddress = TransportSettings.TCP_REUSE_ADDRESS_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            sendBufferSize = TransportSettings.TCP_SEND_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            receiveBufferSize = TransportSettings.TCP_RECEIVE_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            List<String> profileBindHosts = TransportSettings.BIND_HOST_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            bindHosts = (profileBindHosts.isEmpty() ? NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings) : profileBindHosts);
            publishHosts = TransportSettings.PUBLISH_HOST_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            Setting<String> concretePort = TransportSettings.PORT_PROFILE.getConcreteSettingForNamespace(profileName);
            if (concretePort.exists(settings) == false && isDefaultProfile == false) {
                throw new IllegalStateException("profile [" + profileName + "] has no port configured");
            }
            portOrRange = TransportSettings.PORT_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            publishPort = isDefaultProfile
                ? TransportSettings.PUBLISH_PORT.get(settings)
                : TransportSettings.PUBLISH_PORT_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
        }
    }

    @Override
    public final ResponseHandlers getResponseHandlers() {
        return responseHandlers;
    }

    @Override
    public final RequestHandlers getRequestHandlers() {
        return requestHandlers;
    }

    private final class ChannelsConnectedListener implements ActionListener<Void> {

        private final DiscoveryNode node;
        private final ConnectionProfile connectionProfile;
        private final List<TcpChannel> channels;
        private final ActionListener<Transport.Connection> listener;
        private final CountDown countDown;

        private ChannelsConnectedListener(
            DiscoveryNode node,
            ConnectionProfile connectionProfile,
            List<TcpChannel> channels,
            ActionListener<Transport.Connection> listener
        ) {
            this.node = node;
            this.connectionProfile = connectionProfile;
            this.channels = channels;
            this.listener = listener;
            this.countDown = new CountDown(channels.size());
        }

        @Override
        public void onResponse(Void v) {
            // Returns true if all connections have completed successfully
            if (countDown.countDown()) {
                final TcpChannel handshakeChannel = channels.get(0);
                try {
                    executeHandshake(node, handshakeChannel, connectionProfile, ActionListener.wrap(responseVersion -> {
                        final long connectionId = outboundConnectionCount.incrementAndGet();
                        logger.debug("opened transport connection [{}] to [{}] using channels [{}]", connectionId, node, channels);
                        NodeChannels nodeChannels = new NodeChannels(node, channels, connectionProfile, responseVersion);
                        long relativeMillisTime = threadPool.relativeTimeInMillis();
                        nodeChannels.channels.forEach(ch -> {
                            // Mark the channel init time
                            ch.getChannelStats().markAccessed(relativeMillisTime);
                            ch.addCloseListener(new ActionListener<Void>() {
                                @Override
                                public void onResponse(Void ignored) {
                                    nodeChannels.close();
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    nodeChannels.closeAndFail(e);
                                }
                            });
                        });
                        keepAlive.registerNodeConnection(nodeChannels.channels, connectionProfile);
                        nodeChannels.addCloseListener(new ChannelCloseLogger(node, connectionId, relativeMillisTime));
                        listener.onResponse(nodeChannels);
                    },
                        e -> closeAndFail(
                            e instanceof ConnectTransportException
                                ? e
                                : new ConnectTransportException(node, "general node connection failure", e)
                        )
                    ));
                } catch (Exception ex) {
                    closeAndFail(ex);
                }
            }
        }

        @Override
        public void onFailure(Exception ex) {
            if (countDown.fastForward()) {
                closeAndFail(new ConnectTransportException(node, "connect_exception", ex));
            }
        }

        public void onTimeout() {
            if (countDown.fastForward()) {
                closeAndFail(new ConnectTransportException(node, "connect_timeout[" + connectionProfile.getConnectTimeout() + "]"));
            }
        }

        private void closeAndFail(Exception e) {
            try {
                CloseableChannel.closeChannels(channels, false);
            } catch (Exception ex) {
                e.addSuppressed(ex);
            } finally {
                listener.onFailure(e);
            }
        }
    }

    private class ChannelCloseLogger implements ActionListener<Void> {
        private final DiscoveryNode node;
        private final long connectionId;
        private final long openTimeMillis;

        ChannelCloseLogger(DiscoveryNode node, long connectionId, long openTimeMillis) {
            this.node = node;
            this.connectionId = connectionId;
            this.openTimeMillis = openTimeMillis;
        }

        @Override
        public void onResponse(Void ignored) {
            long closeTimeMillis = threadPool.relativeTimeInMillis();
            logger.debug("closed transport connection [{}] to [{}] with age [{}ms]", connectionId, node, closeTimeMillis - openTimeMillis);
        }

        @Override
        public void onFailure(Exception e) {
            long closeTimeMillis = threadPool.relativeTimeInMillis();
            logger.debug(
                () -> format(
                    "closed transport connection [%d] to [%s] with age [%dms], exception:",
                    connectionId,
                    node,
                    closeTimeMillis - openTimeMillis
                ),
                e
            );
        }
    }

    private static final class ChannelOpenTraceLogger implements ActionListener<Void> {
        private final TcpChannel channel;

        ChannelOpenTraceLogger(TcpChannel channel) {
            this.channel = channel;
        }

        @Override
        public void onResponse(Void unused) {
            logger.trace("Tcp transport channel opened: {}", channel);
        }

        @Override
        public void onFailure(Exception e) {
            // Connection failures are generally logged elsewhere, but go via the ChannelsConnectedListener which only captures the first
            // exception for each bundle of channels. If the ChannelOpenTraceLogger is installed then trace-logging is enabled so we can log
            // every failure.
            logger.trace(() -> format("failed to open transport channel: %s", channel), e);
        }
    }

}
