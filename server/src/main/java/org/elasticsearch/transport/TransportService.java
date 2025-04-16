/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class TransportService extends AbstractLifecycleComponent
    implements
        ReportingService<TransportInfo>,
        TransportMessageListener,
        TransportConnectionListener {

    private static final Logger logger = LogManager.getLogger(TransportService.class);

    /**
     * A feature flag enabling transport upgrades for serverless.
     */
    static final String SERVERLESS_TRANSPORT_SYSTEM_PROPERTY = "es.serverless_transport";
    private static final boolean SERVERLESS_TRANSPORT_FEATURE_FLAG = Booleans.parseBoolean(
        System.getProperty(SERVERLESS_TRANSPORT_SYSTEM_PROPERTY),
        false
    );

    public static final String DIRECT_RESPONSE_PROFILE = ".direct";
    public static final String HANDSHAKE_ACTION_NAME = "internal:transport/handshake";

    /**
     * Undocumented on purpose, may be removed at any time. Only use this if instructed to do so, can have other unintended consequences
     * including deadlocks.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION)
    public static final Setting<Boolean> ENABLE_STACK_OVERFLOW_AVOIDANCE = Setting.boolSetting(
        "transport.enable_stack_protection",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    private volatile boolean handleIncomingRequests;
    protected final Transport transport;
    protected final ConnectionManager connectionManager;
    protected final ThreadPool threadPool;
    protected final ClusterName clusterName;
    protected final TaskManager taskManager;
    private final TransportInterceptor.AsyncSender asyncSender;
    private final Function<BoundTransportAddress, DiscoveryNode> localNodeFactory;
    private final boolean remoteClusterClient;
    private final Transport.ResponseHandlers responseHandlers;
    private final TransportInterceptor interceptor;

    private final PendingDirectHandlers pendingDirectHandlers = new PendingDirectHandlers();

    private final boolean enableStackOverflowAvoidance;

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers = Collections.synchronizedMap(new LinkedHashMap<>(100, .75F, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, TimeoutInfoHolder> eldest) {
            return size() > 100;
        }
    });

    public static final TransportInterceptor NOOP_TRANSPORT_INTERCEPTOR = new TransportInterceptor() {
    };

    // tracer log

    private static final Logger tracerLog = Loggers.getLogger(logger, ".tracer");
    private final Tracer tracer;

    volatile String[] tracerLogInclude;
    volatile String[] tracerLogExclude;

    private final RemoteClusterService remoteClusterService;

    /**
     * if set will call requests sent to this id to shortcut and executed locally
     */
    volatile DiscoveryNode localNode = null;
    private final Transport.Connection localNodeConnection = new Transport.Connection() {
        @Override
        public DiscoveryNode getNode() {
            return localNode;
        }

        @Override
        public TransportVersion getTransportVersion() {
            return TransportVersion.current();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) {
            sendLocalRequest(requestId, action, request, options);
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {}

        @Override
        public void addRemovedListener(ActionListener<Void> listener) {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
            assert false : "should not close the local node connection";
        }

        @Override
        public void incRef() {}

        @Override
        public boolean tryIncRef() {
            return true;
        }

        @Override
        public boolean decRef() {
            return false;
        }

        @Override
        public boolean hasReferences() {
            return true;
        }

        @Override
        public void onRemoved() {
            assert false : "should not remove the local node connection";
        }

        @Override
        public String toString() {
            return "local node connection";
        }
    };

    public TransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor transportInterceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders
    ) {
        this(settings, transport, threadPool, transportInterceptor, localNodeFactory, clusterSettings, taskHeaders, Tracer.NOOP);
    }

    /**
     * Build the service.
     *
     * @param clusterSettings if non null, the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
     *                        updates for {@link TransportSettings#TRACE_LOG_EXCLUDE_SETTING}
     *                        and {@link TransportSettings#TRACE_LOG_INCLUDE_SETTING}.
     */
    public TransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor transportInterceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        TaskManager taskManager,
        Tracer tracer
    ) {
        this(
            settings,
            transport,
            threadPool,
            transportInterceptor,
            localNodeFactory,
            clusterSettings,
            new ClusterConnectionManager(settings, transport, threadPool.getThreadContext()),
            taskManager,
            tracer
        );
    }

    // NOTE: Only for use in tests
    public TransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor transportInterceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders,
        Tracer tracer
    ) {
        this(
            settings,
            transport,
            threadPool,
            transportInterceptor,
            localNodeFactory,
            clusterSettings,
            new ClusterConnectionManager(settings, transport, threadPool.getThreadContext()),
            new TaskManager(settings, threadPool, taskHeaders),
            tracer
        );
    }

    @SuppressWarnings("this-escape")
    public TransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor transportInterceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        ConnectionManager connectionManager,
        TaskManager taskManger,
        Tracer tracer
    ) {
        this.transport = transport;
        transport.setSlowLogThreshold(TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING.get(settings));
        this.threadPool = threadPool;
        this.localNodeFactory = localNodeFactory;
        this.connectionManager = connectionManager;
        this.tracer = tracer;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        setTracerLogInclude(TransportSettings.TRACE_LOG_INCLUDE_SETTING.get(settings));
        setTracerLogExclude(TransportSettings.TRACE_LOG_EXCLUDE_SETTING.get(settings));
        this.taskManager = taskManger;
        this.interceptor = transportInterceptor;
        this.asyncSender = interceptor.interceptSender(this::sendRequestInternal);
        this.remoteClusterClient = DiscoveryNode.isRemoteClusterClient(settings);
        this.enableStackOverflowAvoidance = ENABLE_STACK_OVERFLOW_AVOIDANCE.get(settings);
        remoteClusterService = new RemoteClusterService(settings, this);
        responseHandlers = transport.getResponseHandlers();
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(TransportSettings.TRACE_LOG_INCLUDE_SETTING, this::setTracerLogInclude);
            clusterSettings.addSettingsUpdateConsumer(TransportSettings.TRACE_LOG_EXCLUDE_SETTING, this::setTracerLogExclude);
            if (remoteClusterClient) {
                remoteClusterService.listenForUpdates(clusterSettings);
            }
            clusterSettings.addSettingsUpdateConsumer(TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING, transport::setSlowLogThreshold);
        }
        registerRequestHandler(
            HANDSHAKE_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            false,
            false,
            HandshakeRequest::new,
            (request, channel, task) -> channel.sendResponse(
                new HandshakeResponse(localNode.getVersion(), Build.current().hash(), localNode, clusterName)
            )
        );
    }

    public RemoteClusterService getRemoteClusterService() {
        return remoteClusterService;
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public Transport.Connection getLocalNodeConnection() {
        return localNodeConnection;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    void setTracerLogInclude(List<String> tracerLogInclude) {
        this.tracerLogInclude = tracerLogInclude.toArray(Strings.EMPTY_ARRAY);
    }

    void setTracerLogExclude(List<String> tracerLogExclude) {
        this.tracerLogExclude = tracerLogExclude.toArray(Strings.EMPTY_ARRAY);
    }

    @Override
    protected void doStart() {
        transport.setMessageListener(this);
        connectionManager.addListener(this);
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
            for (Map.Entry<String, BoundTransportAddress> entry : transport.profileBoundAddresses().entrySet()) {
                logger.info("profile [{}]: {}", entry.getKey(), entry.getValue());
            }
        }
        localNode = localNodeFactory.apply(transport.boundAddress());

        if (remoteClusterClient) {
            // here we start to connect to the remote clusters
            remoteClusterService.initializeRemoteClusters();
        }
    }

    @Override
    protected void doStop() {
        try {
            IOUtils.close(connectionManager, remoteClusterService, transport::stop, pendingDirectHandlers::stop);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            // The underlying transport has stopped which closed all the connections to remote nodes and hence completed all their handlers,
            // but there may still be pending handlers for node-local requests since this connection is not closed, and we may also
            // (briefly) track handlers for requests which are sent concurrently with stopping even though the underlying connection is
            // now closed. We complete all these outstanding handlers here:
            for (final Transport.ResponseContext<?> holderToNotify : responseHandlers.prune(Predicates.always())) {
                try {
                    final TransportResponseHandler<?> handler = holderToNotify.handler();
                    final var targetNode = holderToNotify.connection().getNode();
                    final long requestId = holderToNotify.requestId();
                    if (tracerLog.isTraceEnabled() && shouldTraceAction(holderToNotify.action())) {
                        tracerLog.trace("[{}][{}] pruning request for node [{}]", requestId, holderToNotify.action(), targetNode);
                    }

                    assert transport instanceof TcpTransport == false
                        /* other transports (used in tests) may not implement the proper close-connection behaviour. TODO fix this. */
                        || targetNode.equals(localNode)
                        /* local node connection cannot be closed so may still have pending handlers */
                        || holderToNotify.connection().isClosed()
                        /* connections to remote nodes must be closed by this point but could still have pending handlers */
                        : "expected only responses for local "
                            + localNode
                            + " but found handler for ["
                            + holderToNotify.action()
                            + "] on open connection to "
                            + targetNode;

                    final var exception = new SendRequestTransportException(
                        targetNode,
                        holderToNotify.action(),
                        new NodeClosedException(localNode)
                    );
                    final var executor = handler.executor();
                    if (executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
                        handler.handleException(exception);
                    } else {
                        executor.execute(new ForkingResponseHandlerRunnable(handler, exception) {
                            @Override
                            protected void doRun() {
                                handler.handleException(exception);
                            }
                        });
                    }
                } catch (Exception e) {
                    assert false : e;
                    logger.warn(() -> format("failed to notify response handler on shutdown, action: %s", holderToNotify.action()), e);
                }
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        transport.close();
    }

    /**
     * Start accepting incoming requests.
     * <p>
     * The transport service starts before it's ready to accept incoming requests because we need to know the address(es) to which we are
     * bound, which means we have to actually bind to them and start accepting incoming connections. However until this method is called we
     * reject any incoming requests, including handshakes, by closing the connection.
     */
    public final void acceptIncomingRequests() {
        assert handleIncomingRequests == false : "transport service was already accepting incoming requests";
        handleIncomingRequests = true;
        logger.debug("now accepting incoming requests");
    }

    @Override
    public TransportInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        final Map<String, BoundTransportAddress> profileAddresses = transport.profileBoundAddresses();
        if (remoteClusterService.isRemoteClusterServerEnabled()) {
            final Map<String, BoundTransportAddress> filteredProfileAddress = profileAddresses.entrySet()
                .stream()
                .filter(entry -> false == RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE.equals(entry.getKey()))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
            return new TransportInfo(boundTransportAddress, filteredProfileAddress);
        } else {
            return new TransportInfo(boundTransportAddress, profileAddresses);
        }
    }

    public TransportStats stats() {
        return transport.getStats();
    }

    public boolean isTransportSecure() {
        return transport.isSecure();
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public BoundTransportAddress boundRemoteAccessAddress() {
        return transport.boundRemoteIngressAddress();
    }

    public List<String> getDefaultSeedAddresses() {
        return transport.getDefaultSeedAddresses();
    }

    /**
     * Returns <code>true</code> iff the given node is already connected.
     */
    public boolean nodeConnected(DiscoveryNode node) {
        return isLocalNode(node) || connectionManager.nodeConnected(node);
    }

    /**
     * Connect to the specified node with the given connection profile.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param node     the node to connect to
     * @param listener the action listener to notify
     */
    public void connectToNode(DiscoveryNode node, ActionListener<Releasable> listener) throws ConnectTransportException {
        connectToNode(node, null, listener);
    }

    /**
     * Connect to the specified node with the given connection profile.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param node              the node to connect to
     * @param connectionProfile the connection profile to use when connecting to this node
     * @param listener          the action listener to notify
     */
    public void connectToNode(
        final DiscoveryNode node,
        @Nullable ConnectionProfile connectionProfile,
        ActionListener<Releasable> listener
    ) {
        if (isLocalNode(node)) {
            listener.onResponse(null);
            return;
        }
        connectionManager.connectToNode(node, connectionProfile, connectionValidator(node), listener);
    }

    public ConnectionManager.ConnectionValidator connectionValidator(DiscoveryNode node) {
        return (newConnection, actualProfile, listener) -> {
            // We don't validate cluster names to allow for CCS connections.
            handshake(newConnection, actualProfile.getHandshakeTimeout(), Predicates.always(), listener.map(resp -> {
                final DiscoveryNode remote = resp.discoveryNode;
                if (node.equals(remote) == false) {
                    throw new ConnectTransportException(
                        node,
                        Strings.format(
                            """
                                Connecting to [%s] failed: expected to connect to [%s] but found [%s] instead. Ensure that each node has \
                                its own distinct publish address, and that your network is configured so that every connection to a node's \
                                publish address is routed to the correct node. See %s for more information.""",
                            node.getAddress(),
                            node.descriptionWithoutAttributes(),
                            remote.descriptionWithoutAttributes(),
                            ReferenceDocs.NETWORK_BINDING_AND_PUBLISHING
                        )
                    );
                }
                return null;
            }));
        };
    }

    /**
     * Establishes a new connection to the given node. The connection is NOT maintained by this service, it's the callers
     * responsibility to close the connection once it goes out of scope.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param node              the node to connect to
     * @param connectionProfile the connection profile to use
     * @param listener          the action listener to notify
     */
    public void openConnection(
        final DiscoveryNode node,
        ConnectionProfile connectionProfile,
        ActionListener<Transport.Connection> listener
    ) {
        if (isLocalNode(node)) {
            listener.onResponse(localNodeConnection);
        } else {
            connectionManager.openConnection(node, connectionProfile, listener);
        }
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node mismatches the local cluster name.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @param listener         action listener to notify
     * @throws ConnectTransportException if the connection failed
     * @throws IllegalStateException     if the handshake failed
     */
    public void handshake(
        final Transport.Connection connection,
        final TimeValue handshakeTimeout,
        final ActionListener<DiscoveryNode> listener
    ) {
        handshake(connection, handshakeTimeout, clusterName.getEqualityPredicate(), listener.map(HandshakeResponse::getDiscoveryNode));
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node doesn't match the local cluster name.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param connection           the connection to a specific node
     * @param handshakeTimeout     handshake timeout
     * @param clusterNamePredicate cluster name validation predicate
     * @param listener             action listener to notify
     * @throws IllegalStateException if the handshake failed
     */
    public void handshake(
        final Transport.Connection connection,
        final TimeValue handshakeTimeout,
        Predicate<ClusterName> clusterNamePredicate,
        final ActionListener<HandshakeResponse> listener
    ) {
        final DiscoveryNode node = connection.getNode();
        sendRequest(
            connection,
            HANDSHAKE_ACTION_NAME,
            HandshakeRequest.INSTANCE,
            TransportRequestOptions.timeout(handshakeTimeout),
            new ActionListenerResponseHandler<>(listener.delegateFailure((l, response) -> {
                if (clusterNamePredicate.test(response.clusterName) == false) {
                    l.onFailure(
                        new IllegalStateException(
                            "handshake with ["
                                + node
                                + "] failed: remote cluster name ["
                                + response.clusterName.value()
                                + "] does not match "
                                + clusterNamePredicate
                        )
                    );
                } else if (response.version.isCompatible(localNode.getVersion()) == false) {
                    l.onFailure(
                        new IllegalStateException(
                            "handshake with ["
                                + node
                                + "] failed: remote node version ["
                                + response.version
                                + "] is incompatible with local node version ["
                                + localNode.getVersion()
                                + "]"
                        )
                    );
                } else {
                    l.onResponse(response);
                }
            }), HandshakeResponse::new, threadPool.generic())
        );
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public RecyclerBytesStreamOutput newNetworkBytesStream() {
        return transport.newNetworkBytesStream();
    }

    static class HandshakeRequest extends AbstractTransportRequest {

        public static final HandshakeRequest INSTANCE = new HandshakeRequest();

        HandshakeRequest(StreamInput in) throws IOException {
            super(in);
        }

        private HandshakeRequest() {}

    }

    public static class HandshakeResponse extends TransportResponse {

        private final Version version;

        private final String buildHash;

        private final DiscoveryNode discoveryNode;

        private final ClusterName clusterName;

        public HandshakeResponse(Version version, String buildHash, DiscoveryNode discoveryNode, ClusterName clusterName) {
            this.buildHash = Objects.requireNonNull(buildHash);
            this.discoveryNode = Objects.requireNonNull(discoveryNode);
            this.version = Objects.requireNonNull(version);
            this.clusterName = Objects.requireNonNull(clusterName);
        }

        public HandshakeResponse(StreamInput in) throws IOException {
            // the first two fields need only VInts and raw (ASCII) characters, so we cross our fingers and hope that they appear
            // on the wire as we expect them to even if this turns out to be an incompatible build
            version = Version.readVersion(in);
            buildHash = in.readString();

            try {
                // If the remote node is incompatible then make an effort to identify it anyway, so we can mention it in the exception
                // message, but recognise that this may fail
                discoveryNode = new DiscoveryNode(in);
            } catch (Exception e) {
                maybeThrowOnIncompatibleBuild(null, e);
                throw e;
            }
            maybeThrowOnIncompatibleBuild(discoveryNode, null);
            clusterName = new ClusterName(in);
        }

        private void maybeThrowOnIncompatibleBuild(@Nullable DiscoveryNode node, @Nullable Exception e) {
            if (SERVERLESS_TRANSPORT_FEATURE_FLAG == false && isIncompatibleBuild(version, buildHash)) {
                throwOnIncompatibleBuild(node, e);
            }
        }

        private void throwOnIncompatibleBuild(@Nullable DiscoveryNode node, @Nullable Exception e) {
            throw new IllegalArgumentException(
                "remote node ["
                    + (node == null ? "unidentifiable" : node)
                    + "] is build ["
                    + buildHash
                    + "] of version ["
                    + version
                    + "] but this node is build ["
                    + Build.current().hash()
                    + "] of version ["
                    + Build.current().version()
                    + "] which has an incompatible wire format",
                e
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Version.writeVersion(version, out);
            out.writeString(buildHash);
            discoveryNode.writeTo(out);
            clusterName.writeTo(out);
        }

        public Version getVersion() {
            return version;
        }

        public String getBuildHash() {
            return buildHash;
        }

        public DiscoveryNode getDiscoveryNode() {
            return discoveryNode;
        }

        public ClusterName getClusterName() {
            return clusterName;
        }

        private static boolean isIncompatibleBuild(Version version, String buildHash) {
            return version == Version.CURRENT && Build.current().hash().equals(buildHash) == false;
        }
    }

    public void disconnectFromNode(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return;
        }
        connectionManager.disconnectFromNode(node);
    }

    public void addConnectionListener(TransportConnectionListener listener) {
        connectionManager.addListener(listener);
    }

    public void removeConnectionListener(TransportConnectionListener listener) {
        connectionManager.removeListener(listener);
    }

    public <T extends TransportResponse> void sendRequest(
        final DiscoveryNode node,
        final String action,
        final TransportRequest request,
        final TransportResponseHandler<T> handler
    ) {
        sendRequest(node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public final <T extends TransportResponse> void sendRequest(
        final DiscoveryNode node,
        final String action,
        final TransportRequest request,
        final TransportRequestOptions options,
        TransportResponseHandler<T> handler
    ) {
        final Transport.Connection connection = getConnectionOrFail(node, action, handler);
        if (connection != null) {
            sendRequest(connection, action, request, options, handler);
        }
    }

    /**
     * Get connection to the given {@code node} if possible and fail {@code handler} otherwise.
     *
     * @return connection if one could be found to {@code node} or null otherwise
     */
    @Nullable
    private Transport.Connection getConnectionOrFail(DiscoveryNode node, String action, TransportResponseHandler<?> handler) {
        try {
            var connection = getConnection(node);
            if (connection == null) {
                final var ex = new NodeNotConnectedException(node, "Node not connected");
                assert false : ex;
                throw ex;
            }
            return connection;
        } catch (TransportException transportException) {
            // should only be a NodeNotConnectedException in practice, but handle all cases anyway to be sure
            assert transportException instanceof NodeNotConnectedException : transportException;
            handleSendRequestException(handler, transportException);
            return null;
        } catch (Exception exception) {
            // shouldn't happen in practice, but handle it anyway to be sure
            assert false : exception;
            handleSendRequestException(handler, new SendRequestTransportException(node, action, exception));
            return null;
        }
    }

    /**
     * Unwraps and returns the actual underlying connection of the given connection.
     */
    public static Transport.Connection unwrapConnection(Transport.Connection connection) {
        Transport.Connection unwrapped = connection;
        while (unwrapped instanceof RemoteConnectionManager.ProxyConnection proxyConnection) {
            unwrapped = proxyConnection.getConnection();
        }
        return unwrapped;
    }

    /**
     * Sends a request on the specified connection. If there is a failure sending the request, the specified handler is invoked.
     *
     * @param connection the connection to send the request on
     * @param action     the name of the action
     * @param request    the request
     * @param options    the options for this request
     * @param handler    the response handler
     * @param <T>        the type of the transport response
     */
    public final <T extends TransportResponse> void sendRequest(
        final Transport.Connection connection,
        final String action,
        final TransportRequest request,
        final TransportRequestOptions options,
        final TransportResponseHandler<T> handler
    ) {
        try {
            final TransportResponseHandler<T> delegate;
            if (request.getParentTask().isSet()) {
                // If the connection is a proxy connection, then we will create a cancellable proxy task on the proxy node and an actual
                // child task on the target node of the remote cluster.
                // ----> a parent task on the local cluster
                // |
                // ----> a proxy task on the proxy node on the remote cluster
                // |
                // ----> an actual child task on the target node on the remote cluster
                // To cancel the child task on the remote cluster, we must send a cancel request to the proxy node instead of the target
                // node as the parent task of the child task is the proxy task not the parent task on the local cluster. Hence, here we
                // unwrap the connection and keep track of the connection to the proxy node instead of the proxy connection.
                final Transport.Connection unwrappedConn = unwrapConnection(connection);
                final Releasable unregisterChildNode = taskManager.registerChildConnection(request.getParentTask().getId(), unwrappedConn);
                if (unregisterChildNode == null) {
                    delegate = handler;
                } else {
                    delegate = new UnregisterChildTransportResponseHandler<>(
                        unregisterChildNode,
                        handler,
                        action,
                        request,
                        unwrappedConn,
                        taskManager
                    );
                }
            } else {
                delegate = handler;
            }
            asyncSender.sendRequest(connection, action, request, options, delegate);
        } catch (TransportException transportException) {
            handleSendRequestException(handler, transportException);
        } catch (Exception exception) {
            handleSendRequestException(handler, new SendRequestTransportException(connection.getNode(), action, exception));
        }
    }

    private static void handleSendRequestException(TransportResponseHandler<?> handler, TransportException transportException) {
        try {
            handler.handleException(transportException);
        } catch (Exception innerException) {
            // should not happen
            innerException.addSuppressed(transportException);
            logger.error("unexpected exception from handler.handleException", innerException);
            assert false : innerException;
        }
    }

    /**
     * Returns either a real transport connection or a local node connection if we are using the local node optimization.
     *
     * @throws NodeNotConnectedException if the given node is not connected
     */
    public Transport.Connection getConnection(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return localNodeConnection;
        } else {
            return connectionManager.getConnection(node);
        }
    }

    public final <T extends TransportResponse> void sendChildRequest(
        final DiscoveryNode node,
        final String action,
        final TransportRequest request,
        final Task parentTask,
        final TransportRequestOptions options,
        final TransportResponseHandler<T> handler
    ) {
        final Transport.Connection connection = getConnectionOrFail(node, action, handler);
        if (connection != null) {
            sendChildRequest(connection, action, request, parentTask, options, handler);
        }
    }

    public <T extends TransportResponse> void sendChildRequest(
        final Transport.Connection connection,
        final String action,
        final TransportRequest request,
        final Task parentTask,
        final TransportResponseHandler<T> handler
    ) {
        sendChildRequest(connection, action, request, parentTask, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> void sendChildRequest(
        final Transport.Connection connection,
        final String action,
        final TransportRequest request,
        final Task parentTask,
        final TransportRequestOptions options,
        final TransportResponseHandler<T> handler
    ) {
        request.setParentTask(localNode.getId(), parentTask.getId());
        sendRequest(connection, action, request, options, handler);
    }

    private <T extends TransportResponse> void sendRequestInternal(
        final Transport.Connection connection,
        final String action,
        final TransportRequest request,
        final TransportRequestOptions options,
        TransportResponseHandler<T> handler
    ) {
        if (connection == null) {
            throw new IllegalStateException("can't send request to a null connection");
        }
        DiscoveryNode node = connection.getNode();

        Supplier<ThreadContext.StoredContext> storedContextSupplier = threadPool.getThreadContext().newRestorableContext(true);
        ContextRestoreResponseHandler<T> responseHandler = new ContextRestoreResponseHandler<>(storedContextSupplier, handler);
        // TODO we can probably fold this entire request ID dance into connection.sendRequest but it will be a bigger refactoring
        final long requestId = responseHandlers.add(responseHandler, connection, action).requestId();
        request.setRequestId(requestId);
        final TimeoutHandler timeoutHandler;
        if (options.timeout() != null) {
            timeoutHandler = new TimeoutHandler(requestId, connection.getNode(), action);
            responseHandler.setTimeoutHandler(timeoutHandler);
        } else {
            timeoutHandler = null;
        }
        if (lifecycle.stoppedOrClosed()) {
            /*
             * If we are not started the exception handling will remove the request holder again and calls the handler to notify the
             * caller. It will only notify if toStop hasn't done the work yet.
             */
            handleInternalSendException(action, node, requestId, timeoutHandler, new NodeClosedException(localNode));
            return;
        }
        try {
            if (timeoutHandler != null) {
                assert options.timeout() != null;
                timeoutHandler.scheduleTimeout(options.timeout());
            }
            logger.trace("sending internal request id [{}] action [{}] request [{}] options [{}]", requestId, action, request, options);
            connection.sendRequest(requestId, action, request, options); // local node optimization happens upstream
        } catch (final Exception e) {
            handleInternalSendException(action, node, requestId, timeoutHandler, e);
        }
    }

    protected void handleInternalSendException(
        String action,
        DiscoveryNode node,
        long requestId,
        @Nullable TimeoutHandler timeoutHandler,
        Exception failure
    ) {
        final Transport.ResponseContext<? extends TransportResponse> contextToNotify = responseHandlers.remove(requestId);
        if (contextToNotify == null) {
            // handler has already been completed somehow, nothing to do here
            logger.debug("Exception while sending request, handler likely already notified due to timeout", failure);
            return;
        }
        if (timeoutHandler != null) {
            timeoutHandler.cancel();
        }
        final var sendRequestException = new SendRequestTransportException(node, action, failure);
        final var handler = contextToNotify.handler();
        final var executor = getInternalSendExceptionExecutor(handler.executor());
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
                    tracerLog.trace("[{}][{}] failed to send request to node [{}]", requestId, action, node);
                }
                try {
                    handler.handleException(sendRequestException);
                } catch (Exception e) {
                    assert false : e;
                    if (e != sendRequestException) {
                        e.addSuppressed(sendRequestException);
                    }
                    logger.error(
                        Strings.format(
                            "[%d][%s] failed to notify handler [%s] of failure to send request to node [%s]",
                            requestId,
                            action,
                            handler,
                            node
                        ),
                        e
                    );
                    // indicates a bug in the handler but there's not much else we can do about it now, just carry on
                }
            }

            @Override
            public void onFailure(Exception e) {
                assert false : e;
                logger.error(() -> format("failed to notify response handler on exception, action: %s", contextToNotify.action()), e);
            }

            @Override
            public boolean isForceExecution() {
                return true; // must complete every waiting listener
            }

            @Override
            public void onRejection(Exception e) {
                if (e != sendRequestException) {
                    sendRequestException.addSuppressed(e);
                }
                // force-execution means we won't be rejected unless we're shutting down
                assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
                // in this case it's better to complete the handler on the calling thread rather than leaking it
                doRun();
            }
        });
    }

    private Executor getInternalSendExceptionExecutor(Executor handlerExecutor) {
        if (lifecycle.stoppedOrClosed()) {
            // too late to try and dispatch anywhere else, let's just use the calling thread
            return EsExecutors.DIRECT_EXECUTOR_SERVICE;
        } else if (handlerExecutor == EsExecutors.DIRECT_EXECUTOR_SERVICE && enableStackOverflowAvoidance) {
            // If the handler is non-forking and stack overflow protection is enabled then dispatch to GENERIC
            // Otherwise we let the handler deal with any potential stack overflow (this is the default)
            return threadPool.generic();
        } else {
            return handlerExecutor;
        }
    }

    private void sendLocalRequest(long requestId, final String action, final TransportRequest request, TransportRequestOptions options) {
        final DirectResponseChannel channel = new DirectResponseChannel(localNode, action, requestId, this);
        try {
            onRequestSent(localNode, requestId, action, request, options);
            onRequestReceived(requestId, action);
            @SuppressWarnings("unchecked")
            final RequestHandlerRegistry<TransportRequest> reg = (RequestHandlerRegistry<TransportRequest>) getRequestHandler(action);
            if (reg == null) {
                assert false : action;
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final Executor executor = reg.getExecutor();
            if (executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
                try (var ignored = threadPool.getThreadContext().newTraceContext()) {
                    try {
                        reg.processMessageReceived(request, channel);
                    } catch (Exception e) {
                        handleSendToLocalException(channel, e, action);
                    }
                }
            } else {
                boolean success = false;
                request.mustIncRef();
                try {
                    executor.execute(threadPool.getThreadContext().preserveContextWithTracing(new AbstractRunnable() {
                        @Override
                        protected void doRun() throws Exception {
                            reg.processMessageReceived(request, channel);
                        }

                        @Override
                        public boolean isForceExecution() {
                            return reg.isForceExecution();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            handleSendToLocalException(channel, e, action);
                        }

                        @Override
                        public String toString() {
                            return "processing of [" + requestId + "][" + action + "]: " + request;
                        }

                        @Override
                        public void onAfter() {
                            request.decRef();
                        }
                    }));
                    success = true;
                } finally {
                    if (success == false) {
                        request.decRef();
                    }
                }
            }
        } catch (Exception e) {
            assert false : e;
            handleSendToLocalException(channel, e, action);
        }
    }

    private static void handleSendToLocalException(DirectResponseChannel channel, Exception e, String action) {
        try {
            channel.sendResponse(e);
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.warn(() -> "failed to notify channel of error message for action [" + action + "]", inner);
        }
    }

    private boolean shouldTraceAction(String action) {
        return shouldTraceAction(action, tracerLogInclude, tracerLogExclude);
    }

    public static boolean shouldTraceAction(String action, String[] include, String[] exclude) {
        if (include.length > 0) {
            if (Regex.simpleMatch(include, action) == false) {
                return false;
            }
        }
        if (exclude.length > 0) {
            return Regex.simpleMatch(exclude, action) == false;
        }
        return true;
    }

    public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
        return transport.addressesFromString(address);
    }

    /**
     * A set of all valid action prefixes.
     */
    public static final Set<String> VALID_ACTION_PREFIXES = Set.of(
        "indices:admin",
        "indices:monitor",
        "indices:data/write",
        "indices:data/read",
        "indices:internal",
        "cluster:admin",
        "cluster:monitor",
        "cluster:internal",
        "internal:"
    );

    private static void validateActionName(String actionName) {
        // TODO we should makes this a hard validation and throw an exception but we need a good way to add backwards layer
        // for it. Maybe start with a deprecation layer
        if (isValidActionName(actionName) == false) {
            logger.warn("invalid action name [" + actionName + "] must start with one of: " + TransportService.VALID_ACTION_PREFIXES);
        }
    }

    /**
     * Returns <code>true</code> iff the action name starts with a valid prefix.
     *
     * @see #VALID_ACTION_PREFIXES
     */
    public static boolean isValidActionName(String actionName) {
        for (String prefix : VALID_ACTION_PREFIXES) {
            if (actionName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Registers a new request handler
     *
     * @param action        The action the request handler is associated with
     * @param requestReader a callable to be used construct new instances for streaming
     * @param executor      The executor the request handling will be executed on
     * @param handler       The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(
        String action,
        Executor executor,
        Writeable.Reader<Request> requestReader,
        TransportRequestHandler<Request> handler
    ) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, false, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action,
            requestReader,
            taskManager,
            handler,
            executor,
            false,
            true,
            tracer
        );
        transport.registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
     *
     * @param action                The action the request handler is associated with
     * @param requestReader         The request class that will be used to construct new instances for streaming
     * @param executor              The executor the request handling will be executed on
     * @param forceExecution        Force execution on the executor queue and never reject it
     * @param canTripCircuitBreaker Check the request size and raise an exception in case the limit is breached.
     * @param handler               The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(
        String action,
        Executor executor,
        boolean forceExecution,
        boolean canTripCircuitBreaker,
        Writeable.Reader<Request> requestReader,
        TransportRequestHandler<Request> handler
    ) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, forceExecution, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action,
            requestReader,
            taskManager,
            handler,
            executor,
            forceExecution,
            canTripCircuitBreaker,
            tracer
        );
        transport.registerRequestHandler(reg);
    }

    /**
     * called by the {@link Transport} implementation when an incoming request arrives but before
     * any parsing of it has happened (with the exception of the requestId and action)
     */
    @Override
    public void onRequestReceived(long requestId, String action) {
        if (handleIncomingRequests == false) {
            throw new TransportNotReadyException();
        }
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] received request", requestId, action);
        }
    }

    /**
     * called by the {@link Transport} implementation once a request has been sent
     */
    @Override
    public void onRequestSent(
        DiscoveryNode node,
        long requestId,
        String action,
        TransportRequest request,
        TransportRequestOptions options
    ) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] sent to [{}] (timeout: [{}])", requestId, action, node, options.timeout());
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void onResponseReceived(long requestId, Transport.ResponseContext holder) {
        if (holder == null) {
            checkForTimeout(requestId);
        } else if (tracerLog.isTraceEnabled() && shouldTraceAction(holder.action())) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, holder.action(), holder.connection().getNode());
        }
    }

    /**
     * called by the {@link Transport} implementation once a response was sent to calling node
     */
    @Override
    public void onResponseSent(long requestId, String action) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] sent response", requestId, action);
        }
    }

    /**
     * called by the {@link Transport} implementation after an exception was sent as a response to an incoming request
     */
    @Override
    public void onResponseSent(long requestId, String action, Exception e) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace(() -> format("[%s][%s] sent error response", requestId, action), e);
        }
    }

    public RequestHandlerRegistry<? extends TransportRequest> getRequestHandler(String action) {
        return transport.getRequestHandlers().getHandler(action);
    }

    private void checkForTimeout(long requestId) {
        // lets see if its in the timeout holder, but sync on mutex to make sure any ongoing timeout handling has finished
        final DiscoveryNode sourceNode;
        final String action;
        assert responseHandlers.contains(requestId) == false;
        TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId);
        if (timeoutInfoHolder != null) {
            long time = threadPool.relativeTimeInMillis();
            long sentMs = time - timeoutInfoHolder.sentTime();
            long timedOutMs = time - timeoutInfoHolder.timeoutTime();
            logger.warn(
                "Received response for a request that has timed out, sent [{}/{}ms] ago, timed out [{}/{}ms] ago, "
                    + "action [{}], node [{}], id [{}]",
                TimeValue.timeValueMillis(sentMs),
                sentMs,
                TimeValue.timeValueMillis(timedOutMs),
                timedOutMs,
                timeoutInfoHolder.action(),
                timeoutInfoHolder.node(),
                requestId
            );
            action = timeoutInfoHolder.action();
            sourceNode = timeoutInfoHolder.node();
        } else {
            logger.warn("Transport response handler not found of id [{}]", requestId);
            action = null;
            sourceNode = null;
        }
        // call tracer out of lock
        if (tracerLog.isTraceEnabled() == false) {
            return;
        }
        if (action == null) {
            assert sourceNode == null;
            tracerLog.trace("[{}] received response but can't resolve it to a request", requestId);
        } else if (shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, action, sourceNode);
        }
    }

    @Override
    public void onConnectionClosed(Transport.Connection connection) {
        List<Transport.ResponseContext<? extends TransportResponse>> pruned = responseHandlers.prune(
            h -> h.connection().getCacheKey().equals(connection.getCacheKey())
        );
        if (pruned.isEmpty()) {
            return;
        }

        for (Transport.ResponseContext<?> holderToNotify : pruned) {
            if (tracerLog.isTraceEnabled() && shouldTraceAction(holderToNotify.action())) {
                tracerLog.trace(
                    "[{}][{}] pruning request because connection to node [{}] closed",
                    holderToNotify.requestId(),
                    holderToNotify.action(),
                    connection.getNode()
                );
            }
            NodeDisconnectedException exception = new NodeDisconnectedException(connection.getNode(), holderToNotify.action());

            TransportResponseHandler<?> handler = holderToNotify.handler();
            // we used to fork to a different thread always to avoid stack overflows, but we avoid doing that now, expecting handlers
            // to handle that themselves instead.
            var executor = handler.executor();
            if (executor == EsExecutors.DIRECT_EXECUTOR_SERVICE && enableStackOverflowAvoidance) {
                executor = threadPool.generic();
            }
            if (executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
                handler.handleException(exception);
            } else {
                executor.execute(new ForkingResponseHandlerRunnable(handler, exception, executor) {
                    @Override
                    protected void doRun() {
                        handler.handleException(exception);
                    }

                    @Override
                    public String toString() {
                        return "onConnectionClosed/handleException[" + handler + "]";
                    }
                });
            }
        }
    }

    final class TimeoutHandler implements Runnable {

        private final long requestId;
        private final long sentTime = threadPool.relativeTimeInMillis();
        private final String action;
        private final DiscoveryNode node;
        volatile Scheduler.Cancellable cancellable;

        TimeoutHandler(long requestId, DiscoveryNode node, String action) {
            this.requestId = requestId;
            this.node = node;
            this.action = action;
        }

        @Override
        public void run() {
            if (responseHandlers.contains(requestId)) {
                long timeoutTime = threadPool.relativeTimeInMillis();
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(node, action, sentTime, timeoutTime));
                // now that we have the information visible via timeoutInfoHandlers, we try to remove the request id
                final Transport.ResponseContext<? extends TransportResponse> holder = responseHandlers.remove(requestId);
                if (holder != null) {
                    assert holder.action().equals(action);
                    assert holder.connection().getNode().equals(node);
                    holder.handler()
                        .handleException(
                            new ReceiveTimeoutTransportException(
                                holder.connection().getNode(),
                                holder.action(),
                                "request_id [" + requestId + "] timed out after [" + (timeoutTime - sentTime) + "ms]"
                            )
                        );
                } else {
                    // response was processed, remove timeout info.
                    timeoutInfoHandlers.remove(requestId);
                }
            }
        }

        /**
         * cancels timeout handling. this is a best effort only to avoid running it. remove the requestId from {@link #responseHandlers}
         * to make sure this doesn't run.
         */
        public void cancel() {
            assert responseHandlers.contains(requestId) == false
                : "cancel must be called after the requestId [" + requestId + "] has been removed from clientHandlers";
            var cancellable = this.cancellable;
            if (cancellable != null) {
                cancellable.cancel();
            }
        }

        @Override
        public String toString() {
            return "timeout handler for [" + requestId + "][" + action + "]";
        }

        private void scheduleTimeout(TimeValue timeout) {
            this.cancellable = threadPool.schedule(this, timeout, threadPool.generic());
        }
    }

    record TimeoutInfoHolder(DiscoveryNode node, String action, long sentTime, long timeoutTime) {}

    /**
     * This handler wrapper ensures that the response thread executes with the correct thread context. Before any of the handle methods
     * are invoked we restore the context.
     */
    public static final class ContextRestoreResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final TransportResponseHandler<T> delegate;
        private final Supplier<ThreadContext.StoredContext> contextSupplier;
        private volatile TimeoutHandler handler;

        public ContextRestoreResponseHandler(Supplier<ThreadContext.StoredContext> contextSupplier, TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
            this.contextSupplier = contextSupplier;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return delegate.read(in);
        }

        @Override
        public void handleResponse(T response) {
            var handler = this.handler;
            if (handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleResponse(response);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            var handler = this.handler;
            if (handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleException(exp);
            }
        }

        @Override
        public Executor executor() {
            return delegate.executor();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + delegate.toString() + "]";
        }

        void setTimeoutHandler(TimeoutHandler timeoutHandler) {
            this.handler = timeoutHandler;
        }

        // for tests
        TransportResponseHandler<T> unwrap() {
            return delegate;
        }
    }

    public static boolean isDirectResponseChannel(TransportChannel transportChannel) {
        return transportChannel instanceof DirectResponseChannel;
    }

    static class DirectResponseChannel implements TransportChannel {
        final DiscoveryNode localNode;
        private final String action;
        private final long requestId;
        final TransportService service;

        DirectResponseChannel(DiscoveryNode localNode, String action, long requestId, TransportService service) {
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.service = service;
        }

        @Override
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public void sendResponse(TransportResponse response) {
            service.onResponseSent(requestId, action);
            try (var shutdownBlock = service.pendingDirectHandlers.withRef()) {
                if (shutdownBlock == null) {
                    // already shutting down, the handler will be completed by sendRequestInternal or doStop
                    return;
                }
                final TransportResponseHandler<?> handler = service.responseHandlers.onResponseReceived(requestId, service);
                if (handler == null) {
                    // handler already completed, likely by a timeout which is logged elsewhere
                    return;
                }
                final var executor = handler.executor();
                if (executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
                    processResponse(handler, response);
                } else {
                    response.mustIncRef();
                    executor.execute(new ForkingResponseHandlerRunnable(handler, null) {
                        @Override
                        protected void doRun() {
                            processResponse(handler, response);
                        }

                        @Override
                        public void onAfter() {
                            response.decRef();
                        }

                        @Override
                        public String toString() {
                            return "delivery of response to [" + requestId + "][" + action + "]: " + response;
                        }
                    });
                }
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        protected void processResponse(TransportResponseHandler handler, TransportResponse response) {
            try {
                handler.handleResponse(response);
            } catch (Exception e) {
                processException(handler, wrapInRemote(new ResponseHandlerFailureTransportException(e)));
            }
        }

        @Override
        public void sendResponse(Exception exception) {
            service.onResponseSent(requestId, action, exception);
            try (var shutdownBlock = service.pendingDirectHandlers.withRef()) {
                if (shutdownBlock == null) {
                    // already shutting down, the handler will be completed by sendRequestInternal or doStop
                    return;
                }
                final TransportResponseHandler<?> handler = service.responseHandlers.onResponseReceived(requestId, service);
                if (handler == null) {
                    // handler already completed, likely by a timeout which is logged elsewhere
                    return;
                }
                final RemoteTransportException rtx = wrapInRemote(exception);
                final var executor = handler.executor();
                if (executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
                    processException(handler, rtx);
                } else {
                    executor.execute(new ForkingResponseHandlerRunnable(handler, rtx) {
                        @Override
                        protected void doRun() {
                            processException(handler, rtx);
                        }

                        @Override
                        public String toString() {
                            return "delivery of failure response to [" + requestId + "][" + action + "]: " + exception;
                        }
                    });
                }
            }
        }

        protected RemoteTransportException wrapInRemote(Exception e) {
            return e instanceof RemoteTransportException remoteTransportException
                ? remoteTransportException
                : new RemoteTransportException(localNode.getName(), localNode.getAddress(), action, e);
        }

        protected void processException(final TransportResponseHandler<?> handler, final RemoteTransportException rtx) {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error(() -> format("failed to handle exception for action [%s], handler [%s]", action, handler), e);
            }
        }

        @Override
        public String toString() {
            return Strings.format("DirectResponseChannel{req=%d}{%s}", requestId, action);
        }
    }

    /**
     * Returns the internal thread pool
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    private boolean isLocalNode(DiscoveryNode discoveryNode) {
        if (discoveryNode == null) {
            throw new NodeNotConnectedException(discoveryNode, "discovery node must not be null");
        }
        return discoveryNode.equals(localNode);
    }

    private static class PendingDirectHandlers extends AbstractRefCounted {

        // To handle a response we (i) remove the handler from responseHandlers and then (ii) enqueue an action to complete the handler on
        // the target executor. Once step (i) succeeds then the handler won't be completed by any other mechanism, but if the target
        // executor is stopped then step (ii) will fail with an EsRejectedExecutionException which means the handler leaks.
        //
        // We wait for all transport threads to finish before stopping any executors, so a transport thread will never fail at step (ii).
        // Remote responses are always delivered on transport threads so there's no problem there, but direct responses may be delivered on
        // a non-transport thread which runs concurrently to the stopping of the transport service. This means we need this explicit
        // mechanism to block the shutdown of the transport service while there are direct handlers in between steps (i) and (ii).

        private final CountDownLatch countDownLatch = new CountDownLatch(1);

        @Override
        protected void closeInternal() {
            countDownLatch.countDown();
        }

        void stop() {
            decRef();
            try {
                final boolean completed = countDownLatch.await(30, TimeUnit.SECONDS);
                assert completed : "timed out waiting for all direct handlers to be enqueued";
            } catch (InterruptedException e) {
                assert false : e;
                Thread.currentThread().interrupt();
            }
        }

        @Nullable
        Releasable withRef() {
            if (tryIncRef()) {
                return this::decRef;
            } else {
                return null;
            }
        }
    }

    private record UnregisterChildTransportResponseHandler<T extends TransportResponse>(
        Releasable unregisterChildNode,
        TransportResponseHandler<T> handler,
        String action,
        TransportRequest childRequest,
        Transport.Connection childConnection,
        TaskManager taskManager
    ) implements TransportResponseHandler<T> {

        @Override
        public void handleResponse(T response) {
            unregisterChildNode.close();
            handler.handleResponse(response);
        }

        @Override
        public void handleException(TransportException exp) {
            assert childRequest.getParentTask().isSet();
            taskManager.cancelChildRemote(childRequest.getParentTask(), childRequest.getRequestId(), childConnection, exp.toString());

            unregisterChildNode.close();
            handler.handleException(exp);
        }

        @Override
        public Executor executor() {
            return handler.executor();
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return handler.read(in);
        }
    }
}
