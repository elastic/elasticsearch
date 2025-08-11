/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class TransportService extends AbstractLifecycleComponent
    implements
        ReportingService<TransportInfo>,
        TransportMessageListener,
        TransportConnectionListener {

    private static final Logger logger = LogManager.getLogger(TransportService.class);

    public static final String PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY = "es.unsafely_permit_handshake_from_incompatible_builds";
    private static final boolean PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS;

    static {
        final String value = System.getProperty(PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY);
        if (value == null) {
            PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS = false;
        } else if (Boolean.parseBoolean(value)) {
            PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS = true;
        } else {
            throw new IllegalArgumentException(
                "invalid value [" + value + "] for system property [" + PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY + "]"
            );
        }
    }

    public static final String DIRECT_RESPONSE_PROFILE = ".direct";
    public static final String HANDSHAKE_ACTION_NAME = "internal:transport/handshake";

    private final AtomicBoolean handleIncomingRequests = new AtomicBoolean();
    private final DelegatingTransportMessageListener messageListener = new DelegatingTransportMessageListener();
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

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers = Collections.synchronizedMap(
        new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, TimeoutInfoHolder> eldest) {
                return size() > 100;
            }
        }
    );

    public static final TransportInterceptor NOOP_TRANSPORT_INTERCEPTOR = new TransportInterceptor() {
    };

    // tracer log

    private final Logger tracerLog;

    volatile String[] tracerLogInclude;
    volatile String[] tracerLogExclude;

    private final RemoteClusterService remoteClusterService;

    private final boolean validateConnections;
    private final boolean requireCompatibleBuild;

    /** if set will call requests sent to this id to shortcut and executed locally */
    volatile DiscoveryNode localNode = null;
    private final Transport.Connection localNodeConnection = new Transport.Connection() {
        @Override
        public DiscoveryNode getNode() {
            return localNode;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {
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

    /**
     * Build the service.
     *
     * @param clusterSettings if non null, the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
    *   *    updates for {@link TransportSettings#TRACE_LOG_EXCLUDE_SETTING} and {@link TransportSettings#TRACE_LOG_INCLUDE_SETTING}.
     */
    public TransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor transportInterceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders
    ) {
        this(
            settings,
            transport,
            threadPool,
            transportInterceptor,
            localNodeFactory,
            clusterSettings,
            taskHeaders,
            new ClusterConnectionManager(settings, transport, threadPool.getThreadContext())
        );
    }

    public TransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor transportInterceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders,
        ConnectionManager connectionManager
    ) {

        final boolean isTransportClient = TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));

        // If we are a transport client then we skip the check that the remote node has a compatible build hash
        this.requireCompatibleBuild = isTransportClient == false;

        // The only time we do not want to validate node connections is when this is a transport client using the simple node sampler
        this.validateConnections = isTransportClient == false || TransportClient.CLIENT_TRANSPORT_SNIFF.get(settings);

        this.transport = transport;
        transport.setSlowLogThreshold(TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING.get(settings));
        this.threadPool = threadPool;
        this.localNodeFactory = localNodeFactory;
        this.connectionManager = connectionManager;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        setTracerLogInclude(TransportSettings.TRACE_LOG_INCLUDE_SETTING.get(settings));
        setTracerLogExclude(TransportSettings.TRACE_LOG_EXCLUDE_SETTING.get(settings));
        tracerLog = Loggers.getLogger(logger, ".tracer");
        taskManager = createTaskManager(settings, threadPool, taskHeaders);
        this.interceptor = transportInterceptor;
        this.asyncSender = interceptor.interceptSender(this::sendRequestInternal);
        this.remoteClusterClient = DiscoveryNode.isRemoteClusterClient(settings);
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
            ThreadPool.Names.SAME,
            false,
            false,
            HandshakeRequest::new,
            (request, channel, task) -> channel.sendResponse(
                new HandshakeResponse(localNode.getVersion(), Build.CURRENT.hash(), localNode, clusterName)
            )
        );

        if (PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS) {
            logger.warn(
                "transport handshakes from incompatible builds are unsafely permitted on this node; remove system property ["
                    + PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY
                    + "] to resolve this warning"
            );
            DeprecationLogger.getLogger(TransportService.class)
                .critical(
                    DeprecationCategory.OTHER,
                    "permit_handshake_from_incompatible_builds",
                    "system property [" + PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY + "] is deprecated and should be removed"
                );
        }
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

    protected TaskManager createTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
        return new TaskManager(settings, threadPool, taskHeaders);
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
            IOUtils.close(connectionManager, remoteClusterService, transport::stop);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            // in case the transport is not connected to our local node (thus cleaned on node disconnect)
            // make sure to clean any leftover on going handles
            for (final Transport.ResponseContext<?> holderToNotify : responseHandlers.prune(h -> true)) {
                try {
                    holderToNotify.handler()
                        .handleException(
                            new SendRequestTransportException(
                                holderToNotify.connection().getNode(),
                                holderToNotify.action(),
                                new NodeClosedException(localNode)
                            )
                        );
                } catch (Exception e) {
                    assert false : e;
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "failed to notify response handler on exception, action: {}",
                            holderToNotify.action()
                        ),
                        e
                    );
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
     *
     * The transport service starts before it's ready to accept incoming requests because we need to know the address(es) to which we are
     * bound, which means we have to actually bind to them and start accepting incoming connections. However until this method is called we
     * reject any incoming requests, including handshakes, by closing the connection.
     */
    public final void acceptIncomingRequests() {
        final boolean startedWithThisCall = handleIncomingRequests.compareAndSet(false, true);
        assert startedWithThisCall : "transport service was already accepting incoming requests";
        logger.debug("now accepting incoming requests");
    }

    @Override
    public TransportInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new TransportInfo(boundTransportAddress, transport.profileBoundAddresses());
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
     * Connect to the specified node with the default connection profile
     *
     * @param node the node to connect to
     */
    public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
        connectToNode(node, (ConnectionProfile) null);
    }

    /**
     * Connect to the specified node with the given connection profile
     *
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use when connecting to this node
     */
    public void connectToNode(final DiscoveryNode node, ConnectionProfile connectionProfile) {
        PlainActionFuture.get(fut -> connectToNode(node, connectionProfile, fut.map(x -> null)));
    }

    /**
     * Connect to the specified node with the given connection profile.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param node the node to connect to
     * @param listener the action listener to notify
     */
    public void connectToNode(DiscoveryNode node, ActionListener<Releasable> listener) throws ConnectTransportException {
        connectToNode(node, null, listener);
    }

    /**
     * Connect to the specified node with the given connection profile.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use when connecting to this node
     * @param listener the action listener to notify
     */
    public void connectToNode(final DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Releasable> listener) {
        if (isLocalNode(node)) {
            listener.onResponse(null);
            return;
        }
        connectionManager.connectToNode(node, connectionProfile, connectionValidator(node), listener);
    }

    public ConnectionManager.ConnectionValidator connectionValidator(DiscoveryNode node) {
        return (newConnection, actualProfile, listener) -> {
            // We don't validate cluster names to allow for CCS connections.
            handshake(newConnection, actualProfile.getHandshakeTimeout(), cn -> true, listener.map(resp -> {
                final DiscoveryNode remote = resp.discoveryNode;
                if (validateConnections && node.equals(remote) == false) {
                    throw new ConnectTransportException(node, "handshake failed. unexpected remote node " + remote);
                }
                return null;
            }));
        };
    }

    /**
     * Establishes and returns a new connection to the given node. The connection is NOT maintained by this service, it's the callers
     * responsibility to close the connection once it goes out of scope.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use
     */
    public Transport.Connection openConnection(final DiscoveryNode node, ConnectionProfile connectionProfile) {
        return PlainActionFuture.get(fut -> openConnection(node, connectionProfile, fut));
    }

    /**
     * Establishes a new connection to the given node. The connection is NOT maintained by this service, it's the callers
     * responsibility to close the connection once it goes out of scope.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use
     * @param listener the action listener to notify
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
     * @throws IllegalStateException if the handshake failed
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
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @param clusterNamePredicate cluster name validation predicate
     * @param listener         action listener to notify
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
            }), in -> new HandshakeResponse(in, requireCompatibleBuild), ThreadPool.Names.GENERIC)
        );
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    static class HandshakeRequest extends TransportRequest {

        public static final HandshakeRequest INSTANCE = new HandshakeRequest();

        HandshakeRequest(StreamInput in) throws IOException {
            super(in);
        }

        private HandshakeRequest() {}

    }

    public static class HandshakeResponse extends TransportResponse {

        private static final Version BUILD_HASH_HANDSHAKE_VERSION = Version.V_7_11_0;

        private final Version version;

        @Nullable // if version < BUILD_HASH_HANDSHAKE_VERSION
        private final String buildHash;

        private final DiscoveryNode discoveryNode;

        private final ClusterName clusterName;

        public HandshakeResponse(Version version, String buildHash, DiscoveryNode discoveryNode, ClusterName clusterName) {
            this.buildHash = Objects.requireNonNull(buildHash);
            this.discoveryNode = Objects.requireNonNull(discoveryNode);
            this.version = Objects.requireNonNull(version);
            this.clusterName = Objects.requireNonNull(clusterName);
        }

        public HandshakeResponse(StreamInput in, boolean requireCompatibleBuild) throws IOException {
            super(in);
            if (in.getVersion().onOrAfter(BUILD_HASH_HANDSHAKE_VERSION)) {
                // the first two fields need only VInts and raw (ASCII) characters, so we cross our fingers and hope that they appear
                // on the wire as we expect them to even if this turns out to be an incompatible build
                version = Version.readVersion(in);
                buildHash = in.readString();

                try {
                    // If the remote node is incompatible then make an effort to identify it anyway, so we can mention it in the exception
                    // message, but recognise that this may fail
                    discoveryNode = new DiscoveryNode(in);
                } catch (Exception e) {
                    if (isIncompatibleBuild(version, buildHash, requireCompatibleBuild)) {
                        throw new IllegalArgumentException(
                            "unidentifiable remote node is build ["
                                + buildHash
                                + "] of version ["
                                + version
                                + "] but this node is build ["
                                + Build.CURRENT.hash()
                                + "] of version ["
                                + Version.CURRENT
                                + "] which has an incompatible wire format",
                            e
                        );
                    } else {
                        throw e;
                    }
                }

                if (isIncompatibleBuild(version, buildHash, requireCompatibleBuild)) {
                    if (PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS) {
                        logger.warn(
                            "remote node [{}] is build [{}] of version [{}] but this node is build [{}] of version [{}] "
                                + "which may not be compatible; remove system property [{}] to resolve this warning",
                            discoveryNode,
                            buildHash,
                            version,
                            Build.CURRENT.hash(),
                            Version.CURRENT,
                            PERMIT_HANDSHAKES_FROM_INCOMPATIBLE_BUILDS_KEY
                        );
                    } else {
                        throw new IllegalArgumentException(
                            "remote node ["
                                + discoveryNode
                                + "] is build ["
                                + buildHash
                                + "] of version ["
                                + version
                                + "] but this node is build ["
                                + Build.CURRENT.hash()
                                + "] of version ["
                                + Version.CURRENT
                                + "] which has an incompatible wire format"
                        );
                    }
                }

                clusterName = new ClusterName(in);
            } else {
                discoveryNode = in.readOptionalWriteable(DiscoveryNode::new);
                clusterName = new ClusterName(in);
                version = Version.readVersion(in);
                buildHash = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(BUILD_HASH_HANDSHAKE_VERSION)) {
                Version.writeVersion(version, out);
                out.writeString(buildHash);
                discoveryNode.writeTo(out);
                clusterName.writeTo(out);
            } else {
                out.writeOptionalWriteable(discoveryNode);
                clusterName.writeTo(out);
                Version.writeVersion(version, out);
            }
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

        private static boolean isIncompatibleBuild(Version version, String buildHash, boolean requireCompatibleBuild) {
            return requireCompatibleBuild && version == Version.CURRENT && Build.CURRENT.hash().equals(buildHash) == false;
        }
    }

    public void disconnectFromNode(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return;
        }
        connectionManager.disconnectFromNode(node);
    }

    public void addMessageListener(TransportMessageListener listener) {
        messageListener.listeners.add(listener);
    }

    public boolean removeMessageListener(TransportMessageListener listener) {
        return messageListener.listeners.remove(listener);
    }

    public void addConnectionListener(TransportConnectionListener listener) {
        connectionManager.addListener(listener);
    }

    public void removeConnectionListener(TransportConnectionListener listener) {
        connectionManager.removeListener(listener);
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(
        DiscoveryNode node,
        String action,
        TransportRequest request,
        TransportResponseHandler<T> handler
    ) throws TransportException {
        return submitRequest(node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(
        DiscoveryNode node,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<T> handler
    ) throws TransportException {
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<>(handler);
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, futureHandler);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            futureHandler.handleException(ex);
        }
        return futureHandler;
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
        final Transport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (final NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendRequest(connection, action, request, options, handler);
    }

    /**
     * Unwraps and returns the actual underlying connection of the given connection.
     */
    public static Transport.Connection unwrapConnection(Transport.Connection connection) {
        Transport.Connection unwrapped = connection;
        while (unwrapped instanceof RemoteConnectionManager.ProxyConnection) {
            unwrapped = ((RemoteConnectionManager.ProxyConnection) unwrapped).getConnection();
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
                delegate = new TransportResponseHandler<T>() {
                    @Override
                    public void handleResponse(T response) {
                        unregisterChildNode.close();
                        handler.handleResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        unregisterChildNode.close();
                        handler.handleException(exp);
                    }

                    @Override
                    public String executor() {
                        return handler.executor();
                    }

                    @Override
                    public T read(StreamInput in) throws IOException {
                        return handler.read(in);
                    }

                    @Override
                    public String toString() {
                        return getClass().getName() + "/[" + action + "]:" + handler.toString();
                    }
                };
            } else {
                delegate = handler;
            }
            asyncSender.sendRequest(connection, action, request, options, delegate);
        } catch (final Exception ex) {
            // the caller might not handle this so we invoke the handler
            final TransportException te;
            if (ex instanceof TransportException) {
                te = (TransportException) ex;
            } else {
                te = new SendRequestTransportException(connection.getNode(), action, ex);
            }
            handler.handleException(te);
        }
    }

    /**
     * Returns either a real transport connection or a local node connection if we are using the local node optimization.
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
        final Transport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (final NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendChildRequest(connection, action, request, parentTask, options, handler);
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
        // TODO we can probably fold this entire request ID dance into connection.sendReqeust but it will be a bigger refactoring
        final long requestId = responseHandlers.add(new Transport.ResponseContext<>(responseHandler, connection, action));
        final TimeoutHandler timeoutHandler;
        if (options.timeout() != null) {
            timeoutHandler = new TimeoutHandler(requestId, connection.getNode(), action);
            responseHandler.setTimeoutHandler(timeoutHandler);
        } else {
            timeoutHandler = null;
        }
        try {
            if (lifecycle.stoppedOrClosed()) {
                /*
                 * If we are not started the exception handling will remove the request holder again and calls the handler to notify the
                 * caller. It will only notify if toStop hasn't done the work yet.
                 */
                throw new NodeClosedException(localNode);
            }
            if (timeoutHandler != null) {
                assert options.timeout() != null;
                timeoutHandler.scheduleTimeout(options.timeout());
            }
            connection.sendRequest(requestId, action, request, options); // local node optimization happens upstream
        } catch (final Exception e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            final Transport.ResponseContext<? extends TransportResponse> contextToNotify = responseHandlers.remove(requestId);
            // If holderToNotify == null then handler has already been taken care of.
            if (contextToNotify != null) {
                if (timeoutHandler != null) {
                    timeoutHandler.cancel();
                }
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows. In the special case of running into a closing node we run on the current
                // thread on a best effort basis though.
                final SendRequestTransportException sendRequestException = new SendRequestTransportException(node, action, e);
                final String executor = lifecycle.stoppedOrClosed() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC;
                threadPool.executor(executor).execute(new AbstractRunnable() {
                    @Override
                    public void onRejection(Exception e) {
                        // if we get rejected during node shutdown we don't wanna bubble it up
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on rejection, action: {}",
                                contextToNotify.action()
                            ),
                            e
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on exception, action: {}",
                                contextToNotify.action()
                            ),
                            e
                        );
                    }

                    @Override
                    protected void doRun() throws Exception {
                        contextToNotify.handler().handleException(sendRequestException);
                    }
                });
            } else {
                logger.debug("Exception while sending request, handler likely already notified due to timeout", e);
            }
        }
    }

    private void sendLocalRequest(long requestId, final String action, final TransportRequest request, TransportRequestOptions options) {
        final DirectResponseChannel channel = new DirectResponseChannel(localNode, action, requestId, this, threadPool);
        try {
            onRequestSent(localNode, requestId, action, request, options);
            onRequestReceived(requestId, action);
            @SuppressWarnings("unchecked")
            final RequestHandlerRegistry<TransportRequest> reg = (RequestHandlerRegistry<TransportRequest>) getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final String executor = reg.getExecutor();
            if (ThreadPool.Names.SAME.equals(executor)) {
                reg.processMessageReceived(request, channel);
            } else {
                boolean success = false;
                request.incRef();
                try {
                    threadPool.executor(executor).execute(new AbstractRunnable() {
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
                            try {
                                channel.sendResponse(e);
                            } catch (Exception inner) {
                                inner.addSuppressed(e);
                                logger.warn(
                                    () -> new ParameterizedMessage("failed to notify channel of error message for action [{}]", action),
                                    inner
                                );
                            }
                        }

                        @Override
                        public String toString() {
                            return "processing of [" + requestId + "][" + action + "]: " + request;
                        }

                        @Override
                        public void onAfter() {
                            request.decRef();
                        }
                    });
                    success = true;
                } finally {
                    if (success == false) {
                        request.decRef();
                    }
                }
            }

        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.warn(() -> new ParameterizedMessage("failed to notify channel of error message for action [{}]", action), inner);
            }
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
    public static final Set<String> VALID_ACTION_PREFIXES = Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                "indices:admin",
                "indices:monitor",
                "indices:data/write",
                "indices:data/read",
                "indices:internal",
                "cluster:admin",
                "cluster:monitor",
                "cluster:internal",
                "internal:"
            )
        )
    );

    private void validateActionName(String actionName) {
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
     * @param action         The action the request handler is associated with
     * @param requestReader  a callable to be used construct new instances for streaming
     * @param executor       The executor the request handling will be executed on
     * @param handler        The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(
        String action,
        String executor,
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
            true
        );
        transport.registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
     *
     * @param action                The action the request handler is associated with
     * @param requestReader               The request class that will be used to construct new instances for streaming
     * @param executor              The executor the request handling will be executed on
     * @param forceExecution        Force execution on the executor queue and never reject it
     * @param canTripCircuitBreaker Check the request size and raise an exception in case the limit is breached.
     * @param handler               The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(
        String action,
        String executor,
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
            canTripCircuitBreaker
        );
        transport.registerRequestHandler(reg);
    }

    /**
     * called by the {@link Transport} implementation when an incoming request arrives but before
     * any parsing of it has happened (with the exception of the requestId and action)
     */
    @Override
    public void onRequestReceived(long requestId, String action) {
        if (handleIncomingRequests.get() == false) {
            throw new TransportNotReadyException();
        }
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] received request", requestId, action);
        }
        messageListener.onRequestReceived(requestId, action);
    }

    /** called by the {@link Transport} implementation once a request has been sent */
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
        messageListener.onRequestSent(node, requestId, action, request, options);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void onResponseReceived(long requestId, Transport.ResponseContext holder) {
        if (holder == null) {
            checkForTimeout(requestId);
        } else if (tracerLog.isTraceEnabled() && shouldTraceAction(holder.action())) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, holder.action(), holder.connection().getNode());
        }
        messageListener.onResponseReceived(requestId, holder);
    }

    /** called by the {@link Transport} implementation once a response was sent to calling node */
    @Override
    public void onResponseSent(long requestId, String action, TransportResponse response) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] sent response", requestId, action);
        }
        messageListener.onResponseSent(requestId, action, response);
    }

    /** called by the {@link Transport} implementation after an exception was sent as a response to an incoming request */
    @Override
    public void onResponseSent(long requestId, String action, Exception e) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace(() -> new ParameterizedMessage("[{}][{}] sent error response", requestId, action), e);
        }
        messageListener.onResponseSent(requestId, action, e);
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

        // Callback that an exception happened, but on a different thread since we don't
        // want handlers to worry about stack overflows.
        // Execute on the current thread in the special case of a node shut down to notify the listener even when the threadpool has
        // already been shut down.
        final String executor = lifecycle.stoppedOrClosed() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC;
        threadPool.executor(executor).execute(new AbstractRunnable() {
            @Override
            public void doRun() {
                for (Transport.ResponseContext<?> holderToNotify : pruned) {
                    holderToNotify.handler().handleException(new NodeDisconnectedException(connection.getNode(), holderToNotify.action()));
                }
            }

            @Override
            public void onFailure(Exception e) {
                assert false : e;
                logger.warn(() -> new ParameterizedMessage("failed to notify response handler on connection close [{}]", connection), e);
            }

            @Override
            public String toString() {
                return "onConnectionClosed(" + connection.getNode() + ")";
            }
        });
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
            if (cancellable != null) {
                cancellable.cancel();
            }
        }

        @Override
        public String toString() {
            return "timeout handler for [" + requestId + "][" + action + "]";
        }

        private void scheduleTimeout(TimeValue timeout) {
            this.cancellable = threadPool.schedule(this, timeout, ThreadPool.Names.GENERIC);
        }
    }

    static class TimeoutInfoHolder {

        private final DiscoveryNode node;
        private final String action;
        private final long sentTime;
        private final long timeoutTime;

        TimeoutInfoHolder(DiscoveryNode node, String action, long sentTime, long timeoutTime) {
            this.node = node;
            this.action = action;
            this.sentTime = sentTime;
            this.timeoutTime = timeoutTime;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String action() {
            return action;
        }

        public long sentTime() {
            return sentTime;
        }

        public long timeoutTime() {
            return timeoutTime;
        }
    }

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
            if (handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleResponse(response);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            if (handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleException(exp);
            }
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate.toString();
        }

        void setTimeoutHandler(TimeoutHandler timeoutHandler) {
            this.handler = timeoutHandler;
        }

    }

    static class DirectResponseChannel implements TransportChannel {
        final DiscoveryNode localNode;
        private final String action;
        private final long requestId;
        final TransportService service;
        final ThreadPool threadPool;

        DirectResponseChannel(DiscoveryNode localNode, String action, long requestId, TransportService service, ThreadPool threadPool) {
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.service = service;
            this.threadPool = threadPool;
        }

        @Override
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            try {
                service.onResponseSent(requestId, action, response);
                final TransportResponseHandler<?> handler = service.responseHandlers.onResponseReceived(requestId, service);
                // ignore if its null, the service logs it
                if (handler != null) {
                    final String executor = handler.executor();
                    if (ThreadPool.Names.SAME.equals(executor)) {
                        processResponse(handler, response);
                    } else {
                        boolean success = false;
                        response.incRef();
                        try {
                            threadPool.executor(executor).execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        processResponse(handler, response);
                                    } finally {
                                        response.decRef();
                                    }
                                }

                                @Override
                                public String toString() {
                                    return "delivery of response to [" + requestId + "][" + action + "]: " + response;
                                }
                            });
                            success = true;
                        } finally {
                            if (success == false) {
                                response.decRef();
                            }
                        }
                    }
                }
            } finally {
                response.decRef();
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
        public void sendResponse(Exception exception) throws IOException {
            service.onResponseSent(requestId, action, exception);
            final TransportResponseHandler<?> handler = service.responseHandlers.onResponseReceived(requestId, service);
            // ignore if its null, the service logs it
            if (handler != null) {
                final RemoteTransportException rtx = wrapInRemote(exception);
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processException(handler, rtx);
                } else {
                    threadPool.executor(handler.executor()).execute(new Runnable() {
                        @Override
                        public void run() {
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
            if (e instanceof RemoteTransportException) {
                return (RemoteTransportException) e;
            }
            return new RemoteTransportException(localNode.getName(), localNode.getAddress(), action, e);
        }

        protected void processException(final TransportResponseHandler<?> handler, final RemoteTransportException rtx) {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error(
                    () -> new ParameterizedMessage("failed to handle exception for action [{}], handler [{}]", action, handler),
                    e
                );
            }
        }

        @Override
        public String getChannelType() {
            return "direct";
        }

        @Override
        public Version getVersion() {
            return localNode.getVersion();
        }
    }

    /**
     * Returns the internal thread pool
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    private boolean isLocalNode(DiscoveryNode discoveryNode) {
        return Objects.requireNonNull(discoveryNode, "discovery node must not be null").equals(localNode);
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
        public void onResponseSent(long requestId, String action, TransportResponse response) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseSent(requestId, action, response);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, Exception error) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseSent(requestId, action, error);
            }
        }

        @Override
        public void onRequestSent(
            DiscoveryNode node,
            long requestId,
            String action,
            TransportRequest request,
            TransportRequestOptions finalOptions
        ) {
            for (TransportMessageListener listener : listeners) {
                listener.onRequestSent(node, requestId, action, request, finalOptions);
            }
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void onResponseReceived(long requestId, Transport.ResponseContext holder) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseReceived(requestId, holder);
            }
        }
    }

}
