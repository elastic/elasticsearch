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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.node.ReportingService;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;

/**
 * Basic service for accessing remote clusters via gateway nodes
 */
public final class RemoteClusterService extends RemoteClusterAware
    implements
        Closeable,
        ReportingService<RemoteClusterServerInfo>,
        IndicesExpressionGrouper {

    private static final Logger logger = LogManager.getLogger(RemoteClusterService.class);

    public static final String REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME = "cluster:internal/remote_cluster/handshake";
    public static final String CONNECTION_ATTEMPT_FAILURES_COUNTER_NAME = "es.projects.linked.connections.error.total";

    private final boolean isRemoteClusterClient;
    private final boolean isSearchNode;
    private final boolean isStateless;
    private final boolean remoteClusterServerEnabled;

    public boolean isRemoteClusterServerEnabled() {
        return remoteClusterServerEnabled;
    }

    private final TransportService transportService;
    private final Map<ProjectId, Map<String, RemoteClusterConnection>> remoteClusters;
    private final RemoteClusterCredentialsManager remoteClusterCredentialsManager;
    private final ProjectResolver projectResolver;
    private final boolean crossProjectEnabled;

    RemoteClusterService(Settings settings, TransportService transportService, ProjectResolver projectResolver) {
        super(settings);
        this.isRemoteClusterClient = DiscoveryNode.isRemoteClusterClient(settings);
        this.isSearchNode = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        this.isStateless = DiscoveryNode.isStateless(settings);
        this.remoteClusterServerEnabled = REMOTE_CLUSTER_SERVER_ENABLED.get(settings);
        this.transportService = transportService;
        this.projectResolver = projectResolver;
        this.remoteClusters = projectResolver.supportsMultipleProjects()
            ? ConcurrentCollections.newConcurrentMap()
            : Map.of(ProjectId.DEFAULT, ConcurrentCollections.newConcurrentMap());
        this.remoteClusterCredentialsManager = new RemoteClusterCredentialsManager(settings);
        if (remoteClusterServerEnabled) {
            registerRemoteClusterHandshakeRequestHandler(transportService);
        }
        /*
         * TODO: This is not the right way to check if we're in CPS context and is more of a temporary measure since
         *  the functionality to do it the right way is not yet ready -- replace this code when it's ready.
         */
        this.crossProjectEnabled = settings.getAsBoolean("serverless.cross_project.enabled", false);
        transportService.getTelemetryProvider()
            .getMeterRegistry()
            .registerLongCounter(CONNECTION_ATTEMPT_FAILURES_COUNTER_NAME, "linked project connection attempt failure count", "count");
    }

    public RemoteClusterCredentialsManager getRemoteClusterCredentialsManager() {
        return remoteClusterCredentialsManager;
    }

    /**
     * Group indices by cluster alias mapped to OriginalIndices for that cluster.
     * @param remoteClusterNames Set of configured remote cluster names.
     * @param indicesOptions IndicesOptions to clarify how the index expressions should be parsed/applied
     * @param indices Multiple index expressions as string[].
     * @param returnLocalAll whether to support the _all functionality needed by _search
     *        (See https://github.com/elastic/elasticsearch/pull/33899). If true, and no indices are specified,
     *        then a Map with one entry for the local cluster with an empty index array is returned.
     *        If false, an empty map is returned when no indices are specified.
     * @return Map keyed by cluster alias having OriginalIndices as the map value parsed from the String[] indices argument
     */
    public Map<String, OriginalIndices> groupIndices(
        Set<String> remoteClusterNames,
        IndicesOptions indicesOptions,
        String[] indices,
        boolean returnLocalAll
    ) {
        final Map<String, OriginalIndices> originalIndicesMap = new HashMap<>();
        final Map<String, List<String>> groupedIndices;
        /*
         * returnLocalAll is used to control whether we'd like to fallback to the local cluster.
         * While this is acceptable in a few cases, there are cases where we should not fallback to the local
         * cluster. Consider _resolve/cluster where the specified patterns do not match any remote clusters.
         * Falling back to the local cluster and returning its details in such cases is not ok. This is why
         * TransportResolveClusterAction sets returnLocalAll to false wherever it uses groupIndices().
         *
         * If such a fallback isn't allowed and the given indices match a pattern whose semantics mean that
         * it's ok to return an empty result (denoted via ["*", "-*"]), empty groupIndices.
         */
        if (returnLocalAll == false && IndexNameExpressionResolver.isNoneExpression(indices)) {
            groupedIndices = Map.of();
        } else {
            groupedIndices = groupClusterIndices(remoteClusterNames, indices);
        }

        if (groupedIndices.isEmpty()) {
            if (returnLocalAll) {
                // search on _all in the local cluster if neither local indices nor remote indices were specified
                originalIndicesMap.put(LOCAL_CLUSTER_GROUP_KEY, new OriginalIndices(Strings.EMPTY_ARRAY, indicesOptions));
            }
        } else {
            for (Map.Entry<String, List<String>> entry : groupedIndices.entrySet()) {
                String clusterAlias = entry.getKey();
                List<String> originalIndices = entry.getValue();
                originalIndicesMap.put(clusterAlias, new OriginalIndices(originalIndices.toArray(new String[0]), indicesOptions));
            }
        }
        return originalIndicesMap;
    }

    /**
     * If no indices are specified, then a Map with one entry for the local cluster with an empty index array is returned.
     * For details see {@code groupIndices(IndicesOptions indicesOptions, String[] indices, boolean returnLocalAll)}
     * @param remoteClusterNames Set of configured remote cluster names.
     * @param indicesOptions IndicesOptions to clarify how the index expressions should be parsed/applied
     * @param indices Multiple index expressions as string[].
     * @return Map keyed by cluster alias having OriginalIndices as the map value parsed from the String[] indices argument
     */
    public Map<String, OriginalIndices> groupIndices(Set<String> remoteClusterNames, IndicesOptions indicesOptions, String[] indices) {
        return groupIndices(remoteClusterNames, indicesOptions, indices, true);
    }

    @Override
    public Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String[] indices, boolean returnLocalAll) {
        return groupIndices(getRegisteredRemoteClusterNames(), indicesOptions, indices, returnLocalAll);
    }

    public Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String[] indices) {
        return groupIndices(getRegisteredRemoteClusterNames(), indicesOptions, indices, true);
    }

    /**
     * Returns the registered remote cluster names.
     */
    @FixForMultiProject(description = "Analyze use cases, determine possible need for cluster scoped and project scoped versions.")
    public Set<String> getRegisteredRemoteClusterNames() {
        return getConnectionsMapForCurrentProject().keySet();
    }

    /**
     * Returns the registered linked project aliases for the provided origin Project ID.
     */
    public Set<String> getRegisteredRemoteClusterNames(ProjectId originProjectId) {
        return getConnectionsMapForProject(originProjectId).keySet();
    }

    /**
     * Returns a connection to the given node on the given remote cluster
     *
     * @throws IllegalArgumentException if the remote cluster is unknown
     */
    public Transport.Connection getConnection(DiscoveryNode node, String cluster) {
        return getRemoteClusterConnection(cluster).getConnection(node);
    }

    /**
     * Ensures that the given cluster alias is connected. If the cluster is connected this operation
     * will invoke the listener immediately.
     */
    void ensureConnected(String clusterAlias, ActionListener<Void> listener) {
        final RemoteClusterConnection remoteClusterConnection;
        try {
            remoteClusterConnection = getRemoteClusterConnection(clusterAlias);
        } catch (NoSuchRemoteClusterException e) {
            listener.onFailure(e);
            return;
        }
        remoteClusterConnection.ensureConnected(listener);
    }

    /**
     * Returns whether the cluster identified by the provided alias is configured to be skipped when unavailable.
     * @param clusterAlias Name of the cluster
     * @return A boolean optional that denotes if the cluster is configured to be skipped. In CPS-like environment,
     * it returns an empty value where we default/fall back to true.
     */
    public Optional<Boolean> isSkipUnavailable(String clusterAlias) {
        if (crossProjectEnabled) {
            return Optional.empty();
        } else {
            return Optional.of(getRemoteClusterConnection(clusterAlias).isSkipUnavailable());
        }
    }

    public boolean crossProjectEnabled() {
        return crossProjectEnabled;
    }

    /**
     * Signifies if an error can be skipped for the specified cluster based on skip_unavailable, or,
     * allow_partial_search_results if in CPS-like environment.
     * @param clusterAlias Name of the cluster
     * @param allowPartialSearchResults If partial results can be served for the search request.
     * @return boolean
     */
    public boolean shouldSkipOnFailure(String clusterAlias, Boolean allowPartialSearchResults) {
        return isSkipUnavailable(clusterAlias).orElseGet(() -> allowPartialSearchResults != null && allowPartialSearchResults);
    }

    public Transport.Connection getConnection(String cluster) {
        return getRemoteClusterConnection(cluster).getConnection();
    }

    /**
     * Unlike {@link #getConnection(String)} this method might attempt to re-establish a remote connection if there is no connection
     * available before returning a connection to the remote cluster.
     *
     * @param clusterAlias    the remote cluster
     * @param ensureConnected whether requests should wait for a connection attempt when there isn't available connection
     * @param listener        a listener that will be notified the connection or failure
     */
    public void maybeEnsureConnectedAndGetConnection(
        String clusterAlias,
        boolean ensureConnected,
        ActionListener<Transport.Connection> listener
    ) {
        ActionListener<Void> ensureConnectedListener = listener.delegateFailureAndWrap(
            (l, nullValue) -> ActionListener.completeWith(l, () -> {
                try {
                    return getConnection(clusterAlias);
                } catch (ConnectTransportException e) {
                    if (ensureConnected == false) {
                        // trigger another connection attempt, but don't wait for it to complete
                        ensureConnected(clusterAlias, ActionListener.noop());
                    }
                    throw e;
                }
            })
        );
        if (ensureConnected) {
            ensureConnected(clusterAlias, ensureConnectedListener);
        } else {
            ensureConnectedListener.onResponse(null);
        }
    }

    public RemoteClusterConnection getRemoteClusterConnection(String cluster) {
        ensureClientIsEnabled();
        @FixForMultiProject(description = "Verify all callers will have the proper context set for resolving the origin project ID.")
        RemoteClusterConnection connection = getConnectionsMapForCurrentProject().get(cluster);
        if (connection == null) {
            throw new NoSuchRemoteClusterException(cluster);
        }
        return connection;
    }

    @Override
    public void updateLinkedProject(LinkedProjectConfig config) {
        final var projectId = config.originProjectId();
        final var clusterAlias = config.linkedProjectAlias();
        CountDownLatch latch = new CountDownLatch(1);
        updateRemoteCluster(config, false, ActionListener.runAfter(new ActionListener<>() {
            @Override
            public void onResponse(RemoteClusterConnectionStatus status) {
                logger.info("project [{}] remote cluster connection [{}] updated: {}", projectId, clusterAlias, status);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> "project [" + projectId + "] failed to update remote cluster connection [" + clusterAlias + "]", e);
            }
        }, latch::countDown));

        try {
            // Wait 10 seconds for a connections. We must use a latch instead of a future because we
            // are on the cluster state thread and our custom future implementation will throw an
            // assertion.
            if (latch.await(10, TimeUnit.SECONDS) == false) {
                logger.warn(
                    "project [{}] failed to update remote cluster connection [{}] within {}",
                    projectId,
                    clusterAlias,
                    TimeValue.timeValueSeconds(10)
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Adds, rebuilds, or closes and removes the connection for the specified remote cluster.
     *
     * @param config The linked project configuration.
     * @param forceRebuild Forces an existing connection to be closed and reconnected even if the connection strategy does not require it.
     * @param listener The listener invoked once the configured cluster has been connected.
     */
    public synchronized void updateRemoteCluster(
        LinkedProjectConfig config,
        boolean forceRebuild,
        ActionListener<RemoteClusterConnectionStatus> listener
    ) {
        final var projectId = config.originProjectId();
        final var clusterAlias = config.linkedProjectAlias();
        final var connectionMap = getConnectionsMapForProject(projectId);
        RemoteClusterConnection remote = connectionMap.get(clusterAlias);
        if (config.isConnectionEnabled() == false) {
            try {
                IOUtils.close(remote);
            } catch (IOException e) {
                logger.warn("project [" + projectId + "] failed to close remote cluster connections for cluster: " + clusterAlias, e);
            }
            connectionMap.remove(clusterAlias);
            listener.onResponse(RemoteClusterConnectionStatus.DISCONNECTED);
            return;
        }

        if (remote == null) {
            // this is a new cluster we have to add a new representation
            remote = new RemoteClusterConnection(config, transportService, remoteClusterCredentialsManager, crossProjectEnabled);
            connectionMap.put(clusterAlias, remote);
            remote.ensureConnected(listener.map(ignored -> RemoteClusterConnectionStatus.CONNECTED));
        } else if (forceRebuild || remote.shouldRebuildConnection(config)) {
            // Changes to connection configuration. Must tear down existing connection
            try {
                IOUtils.close(remote);
            } catch (IOException e) {
                logger.warn("project [" + projectId + "] failed to close remote cluster connections for cluster: " + clusterAlias, e);
            }
            connectionMap.remove(clusterAlias);
            remote = new RemoteClusterConnection(config, transportService, remoteClusterCredentialsManager, crossProjectEnabled);
            connectionMap.put(clusterAlias, remote);
            remote.ensureConnected(listener.map(ignored -> RemoteClusterConnectionStatus.RECONNECTED));
        } else if (remote.isSkipUnavailable() != config.skipUnavailable()) {
            remote.setSkipUnavailable(config.skipUnavailable());
            listener.onResponse(RemoteClusterConnectionStatus.UPDATED);
        } else {
            // No changes to connection configuration.
            listener.onResponse(RemoteClusterConnectionStatus.UNCHANGED);
        }
    }

    public enum RemoteClusterConnectionStatus {
        CONNECTED,
        DISCONNECTED,
        RECONNECTED,
        UNCHANGED,
        UPDATED
    }

    /**
     * Connects to all remote clusters in a blocking fashion. This should be called on node startup to establish an initial connection
     * to all configured seed nodes.
     */
    void initializeRemoteClusters(Collection<LinkedProjectConfig> configs) {
        if (configs.isEmpty()) {
            return;
        }

        @FixForMultiProject(description = "Refactor for initializing connections to linked projects for each origin project supported.")
        final var projectId = projectResolver.getProjectId();
        final TimeValue timeValue = RemoteClusterSettings.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(settings);
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        CountDownActionListener listener = new CountDownActionListener(configs.size(), future);
        for (LinkedProjectConfig config : configs) {
            updateRemoteCluster(config, false, listener.map(ignored -> null));
        }

        try {
            future.get(timeValue.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException ex) {
            logger.warn("project [{}] failed to connect to remote clusters within {}", projectId, timeValue.toString());
        } catch (Exception e) {
            logger.warn("project [" + projectId + "] failed to connect to remote clusters", e);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(remoteClusters.values().stream().flatMap(map -> map.values().stream()).collect(Collectors.toList()));
    }

    @FixForMultiProject(description = "Analyze use cases, determine possible need for cluster scoped and project scoped versions.")
    public Stream<RemoteConnectionInfo> getRemoteConnectionInfos() {
        return getConnectionsMapForCurrentProject().values().stream().map(RemoteClusterConnection::getConnectionInfo);
    }

    @Override
    public RemoteClusterServerInfo info() {
        if (remoteClusterServerEnabled) {
            return new RemoteClusterServerInfo(transportService.boundRemoteAccessAddress());
        } else {
            return null;
        }
    }

    /**
     * Collects all nodes of the given clusters and returns / passes a (clusterAlias, nodeId) to {@link DiscoveryNode}
     * function on success.
     */
    public void collectNodes(Set<String> clusters, ActionListener<BiFunction<String, String, DiscoveryNode>> listener) {
        ensureClientIsEnabled();
        @FixForMultiProject(description = "Analyze usages and determine if the project ID must be provided.")
        final var projectConnectionsMap = getConnectionsMapForCurrentProject();
        final var connectionsMap = new HashMap<String, RemoteClusterConnection>();
        for (String cluster : clusters) {
            final var connection = projectConnectionsMap.get(cluster);
            if (connection == null) {
                listener.onFailure(new NoSuchRemoteClusterException(cluster));
                return;
            }
            connectionsMap.put(cluster, connection);
        }

        final Map<String, Function<String, DiscoveryNode>> clusterMap = new HashMap<>();
        final var finalListener = listener.<Void>safeMap(
            ignored -> (clusterAlias, nodeId) -> clusterMap.getOrDefault(clusterAlias, s -> null).apply(nodeId)
        );
        try (var refs = new RefCountingListener(finalListener)) {
            connectionsMap.forEach((cluster, connection) -> connection.collectNodes(refs.acquire(nodeLookup -> {
                synchronized (clusterMap) {
                    clusterMap.put(cluster, nodeLookup);
                }
            })));
        }
    }

    /**
     * Specifies how to behave when executing a request against a disconnected remote cluster.
     */
    public enum DisconnectedStrategy {
        /**
         * Always try and reconnect before executing a request, waiting for {@link TransportSettings#CONNECT_TIMEOUT} before failing if the
         * remote cluster is totally unresponsive.
         */
        RECONNECT_IF_DISCONNECTED,

        /**
         * Fail the request immediately if the remote cluster is disconnected (but also trigger another attempt to reconnect to the remote
         * cluster in the background so that the next request might succeed).
         */
        FAIL_IF_DISCONNECTED,

        /**
         * Behave according to the {@link RemoteClusterSettings#REMOTE_CLUSTER_SKIP_UNAVAILABLE} setting for this remote cluster: if this
         * setting is {@code false} (the default) then behave like {@link #RECONNECT_IF_DISCONNECTED}, but if it is {@code true} then behave
         * like {@link #FAIL_IF_DISCONNECTED}.
         */
        RECONNECT_UNLESS_SKIP_UNAVAILABLE
    }

    /**
     * Returns a client to the remote cluster if the given cluster alias exists.
     *
     * @param clusterAlias         the cluster alias the remote cluster is registered under
     * @param responseExecutor     the executor to use to process the response
     * @param disconnectedStrategy how to handle the situation where the remote cluster is disconnected when executing a request
     * @throws IllegalArgumentException if the given clusterAlias doesn't exist
     */
    public RemoteClusterClient getRemoteClusterClient(
        String clusterAlias,
        Executor responseExecutor,
        DisconnectedStrategy disconnectedStrategy
    ) {
        ensureClientIsEnabled();
        if (transportService.getRemoteClusterService().getRegisteredRemoteClusterNames().contains(clusterAlias) == false) {
            throw new NoSuchRemoteClusterException(clusterAlias);
        }
        return new RemoteClusterAwareClient(transportService, clusterAlias, responseExecutor, switch (disconnectedStrategy) {
            case RECONNECT_IF_DISCONNECTED -> true;
            case FAIL_IF_DISCONNECTED -> false;
            case RECONNECT_UNLESS_SKIP_UNAVAILABLE -> transportService.getRemoteClusterService()
                .isSkipUnavailable(clusterAlias)
                .orElse(true) == false;
        });
    }

    /**
     * Verifies this node is configured to support linked project client operations.
     * @throws IllegalArgumentException If this node is not configured to support client operations.
     */
    public void ensureClientIsEnabled() {
        if (isRemoteClusterClient) {
            return;
        }
        if (isStateless == false) {
            throw new IllegalArgumentException(
                "node [" + getNodeName() + "] does not have the [" + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + "] role"
            );
        }
        // For stateless the remote cluster client is enabled by default for search nodes,
        // REMOTE_CLUSTER_CLIENT_ROLE is not explicitly required.
        if (isSearchNode == false) {
            throw new IllegalArgumentException(
                "node ["
                    + getNodeName()
                    + "] must have the ["
                    + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName()
                    + "] role or the ["
                    + DiscoveryNodeRole.SEARCH_ROLE.roleName()
                    + "] role in stateless environments to use linked project client features"
            );
        }
    }

    static void registerRemoteClusterHandshakeRequestHandler(TransportService transportService) {
        transportService.registerRequestHandler(
            REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            false,
            false,
            TransportService.HandshakeRequest::new,
            (request, channel, task) -> {
                if (false == RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE.equals(channel.getProfileName())) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            "remote cluster handshake action requires channel profile to be [%s], but got [%s]",
                            RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE,
                            channel.getProfileName()
                        )
                    );
                }
                logger.trace("handling remote cluster handshake request");
                channel.sendResponse(
                    new TransportService.HandshakeResponse(
                        transportService.getLocalNode().getVersion(),
                        Build.current().hash(),
                        transportService.getLocalNode().withTransportAddress(transportService.boundRemoteAccessAddress().publishAddress()),
                        transportService.clusterName
                    )
                );
            }
        );
    }

    /**
     * Returns the map of connections for the {@link ProjectId} currently returned by the {@link ProjectResolver}.
     */
    private Map<String, RemoteClusterConnection> getConnectionsMapForCurrentProject() {
        return getConnectionsMapForProject(projectResolver.getProjectId());
    }

    /**
     * Returns the map of connections for the given {@link ProjectId}.
     */
    @FixForMultiProject(description = "Assert ProjectId.DEFAULT should not be used in multi-project environment")
    private Map<String, RemoteClusterConnection> getConnectionsMapForProject(ProjectId projectId) {
        if (projectResolver.supportsMultipleProjects()) {
            return remoteClusters.computeIfAbsent(projectId, unused -> ConcurrentCollections.newConcurrentMap());
        }
        assert ProjectId.DEFAULT.equals(projectId) : "Only the default project ID should be used when multiple projects are not supported";
        return remoteClusters.get(projectId);
    }
}
