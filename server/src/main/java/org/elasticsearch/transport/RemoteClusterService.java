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
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.transport.RemoteClusterCredentialsManager.UpdateRemoteClusterCredentialsResult;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.enumSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;
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

    /**
     * The initial connect timeout for remote cluster connections
     */
    public static final Setting<TimeValue> REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "cluster.remote.initial_connect_timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    /**
     * The name of a node attribute to select nodes that should be connected to in the remote cluster.
     * For instance a node can be configured with {@code node.attr.gateway: true} in order to be eligible as a gateway node between
     * clusters. In that case {@code cluster.remote.node.attr: gateway} can be used to filter out other nodes in the remote cluster.
     * The value of the setting is expected to be a boolean, {@code true} for nodes that can become gateways, {@code false} otherwise.
     */
    public static final Setting<String> REMOTE_NODE_ATTRIBUTE = Setting.simpleString(
        "cluster.remote.node.attr",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<Boolean> REMOTE_CLUSTER_SKIP_UNAVAILABLE = Setting.affixKeySetting(
        "cluster.remote.",
        "skip_unavailable",
        (ns, key) -> boolSetting(key, true, new RemoteConnectionEnabled<>(ns, key), Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> REMOTE_CLUSTER_PING_SCHEDULE = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.ping_schedule",
        (ns, key) -> timeSetting(
            key,
            TransportSettings.PING_SCHEDULE,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    public static final Setting.AffixSetting<Compression.Enabled> REMOTE_CLUSTER_COMPRESS = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.compress",
        (ns, key) -> enumSetting(
            Compression.Enabled.class,
            key,
            TransportSettings.TRANSPORT_COMPRESS,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    public static final Setting.AffixSetting<Compression.Scheme> REMOTE_CLUSTER_COMPRESSION_SCHEME = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.compression_scheme",
        (ns, key) -> enumSetting(
            Compression.Scheme.class,
            key,
            TransportSettings.TRANSPORT_COMPRESSION_SCHEME,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    public static final Setting.AffixSetting<SecureString> REMOTE_CLUSTER_CREDENTIALS = Setting.affixKeySetting(
        "cluster.remote.",
        "credentials",
        key -> SecureSetting.secureString(key, null)
    );

    public static final String REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME = "cluster:internal/remote_cluster/handshake";

    private final boolean enabled;
    private final boolean remoteClusterServerEnabled;

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isRemoteClusterServerEnabled() {
        return remoteClusterServerEnabled;
    }

    private final TransportService transportService;
    private final Map<ProjectId, Map<String, RemoteClusterConnection>> remoteClusters;
    private final RemoteClusterCredentialsManager remoteClusterCredentialsManager;
    private final ProjectResolver projectResolver;

    @FixForMultiProject(description = "Inject the ProjectResolver instance.")
    RemoteClusterService(Settings settings, TransportService transportService) {
        super(settings);
        this.enabled = DiscoveryNode.isRemoteClusterClient(settings);
        this.remoteClusterServerEnabled = REMOTE_CLUSTER_SERVER_ENABLED.get(settings);
        this.transportService = transportService;
        this.projectResolver = DefaultProjectResolver.INSTANCE;
        this.remoteClusters = projectResolver.supportsMultipleProjects()
            ? ConcurrentCollections.newConcurrentMap()
            : Map.of(ProjectId.DEFAULT, ConcurrentCollections.newConcurrentMap());
        this.remoteClusterCredentialsManager = new RemoteClusterCredentialsManager(settings);
        if (remoteClusterServerEnabled) {
            registerRemoteClusterHandshakeRequestHandler(transportService);
        }
    }

    public DiscoveryNode getLocalNode() {
        return transportService.getLocalNode();
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

    public Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String[] indices, boolean returnLocalAll) {
        return groupIndices(getRegisteredRemoteClusterNames(), indicesOptions, indices, returnLocalAll);
    }

    public Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String[] indices) {
        return groupIndices(getRegisteredRemoteClusterNames(), indicesOptions, indices, true);
    }

    @Override
    public Set<String> getConfiguredClusters() {
        return getRegisteredRemoteClusterNames();
    }

    /**
     * Returns the registered remote cluster names.
     */
    @FixForMultiProject(description = "Analyze use cases, determine possible need for cluster scoped and project scoped versions.")
    public Set<String> getRegisteredRemoteClusterNames() {
        return getConnectionsMapForCurrentProject().keySet();
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
     * Returns whether the cluster identified by the provided alias is configured to be skipped when unavailable
     */
    public boolean isSkipUnavailable(String clusterAlias) {
        return getRemoteClusterConnection(clusterAlias).isSkipUnavailable();
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
        if (enabled == false) {
            throw new IllegalArgumentException(
                "this node does not have the " + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + " role"
            );
        }
        @FixForMultiProject(description = "Verify all callers will have the proper context set for resolving the origin project ID.")
        RemoteClusterConnection connection = getConnectionsMapForCurrentProject().get(cluster);
        if (connection == null) {
            throw new NoSuchRemoteClusterException(cluster);
        }
        return connection;
    }

    @Override
    public void listenForUpdates(ClusterSettings clusterSettings) {
        super.listenForUpdates(clusterSettings);
        clusterSettings.addAffixUpdateConsumer(REMOTE_CLUSTER_SKIP_UNAVAILABLE, this::updateSkipUnavailable, (alias, value) -> {});
    }

    private synchronized void updateSkipUnavailable(String clusterAlias, Boolean skipUnavailable) {
        RemoteClusterConnection remote = getConnectionsMapForCurrentProject().get(clusterAlias);
        if (remote != null) {
            remote.setSkipUnavailable(skipUnavailable);
        }
    }

    @FixForMultiProject(description = "Refactor as needed to support project specific changes to linked remotes.")
    public synchronized void updateRemoteClusterCredentials(Supplier<Settings> settingsSupplier, ActionListener<Void> listener) {
        final var projectId = projectResolver.getProjectId();
        final Settings settings = settingsSupplier.get();
        final UpdateRemoteClusterCredentialsResult result = remoteClusterCredentialsManager.updateClusterCredentials(settings);
        // We only need to rebuild connections when a credential was newly added or removed for a cluster alias, not if the credential
        // value was updated. Therefore, only consider added or removed aliases
        final int totalConnectionsToRebuild = result.addedClusterAliases().size() + result.removedClusterAliases().size();
        if (totalConnectionsToRebuild == 0) {
            logger.debug("project [{}] no connection rebuilding required after credentials update", projectId);
            listener.onResponse(null);
            return;
        }
        logger.info("project [{}] rebuilding [{}] connections after credentials update", projectId, totalConnectionsToRebuild);
        try (var connectionRefs = new RefCountingRunnable(() -> listener.onResponse(null))) {
            for (var clusterAlias : result.addedClusterAliases()) {
                maybeRebuildConnectionOnCredentialsChange(projectId, clusterAlias, settings, connectionRefs);
            }
            for (var clusterAlias : result.removedClusterAliases()) {
                maybeRebuildConnectionOnCredentialsChange(projectId, clusterAlias, settings, connectionRefs);
            }
        }
    }

    private void maybeRebuildConnectionOnCredentialsChange(
        ProjectId projectId,
        String clusterAlias,
        Settings settings,
        RefCountingRunnable connectionRefs
    ) {
        final var connectionsMap = getConnectionsMapForProject(projectId);
        if (false == connectionsMap.containsKey(clusterAlias)) {
            // A credential was added or removed before a remote connection was configured.
            // Without an existing connection, there is nothing to rebuild.
            logger.info(
                "project [{}] no connection rebuild required for remote cluster [{}] after credentials change",
                projectId,
                clusterAlias
            );
            return;
        }

        updateRemoteCluster(projectId, clusterAlias, settings, true, ActionListener.releaseAfter(new ActionListener<>() {
            @Override
            public void onResponse(RemoteClusterConnectionStatus status) {
                logger.info(
                    "project [{}] remote cluster connection [{}] updated after credentials change: [{}]",
                    projectId,
                    clusterAlias,
                    status
                );
            }

            @Override
            public void onFailure(Exception e) {
                // We don't want to return an error to the upstream listener here since a connection rebuild failure
                // does *not* imply a failure to reload secure settings; however, that's how it would surface in the reload-settings call.
                // Instead, we log a warning which is also consistent with how we handle remote cluster settings updates (logging instead of
                // returning an error)
                logger.warn(
                    () -> "project ["
                        + projectId
                        + "] failed to update remote cluster connection ["
                        + clusterAlias
                        + "] after credentials change",
                    e
                );
            }
        }, connectionRefs.acquire()));
    }

    @Override
    protected void updateRemoteCluster(String clusterAlias, Settings settings) {
        @FixForMultiProject(description = "ES-12270: Refactor as needed to support project specific changes to linked remotes.")
        final var projectId = projectResolver.getProjectId();
        CountDownLatch latch = new CountDownLatch(1);
        updateRemoteCluster(projectId, clusterAlias, settings, false, ActionListener.runAfter(new ActionListener<>() {
            @Override
            public void onResponse(RemoteClusterConnectionStatus status) {
                logger.info("project [{}] remote cluster connection [{}] updated: {}", projectId, clusterAlias, status);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> "project [" + projectId + " failed to update remote cluster connection [" + clusterAlias + "]", e);
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

    // Package-access for testing.
    @FixForMultiProject(description = "Refactor to supply the project ID associated with the alias and settings, or eliminate this method.")
    void updateRemoteCluster(String clusterAlias, Settings newSettings, ActionListener<RemoteClusterConnectionStatus> listener) {
        updateRemoteCluster(projectResolver.getProjectId(), clusterAlias, newSettings, false, listener);
    }

    /**
     * Adds, rebuilds, or closes and removes the connection for the specified remote cluster.
     *
     * @param projectId The project the remote cluster is associated with.
     * @param clusterAlias The alias used for the remote cluster being connected.
     * @param newSettings The updated settings for the remote connection.
     * @param forceRebuild Forces an existing connection to be closed and reconnected even if the connection strategy does not require it.
     * @param listener The listener invoked once the configured cluster has been connected.
     */
    private synchronized void updateRemoteCluster(
        ProjectId projectId,
        String clusterAlias,
        Settings newSettings,
        boolean forceRebuild,
        ActionListener<RemoteClusterConnectionStatus> listener
    ) {
        if (LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)) {
            throw new IllegalArgumentException("remote clusters must not have the empty string as its key");
        }

        final var connectionMap = getConnectionsMapForProject(projectId);
        RemoteClusterConnection remote = connectionMap.get(clusterAlias);
        if (RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, newSettings) == false) {
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
            Settings finalSettings = Settings.builder().put(this.settings, false).put(newSettings, false).build();
            remote = new RemoteClusterConnection(finalSettings, clusterAlias, transportService, remoteClusterCredentialsManager);
            connectionMap.put(clusterAlias, remote);
            remote.ensureConnected(listener.map(ignored -> RemoteClusterConnectionStatus.CONNECTED));
        } else if (forceRebuild || remote.shouldRebuildConnection(newSettings)) {
            // Changes to connection configuration. Must tear down existing connection
            try {
                IOUtils.close(remote);
            } catch (IOException e) {
                logger.warn("project [" + projectId + "] failed to close remote cluster connections for cluster: " + clusterAlias, e);
            }
            connectionMap.remove(clusterAlias);
            Settings finalSettings = Settings.builder().put(this.settings, false).put(newSettings, false).build();
            remote = new RemoteClusterConnection(finalSettings, clusterAlias, transportService, remoteClusterCredentialsManager);
            connectionMap.put(clusterAlias, remote);
            remote.ensureConnected(listener.map(ignored -> RemoteClusterConnectionStatus.RECONNECTED));
        } else {
            // No changes to connection configuration.
            listener.onResponse(RemoteClusterConnectionStatus.UNCHANGED);
        }
    }

    enum RemoteClusterConnectionStatus {
        CONNECTED,
        DISCONNECTED,
        RECONNECTED,
        UNCHANGED
    }

    /**
     * Connects to all remote clusters in a blocking fashion. This should be called on node startup to establish an initial connection
     * to all configured seed nodes.
     */
    void initializeRemoteClusters() {
        @FixForMultiProject(description = "Refactor for initializing connections to linked projects for each origin project supported.")
        final var projectId = projectResolver.getProjectId();
        final TimeValue timeValue = REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(settings);
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        Set<String> enabledClusters = RemoteClusterAware.getEnabledRemoteClusters(settings);

        if (enabledClusters.isEmpty()) {
            return;
        }

        CountDownActionListener listener = new CountDownActionListener(enabledClusters.size(), future);
        for (String clusterAlias : enabledClusters) {
            updateRemoteCluster(projectId, clusterAlias, settings, false, listener.map(ignored -> null));
        }

        if (enabledClusters.isEmpty()) {
            future.onResponse(null);
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
        if (enabled == false) {
            throw new IllegalArgumentException(
                "this node does not have the " + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + " role"
            );
        }
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
         * Behave according to the {@link #REMOTE_CLUSTER_SKIP_UNAVAILABLE} setting for this remote cluster: if this setting is
         * {@code false} (the default) then behave like {@link #RECONNECT_IF_DISCONNECTED}, but if it is {@code true} then behave like
         * {@link #FAIL_IF_DISCONNECTED}.
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
        if (transportService.getRemoteClusterService().isEnabled() == false) {
            throw new IllegalArgumentException(
                "this node does not have the " + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + " role"
            );
        }
        if (transportService.getRemoteClusterService().getRegisteredRemoteClusterNames().contains(clusterAlias) == false) {
            throw new NoSuchRemoteClusterException(clusterAlias);
        }
        return new RemoteClusterAwareClient(transportService, clusterAlias, responseExecutor, switch (disconnectedStrategy) {
            case RECONNECT_IF_DISCONNECTED -> true;
            case FAIL_IF_DISCONNECTED -> false;
            case RECONNECT_UNLESS_SKIP_UNAVAILABLE -> transportService.getRemoteClusterService().isSkipUnavailable(clusterAlias) == false;
        });
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
    private Map<String, RemoteClusterConnection> getConnectionsMapForProject(ProjectId projectId) {
        if (projectResolver.supportsMultipleProjects()) {
            assert ProjectId.DEFAULT.equals(projectId) == false : "The default project ID should not be used in multi-project environment";
            return remoteClusters.computeIfAbsent(projectId, unused -> ConcurrentCollections.newConcurrentMap());
        }
        assert ProjectId.DEFAULT.equals(projectId) : "Only the default project ID should be used when multiple projects are not supported";
        return remoteClusters.get(projectId);
    }

    private static class RemoteConnectionEnabled<T> implements Setting.Validator<T> {

        private final String clusterAlias;
        private final String key;

        private RemoteConnectionEnabled(String clusterAlias, String key) {
            this.clusterAlias = clusterAlias;
            this.key = key;
        }

        @Override
        public void validate(T value) {}

        @Override
        public void validate(T value, Map<Setting<?>, Object> settings, boolean isPresent) {
            if (isPresent && RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, settings) == false) {
                throw new IllegalArgumentException("Cannot configure setting [" + key + "] if remote cluster is not enabled.");
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return Stream.concat(
                Stream.of(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias)),
                settingsStream()
            ).iterator();
        }

        private Stream<Setting<?>> settingsStream() {
            return Arrays.stream(RemoteConnectionStrategy.ConnectionStrategy.values())
                .flatMap(strategy -> strategy.getEnablementSettings().get())
                .map(as -> as.getConcreteSettingForNamespace(clusterAlias));
        }
    };
}
