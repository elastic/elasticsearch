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
import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.transport.RemoteClusterCredentialsManager.UpdateRemoteClusterCredentialsResult;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.enumSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;

/**
 * Basic service for accessing remote clusters via gateway nodes
 */
public final class RemoteClusterService extends RemoteClusterAware implements Closeable, ReportingService<RemoteClusterServerInfo> {

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
        (ns, key) -> boolSetting(key, false, new RemoteConnectionEnabled<>(ns, key), Setting.Property.Dynamic, Setting.Property.NodeScope)
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
    private final Map<String, RemoteClusterConnection> remoteClusters = ConcurrentCollections.newConcurrentMap();
    private final RemoteClusterCredentialsManager remoteClusterCredentialsManager;

    RemoteClusterService(Settings settings, TransportService transportService) {
        super(settings);
        this.enabled = DiscoveryNode.isRemoteClusterClient(settings);
        this.remoteClusterServerEnabled = REMOTE_CLUSTER_SERVER_ENABLED.get(settings);
        this.transportService = transportService;
        this.remoteClusterCredentialsManager = new RemoteClusterCredentialsManager(settings);
        if (remoteClusterServerEnabled) {
            registerRemoteClusterHandshakeRequestHandler(transportService);
        }
    }

    public DiscoveryNode getLocalNode() {
        return transportService.getLocalNode();
    }

    /**
     * Returns <code>true</code> if at least one remote cluster is configured
     */
    public boolean isCrossClusterSearchEnabled() {
        return remoteClusters.isEmpty() == false;
    }

    boolean isRemoteNodeConnected(final String remoteCluster, final DiscoveryNode node) {
        return remoteClusters.get(remoteCluster).isNodeConnected(node);
    }

    public Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String[] indices) {
        final Map<String, OriginalIndices> originalIndicesMap = new HashMap<>();
        final Map<String, List<String>> groupedIndices = groupClusterIndices(getRemoteClusterNames(), indices);
        if (groupedIndices.isEmpty()) {
            // search on _all in the local cluster if neither local indices nor remote indices were specified
            originalIndicesMap.put(LOCAL_CLUSTER_GROUP_KEY, new OriginalIndices(Strings.EMPTY_ARRAY, indicesOptions));
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
     * Returns <code>true</code> iff the given cluster is configured as a remote cluster. Otherwise <code>false</code>
     */
    boolean isRemoteClusterRegistered(String clusterName) {
        return remoteClusters.containsKey(clusterName);
    }

    /**
     * Returns the registered remote cluster names.
     */
    public Set<String> getRegisteredRemoteClusterNames() {
        // remoteClusters is unmodifiable so its key set will be unmodifiable too
        return remoteClusters.keySet();
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
                } catch (NoSuchRemoteClusterException e) {
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

    RemoteClusterConnection getRemoteClusterConnection(String cluster) {
        if (enabled == false) {
            throw new IllegalArgumentException(
                "this node does not have the " + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + " role"
            );
        }
        RemoteClusterConnection connection = remoteClusters.get(cluster);
        if (connection == null) {
            throw new NoSuchRemoteClusterException(cluster);
        }
        return connection;
    }

    Set<String> getRemoteClusterNames() {
        return this.remoteClusters.keySet();
    }

    @Override
    public void listenForUpdates(ClusterSettings clusterSettings) {
        super.listenForUpdates(clusterSettings);
        clusterSettings.addAffixUpdateConsumer(REMOTE_CLUSTER_SKIP_UNAVAILABLE, this::updateSkipUnavailable, (alias, value) -> {});
    }

    private synchronized void updateSkipUnavailable(String clusterAlias, Boolean skipUnavailable) {
        RemoteClusterConnection remote = this.remoteClusters.get(clusterAlias);
        if (remote != null) {
            remote.setSkipUnavailable(skipUnavailable);
        }
    }

    public synchronized void updateRemoteClusterCredentials(Supplier<Settings> settingsSupplier, ActionListener<Void> listener) {
        final Settings settings = settingsSupplier.get();
        final UpdateRemoteClusterCredentialsResult result = remoteClusterCredentialsManager.updateClusterCredentials(settings);
        // We only need to rebuild connections when a credential was newly added or removed for a cluster alias, not if the credential
        // value was updated. Therefore, only consider added or removed aliases
        final int totalConnectionsToRebuild = result.addedClusterAliases().size() + result.removedClusterAliases().size();
        if (totalConnectionsToRebuild == 0) {
            logger.debug("no connection rebuilding required after credentials update");
            listener.onResponse(null);
            return;
        }
        logger.info("rebuilding [{}] connections after credentials update", totalConnectionsToRebuild);
        try (var connectionRefs = new RefCountingRunnable(() -> listener.onResponse(null))) {
            for (var clusterAlias : result.addedClusterAliases()) {
                maybeRebuildConnectionOnCredentialsChange(clusterAlias, settings, connectionRefs);
            }
            for (var clusterAlias : result.removedClusterAliases()) {
                maybeRebuildConnectionOnCredentialsChange(clusterAlias, settings, connectionRefs);
            }
        }
    }

    // package-private for testing

    private void maybeRebuildConnectionOnCredentialsChange(String clusterAlias, Settings settings, RefCountingRunnable connectionRefs) {
        if (false == remoteClusters.containsKey(clusterAlias)) {
            // A credential was added or removed before a remote connection was configured.
            // Without an existing connection, there is nothing to rebuild.
            logger.info("no connection rebuild required for remote cluster [{}] after credentials change", clusterAlias);
            return;
        }

        updateRemoteCluster(clusterAlias, settings, true, ActionListener.releaseAfter(new ActionListener<>() {
            @Override
            public void onResponse(RemoteClusterConnectionStatus status) {
                logger.info("remote cluster connection [{}] updated after credentials change: [{}]", clusterAlias, status);
            }

            @Override
            public void onFailure(Exception e) {
                // We don't want to return an error to the upstream listener here since a connection rebuild failure
                // does *not* imply a failure to reload secure settings; however, that's how it would surface in the reload-settings call.
                // Instead, we log a warning which is also consistent with how we handle remote cluster settings updates (logging instead of
                // returning an error)
                logger.warn(() -> "failed to update remote cluster connection [" + clusterAlias + "] after credentials change", e);
            }
        }, connectionRefs.acquire()));
    }

    @Override
    protected void updateRemoteCluster(String clusterAlias, Settings settings) {
        CountDownLatch latch = new CountDownLatch(1);
        updateRemoteCluster(clusterAlias, settings, ActionListener.runAfter(new ActionListener<>() {
            @Override
            public void onResponse(RemoteClusterConnectionStatus status) {
                logger.info("remote cluster connection [{}] updated: {}", clusterAlias, status);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> "failed to update remote cluster connection [" + clusterAlias + "]", e);
            }
        }, latch::countDown));

        try {
            // Wait 10 seconds for a connections. We must use a latch instead of a future because we
            // are on the cluster state thread and our custom future implementation will throw an
            // assertion.
            if (latch.await(10, TimeUnit.SECONDS) == false) {
                logger.warn("failed to update remote cluster connection [{}] within {}", clusterAlias, TimeValue.timeValueSeconds(10));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * This method updates the list of remote clusters. It's intended to be used as an update consumer on the settings infrastructure
     *
     * @param clusterAlias a cluster alias to discovery node mapping representing the remote clusters seeds nodes
     * @param newSettings the updated settings for the remote connection
     * @param listener a listener invoked once every configured cluster has been connected to
     */
    void updateRemoteCluster(String clusterAlias, Settings newSettings, ActionListener<RemoteClusterConnectionStatus> listener) {
        updateRemoteCluster(clusterAlias, newSettings, false, listener);
    }

    private synchronized void updateRemoteCluster(
        String clusterAlias,
        Settings newSettings,
        boolean forceRebuild,
        ActionListener<RemoteClusterConnectionStatus> listener
    ) {
        if (LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)) {
            throw new IllegalArgumentException("remote clusters must not have the empty string as its key");
        }

        RemoteClusterConnection remote = this.remoteClusters.get(clusterAlias);
        if (RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, newSettings) == false) {
            try {
                IOUtils.close(remote);
            } catch (IOException e) {
                logger.warn("failed to close remote cluster connections for cluster: " + clusterAlias, e);
            }
            remoteClusters.remove(clusterAlias);
            listener.onResponse(RemoteClusterConnectionStatus.DISCONNECTED);
            return;
        }

        if (remote == null) {
            // this is a new cluster we have to add a new representation
            Settings finalSettings = Settings.builder().put(this.settings, false).put(newSettings, false).build();
            remote = new RemoteClusterConnection(finalSettings, clusterAlias, transportService, remoteClusterCredentialsManager);
            remoteClusters.put(clusterAlias, remote);
            remote.ensureConnected(listener.map(ignored -> RemoteClusterConnectionStatus.CONNECTED));
        } else if (forceRebuild || remote.shouldRebuildConnection(newSettings)) {
            // Changes to connection configuration. Must tear down existing connection
            try {
                IOUtils.close(remote);
            } catch (IOException e) {
                logger.warn("failed to close remote cluster connections for cluster: " + clusterAlias, e);
            }
            remoteClusters.remove(clusterAlias);
            Settings finalSettings = Settings.builder().put(this.settings, false).put(newSettings, false).build();
            remote = new RemoteClusterConnection(finalSettings, clusterAlias, transportService, remoteClusterCredentialsManager);
            remoteClusters.put(clusterAlias, remote);
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
        final TimeValue timeValue = REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(settings);
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        Set<String> enabledClusters = RemoteClusterAware.getEnabledRemoteClusters(settings);

        if (enabledClusters.isEmpty()) {
            return;
        }

        CountDownActionListener listener = new CountDownActionListener(enabledClusters.size(), future);
        for (String clusterAlias : enabledClusters) {
            updateRemoteCluster(clusterAlias, settings, listener.map(ignored -> null));
        }

        if (enabledClusters.isEmpty()) {
            future.onResponse(null);
        }

        try {
            future.get(timeValue.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException ex) {
            logger.warn("failed to connect to remote clusters within {}", timeValue.toString());
        } catch (Exception e) {
            logger.warn("failed to connect to remote clusters", e);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(remoteClusters.values());
    }

    public Stream<RemoteConnectionInfo> getRemoteConnectionInfos() {
        return remoteClusters.values().stream().map(RemoteClusterConnection::getConnectionInfo);
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
        for (String cluster : clusters) {
            if (this.remoteClusters.containsKey(cluster) == false) {
                listener.onFailure(new NoSuchRemoteClusterException(cluster));
                return;
            }
        }

        final Map<String, Function<String, DiscoveryNode>> clusterMap = new HashMap<>();
        CountDown countDown = new CountDown(clusters.size());
        Function<String, DiscoveryNode> nullFunction = s -> null;
        for (final String cluster : clusters) {
            RemoteClusterConnection connection = this.remoteClusters.get(cluster);
            connection.collectNodes(new ActionListener<Function<String, DiscoveryNode>>() {
                @Override
                public void onResponse(Function<String, DiscoveryNode> nodeLookup) {
                    synchronized (clusterMap) {
                        clusterMap.put(cluster, nodeLookup);
                    }
                    if (countDown.countDown()) {
                        listener.onResponse((clusterAlias, nodeId) -> clusterMap.getOrDefault(clusterAlias, nullFunction).apply(nodeId));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (countDown.fastForward()) { // we need to check if it's true since we could have multiple failures
                        listener.onFailure(e);
                    }
                }
            });
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
        if (transportService.getRemoteClusterService().getRemoteClusterNames().contains(clusterAlias) == false) {
            throw new NoSuchRemoteClusterException(clusterAlias);
        }
        return new RemoteClusterAwareClient(transportService, clusterAlias, responseExecutor, switch (disconnectedStrategy) {
            case RECONNECT_IF_DISCONNECTED -> true;
            case FAIL_IF_DISCONNECTED -> false;
            case RECONNECT_UNLESS_SKIP_UNAVAILABLE -> transportService.getRemoteClusterService().isSkipUnavailable(clusterAlias) == false;
        });
    }

    Collection<RemoteClusterConnection> getConnections() {
        return remoteClusters.values();
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
