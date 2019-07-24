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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

/**
 * Basic service for accessing remote clusters via gateway nodes
 */
public final class RemoteClusterService extends RemoteClusterAware implements Closeable {

    private static final Logger logger = LogManager.getLogger(RemoteClusterService.class);

    private static final ActionListener<Void> noopListener = ActionListener.wrap((x) -> {}, (x) -> {});

    /**
     * The maximum number of connections that will be established to a remote cluster. For instance if there is only a single
     * seed node, other nodes will be discovered up to the given number of nodes in this setting. The default is 3.
     */
    public static final Setting<Integer> REMOTE_CONNECTIONS_PER_CLUSTER =
            Setting.intSetting(
                    "cluster.remote.connections_per_cluster",
                    3,
                    1,
                    Setting.Property.NodeScope);

    /**
     * The initial connect timeout for remote cluster connections
     */
    public static final Setting<TimeValue> REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING =
            Setting.positiveTimeSetting(
                    "cluster.remote.initial_connect_timeout",
                    TimeValue.timeValueSeconds(30),
                    Setting.Property.NodeScope);

    /**
     * The name of a node attribute to select nodes that should be connected to in the remote cluster.
     * For instance a node can be configured with {@code node.attr.gateway: true} in order to be eligible as a gateway node between
     * clusters. In that case {@code cluster.remote.node.attr: gateway} can be used to filter out other nodes in the remote cluster.
     * The value of the setting is expected to be a boolean, {@code true} for nodes that can become gateways, {@code false} otherwise.
     */
    public static final Setting<String> REMOTE_NODE_ATTRIBUTE =
            Setting.simpleString("cluster.remote.node.attr", Setting.Property.NodeScope);

    /**
     * If <code>true</code> connecting to remote clusters is supported on this node. If <code>false</code> this node will not establish
     * connections to any remote clusters configured. Search requests executed against this node (where this node is the coordinating node)
     * will fail if remote cluster syntax is used as an index pattern. The default is <code>true</code>
     */
    public static final Setting<Boolean> ENABLE_REMOTE_CLUSTERS =
            Setting.boolSetting(
                    "cluster.remote.connect",
                    true,
                    Setting.Property.NodeScope);

    public static final Setting.AffixSetting<Boolean> REMOTE_CLUSTER_SKIP_UNAVAILABLE =
            Setting.affixKeySetting(
                    "cluster.remote.",
                    "skip_unavailable",
                    key -> boolSetting(
                            key,
                            false,
                            Setting.Property.Dynamic,
                            Setting.Property.NodeScope),
                    REMOTE_CLUSTERS_SEEDS);

    public static final Setting.AffixSetting<TimeValue> REMOTE_CLUSTER_PING_SCHEDULE = Setting.affixKeySetting(
            "cluster.remote.",
            "transport.ping_schedule",
            key -> timeSetting(key, TransportSettings.PING_SCHEDULE, Setting.Property.Dynamic, Setting.Property.NodeScope),
            REMOTE_CLUSTERS_SEEDS);

    public static final Setting.AffixSetting<Boolean> REMOTE_CLUSTER_COMPRESS = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.compress",
        key -> boolSetting(key, TransportSettings.TRANSPORT_COMPRESS, Setting.Property.Dynamic, Setting.Property.NodeScope),
        REMOTE_CLUSTERS_SEEDS);

    private static final Predicate<DiscoveryNode> DEFAULT_NODE_PREDICATE = (node) -> Version.CURRENT.isCompatible(node.getVersion())
            && (node.isMasterNode() == false  || node.isDataNode() || node.isIngestNode());

    private final TransportService transportService;
    private final int numRemoteConnections;
    private volatile Map<String, RemoteClusterConnection> remoteClusters = Collections.emptyMap();
    private volatile Map<String, ConnectionProfile> remoteClusterConnectionProfiles = Collections.emptyMap();

    RemoteClusterService(Settings settings, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
        numRemoteConnections = REMOTE_CONNECTIONS_PER_CLUSTER.get(settings);
    }

    /**
     * This method updates the list of remote clusters. It's intended to be used as an update consumer on the settings infrastructure
     * @param seeds a cluster alias to discovery node mapping representing the remote clusters seeds nodes
     * @param connectionListener a listener invoked once every configured cluster has been connected to
     */
    private synchronized void updateRemoteClusters(Map<String, Tuple<String, List<Tuple<String, Supplier<DiscoveryNode>>>>> seeds,
                                                   ActionListener<Void> connectionListener) {
        if (seeds.containsKey(LOCAL_CLUSTER_GROUP_KEY)) {
            throw new IllegalArgumentException("remote clusters must not have the empty string as its key");
        }
        Map<String, RemoteClusterConnection> remoteClusters = new HashMap<>();
        if (seeds.isEmpty()) {
            connectionListener.onResponse(null);
        } else {
            CountDown countDown = new CountDown(seeds.size());
            remoteClusters.putAll(this.remoteClusters);
            for (Map.Entry<String, Tuple<String, List<Tuple<String, Supplier<DiscoveryNode>>>>> entry : seeds.entrySet()) {
                List<Tuple<String, Supplier<DiscoveryNode>>> seedList = entry.getValue().v2();
                String proxyAddress = entry.getValue().v1();

                String clusterAlias = entry.getKey();
                RemoteClusterConnection remote = this.remoteClusters.get(clusterAlias);
                ConnectionProfile connectionProfile = this.remoteClusterConnectionProfiles.get(clusterAlias);
                if (seedList.isEmpty()) { // with no seed nodes we just remove the connection
                    try {
                        IOUtils.close(remote);
                    } catch (IOException e) {
                        logger.warn("failed to close remote cluster connections for cluster: " + clusterAlias, e);
                    }
                    remoteClusters.remove(clusterAlias);
                    continue;
                }

                if (remote == null) { // this is a new cluster we have to add a new representation
                    remote = new RemoteClusterConnection(settings, clusterAlias, seedList, transportService, numRemoteConnections,
                        getNodePredicate(settings), proxyAddress, connectionProfile);
                    remoteClusters.put(clusterAlias, remote);
                } else if (connectionProfileChanged(remote.getConnectionManager().getConnectionProfile(), connectionProfile)
                        || seedsChanged(remote.getSeedNodes(), seedList)) {
                    // New ConnectionProfile. Must tear down existing connection
                    try {
                        IOUtils.close(remote);
                    } catch (IOException e) {
                        logger.warn("failed to close remote cluster connections for cluster: " + clusterAlias, e);
                    }
                    remoteClusters.remove(clusterAlias);
                    remote = new RemoteClusterConnection(settings, clusterAlias, seedList, transportService, numRemoteConnections,
                        getNodePredicate(settings), proxyAddress, connectionProfile);
                    remoteClusters.put(clusterAlias, remote);
                }

                // now update the seed nodes no matter if it's new or already existing
                RemoteClusterConnection finalRemote = remote;
                remote.updateSeedNodes(proxyAddress, seedList, ActionListener.wrap(
                        response -> {
                            if (countDown.countDown()) {
                                connectionListener.onResponse(response);
                            }
                        },
                        exception -> {
                            if (countDown.fastForward()) {
                                connectionListener.onFailure(exception);
                            }
                            if (finalRemote.isClosed() == false) {
                                logger.warn("failed to update seed list for cluster: " + clusterAlias, exception);
                            }
                        }));
            }
        }
        this.remoteClusters = Collections.unmodifiableMap(remoteClusters);
    }

    static Predicate<DiscoveryNode> getNodePredicate(Settings settings) {
        if (REMOTE_NODE_ATTRIBUTE.exists(settings)) {
            // nodes can be tagged with node.attr.remote_gateway: true to allow a node to be a gateway node for cross cluster search
            String attribute = REMOTE_NODE_ATTRIBUTE.get(settings);
            return DEFAULT_NODE_PREDICATE.and((node) -> Booleans.parseBoolean(node.getAttributes().getOrDefault(attribute, "false")));
        }
        return DEFAULT_NODE_PREDICATE;
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
        Map<String, OriginalIndices> originalIndicesMap = new HashMap<>();
        if (isCrossClusterSearchEnabled()) {
            final Map<String, List<String>> groupedIndices = groupClusterIndices(getRemoteClusterNames(), indices);
            if (groupedIndices.isEmpty()) {
                //search on _all in the local cluster if neither local indices nor remote indices were specified
                originalIndicesMap.put(LOCAL_CLUSTER_GROUP_KEY, new OriginalIndices(Strings.EMPTY_ARRAY, indicesOptions));
            } else {
                for (Map.Entry<String, List<String>> entry : groupedIndices.entrySet()) {
                    String clusterAlias = entry.getKey();
                    List<String> originalIndices = entry.getValue();
                    originalIndicesMap.put(clusterAlias,
                        new OriginalIndices(originalIndices.toArray(new String[0]), indicesOptions));
                }
            }
        } else {
            originalIndicesMap.put(LOCAL_CLUSTER_GROUP_KEY, new OriginalIndices(indices, indicesOptions));
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
        getRemoteClusterConnection(clusterAlias).ensureConnected(listener);
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

    RemoteClusterConnection getRemoteClusterConnection(String cluster) {
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
            remote.updateSkipUnavailable(skipUnavailable);
        }
    }

    @Override
    protected void updateRemoteCluster(String clusterAlias, List<String> addresses, String proxyAddress, boolean compressionEnabled,
                                       TimeValue pingSchedule) {
        if (LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)) {
            throw new IllegalArgumentException("remote clusters must not have the empty string as its key");
        }
        ConnectionProfile oldProfile = remoteClusterConnectionProfiles.get(clusterAlias);
        ConnectionProfile newProfile;
        if (oldProfile != null) {
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder(oldProfile);
            builder.setCompressionEnabled(compressionEnabled);
            builder.setPingInterval(pingSchedule);
            newProfile = builder.build();
        } else {
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder(buildConnectionProfileFromSettings(clusterAlias));
            builder.setCompressionEnabled(compressionEnabled);
            builder.setPingInterval(pingSchedule);
            newProfile = builder.build();
        }
        updateRemoteCluster(clusterAlias, addresses, proxyAddress, newProfile, noopListener);
    }

    void updateRemoteCluster(final String clusterAlias, final List<String> addresses, final String proxyAddress,
                             final ConnectionProfile connectionProfile, final ActionListener<Void> connectionListener) {
        HashMap<String, ConnectionProfile> connectionProfiles = new HashMap<>(remoteClusterConnectionProfiles);
        connectionProfiles.put(clusterAlias, connectionProfile);
        this.remoteClusterConnectionProfiles = Collections.unmodifiableMap(connectionProfiles);
        final List<Tuple<String, Supplier<DiscoveryNode>>> nodes =
            addresses.stream().<Tuple<String, Supplier<DiscoveryNode>>>map(address -> Tuple.tuple(address, () ->
                buildSeedNode(clusterAlias, address, Strings.hasLength(proxyAddress)))
            ).collect(Collectors.toList());
        updateRemoteClusters(Collections.singletonMap(clusterAlias, new Tuple<>(proxyAddress, nodes)), connectionListener);
    }

    /**
     * Connects to all remote clusters in a blocking fashion. This should be called on node startup to establish an initial connection
     * to all configured seed nodes.
     */
    void initializeRemoteClusters() {
        final TimeValue timeValue = REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(settings);
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        Map<String, Tuple<String, List<Tuple<String, Supplier<DiscoveryNode>>>>> seeds =
                RemoteClusterAware.buildRemoteClustersDynamicConfig(settings);
        initializeConnectionProfiles(seeds.keySet());
        updateRemoteClusters(seeds, future);
        try {
            future.get(timeValue.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException ex) {
            logger.warn("failed to connect to remote clusters within {}", timeValue.toString());
        } catch (Exception e) {
            throw new IllegalStateException("failed to connect to remote clusters", e);
        }
    }

    private synchronized void initializeConnectionProfiles(Set<String> remoteClusters) {
        Map<String, ConnectionProfile> connectionProfiles = new HashMap<>(remoteClusters.size());
        for (String clusterName : remoteClusters) {
            connectionProfiles.put(clusterName, buildConnectionProfileFromSettings(clusterName));
        }
        this.remoteClusterConnectionProfiles = Collections.unmodifiableMap(connectionProfiles);
    }

    private ConnectionProfile buildConnectionProfileFromSettings(String clusterName) {
        return buildConnectionProfileFromSettings(settings, clusterName);
    }

    static ConnectionProfile buildConnectionProfileFromSettings(Settings settings, String clusterName) {
        return new ConnectionProfile.Builder()
            .setConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .addConnections(6, TransportRequestOptions.Type.REG, TransportRequestOptions.Type.PING) // TODO make this configurable?
            // we don't want this to be used for anything else but search
            .addConnections(0, TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.STATE,
                TransportRequestOptions.Type.RECOVERY)
            .setCompressionEnabled(REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace(clusterName).get(settings))
            .setPingInterval(REMOTE_CLUSTER_PING_SCHEDULE.getConcreteSettingForNamespace(clusterName).get(settings))
            .build();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(remoteClusters.values());
    }

    public Stream<RemoteConnectionInfo> getRemoteConnectionInfos() {
        return remoteClusters.values().stream().map(RemoteClusterConnection::getConnectionInfo);
    }

    private boolean connectionProfileChanged(ConnectionProfile oldProfile, ConnectionProfile newProfile) {
        return Objects.equals(oldProfile.getCompressionEnabled(), newProfile.getCompressionEnabled()) == false
            || Objects.equals(oldProfile.getPingInterval(), newProfile.getPingInterval()) == false;
    }

    private boolean seedsChanged(final List<Tuple<String, Supplier<DiscoveryNode>>> oldSeedNodes,
                                 final List<Tuple<String, Supplier<DiscoveryNode>>> newSeedNodes) {
        if (oldSeedNodes.size() != newSeedNodes.size()) {
            return true;
        }
        Set<String> oldSeeds = oldSeedNodes.stream().map(Tuple::v1).collect(Collectors.toSet());
        Set<String> newSeeds = newSeedNodes.stream().map(Tuple::v1).collect(Collectors.toSet());
        return oldSeeds.equals(newSeeds) == false;
    }

    /**
     * Collects all nodes of the given clusters and returns / passes a (clusterAlias, nodeId) to {@link DiscoveryNode}
     * function on success.
     */
    public void collectNodes(Set<String> clusters, ActionListener<BiFunction<String, String, DiscoveryNode>> listener) {
        Map<String, RemoteClusterConnection> remoteClusters = this.remoteClusters;
        for (String cluster : clusters) {
            if (remoteClusters.containsKey(cluster) == false) {
                listener.onFailure(new NoSuchRemoteClusterException(cluster));
                return;
            }
        }

        final Map<String, Function<String, DiscoveryNode>> clusterMap = new HashMap<>();
        CountDown countDown = new CountDown(clusters.size());
        Function<String, DiscoveryNode> nullFunction = s -> null;
        for (final String cluster : clusters) {
            RemoteClusterConnection connection = remoteClusters.get(cluster);
            connection.collectNodes(new ActionListener<Function<String, DiscoveryNode>>() {
                @Override
                public void onResponse(Function<String, DiscoveryNode> nodeLookup) {
                    synchronized (clusterMap) {
                        clusterMap.put(cluster, nodeLookup);
                    }
                    if (countDown.countDown()) {
                        listener.onResponse((clusterAlias, nodeId)
                                -> clusterMap.getOrDefault(clusterAlias, nullFunction).apply(nodeId));
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
     * Returns a client to the remote cluster if the given cluster alias exists.
     * @param threadPool the {@link ThreadPool} for the client
     * @param clusterAlias the cluster alias the remote cluster is registered under
     *
     * @throws IllegalArgumentException if the given clusterAlias doesn't exist
     */
    public Client getRemoteClusterClient(ThreadPool threadPool, String clusterAlias) {
        if (transportService.getRemoteClusterService().getRemoteClusterNames().contains(clusterAlias) == false) {
            throw new NoSuchRemoteClusterException(clusterAlias);
        }
        return new RemoteClusterAwareClient(settings, threadPool, transportService, clusterAlias);
    }

    Collection<RemoteClusterConnection> getConnections() {
        return remoteClusters.values();
    }
}
