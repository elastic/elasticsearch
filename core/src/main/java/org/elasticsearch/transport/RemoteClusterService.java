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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.Setting.boolSetting;

/**
 * Basic service for accessing remote clusters via gateway nodes
 */
public final class RemoteClusterService extends RemoteClusterAware implements Closeable {

    /**
     * The maximum number of connections that will be established to a remote cluster. For instance if there is only a single
     * seed node, other nodes will be discovered up to the given number of nodes in this setting. The default is 3.
     */
    public static final Setting<Integer> REMOTE_CONNECTIONS_PER_CLUSTER = Setting.intSetting("search.remote.connections_per_cluster",
        3, 1, Setting.Property.NodeScope);

    /**
     * The initial connect timeout for remote cluster connections
     */
    public static final Setting<TimeValue> REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("search.remote.initial_connect_timeout", TimeValue.timeValueSeconds(30), Setting.Property.NodeScope);

    /**
     * The name of a node attribute to select nodes that should be connected to in the remote cluster.
     * For instance a node can be configured with <tt>node.attr.gateway: true</tt> in order to be eligible as a gateway node between
     * clusters. In that case <tt>search.remote.node.attr: gateway</tt> can be used to filter out other nodes in the remote cluster.
     * The value of the setting is expected to be a boolean, <tt>true</tt> for nodes that can become gateways, <tt>false</tt> otherwise.
     */
    public static final Setting<String> REMOTE_NODE_ATTRIBUTE = Setting.simpleString("search.remote.node.attr",
        Setting.Property.NodeScope);

    /**
     * If <code>true</code> connecting to remote clusters is supported on this node. If <code>false</code> this node will not establish
     * connections to any remote clusters configured. Search requests executed against this node (where this node is the coordinating node)
     * will fail if remote cluster syntax is used as an index pattern. The default is <code>true</code>
     */
    public static final Setting<Boolean> ENABLE_REMOTE_CLUSTERS = Setting.boolSetting("search.remote.connect", true,
        Setting.Property.NodeScope);

    public static final Setting.AffixSetting<Boolean> REMOTE_CLUSTER_SKIP_UNAVAILABLE =
            Setting.affixKeySetting("search.remote.", "skip_unavailable",
                    key -> boolSetting(key, false, Setting.Property.NodeScope, Setting.Property.Dynamic), REMOTE_CLUSTERS_SEEDS);

    private final TransportService transportService;
    private final int numRemoteConnections;
    private volatile Map<String, RemoteClusterConnection> remoteClusters = Collections.emptyMap();

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
    private synchronized void updateRemoteClusters(Map<String, List<DiscoveryNode>> seeds, ActionListener<Void> connectionListener) {
        if (seeds.containsKey(LOCAL_CLUSTER_GROUP_KEY)) {
            throw new IllegalArgumentException("remote clusters must not have the empty string as its key");
        }
        Map<String, RemoteClusterConnection> remoteClusters = new HashMap<>();
        if (seeds.isEmpty()) {
            connectionListener.onResponse(null);
        } else {
            CountDown countDown = new CountDown(seeds.size());
            Predicate<DiscoveryNode> nodePredicate = (node) -> Version.CURRENT.isCompatible(node.getVersion());
            if (REMOTE_NODE_ATTRIBUTE.exists(settings)) {
                // nodes can be tagged with node.attr.remote_gateway: true to allow a node to be a gateway node for
                // cross cluster search
                String attribute = REMOTE_NODE_ATTRIBUTE.get(settings);
                nodePredicate = nodePredicate.and((node) -> Booleans.parseBoolean(node.getAttributes().getOrDefault(attribute, "false")));
            }
            remoteClusters.putAll(this.remoteClusters);
            for (Map.Entry<String, List<DiscoveryNode>> entry : seeds.entrySet()) {
                RemoteClusterConnection remote = this.remoteClusters.get(entry.getKey());
                if (entry.getValue().isEmpty()) { // with no seed nodes we just remove the connection
                    try {
                        IOUtils.close(remote);
                    } catch (IOException e) {
                        logger.warn("failed to close remote cluster connections for cluster: " + entry.getKey(), e);
                    }
                    remoteClusters.remove(entry.getKey());
                    continue;
                }

                if (remote == null) { // this is a new cluster we have to add a new representation
                    remote = new RemoteClusterConnection(settings, entry.getKey(), entry.getValue(), transportService, numRemoteConnections,
                        nodePredicate);
                    remoteClusters.put(entry.getKey(), remote);
                }

                // now update the seed nodes no matter if it's new or already existing
                RemoteClusterConnection finalRemote = remote;
                remote.updateSeedNodes(entry.getValue(), ActionListener.wrap(
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
                            logger.warn("failed to update seed list for cluster: " + entry.getKey(), exception);
                        }
                    }));
            }
        }
        this.remoteClusters = Collections.unmodifiableMap(remoteClusters);
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

    public Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String[] indices, Predicate<String> indexExists) {
        Map<String, OriginalIndices> originalIndicesMap = new HashMap<>();
        if (isCrossClusterSearchEnabled()) {
            final Map<String, List<String>> groupedIndices = groupClusterIndices(indices, indexExists);
            for (Map.Entry<String, List<String>> entry : groupedIndices.entrySet()) {
                String clusterAlias = entry.getKey();
                List<String> originalIndices = entry.getValue();
                originalIndicesMap.put(clusterAlias,
                    new OriginalIndices(originalIndices.toArray(new String[originalIndices.size()]), indicesOptions));
            }
            if (originalIndicesMap.containsKey(LOCAL_CLUSTER_GROUP_KEY) == false) {
                originalIndicesMap.put(LOCAL_CLUSTER_GROUP_KEY, new OriginalIndices(Strings.EMPTY_ARRAY, indicesOptions));
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

    public void collectSearchShards(IndicesOptions indicesOptions, String preference, String routing,
                                    Map<String, OriginalIndices> remoteIndicesByCluster,
                                    ActionListener<Map<String, ClusterSearchShardsResponse>> listener) {
        final CountDown responsesCountDown = new CountDown(remoteIndicesByCluster.size());
        final Map<String, ClusterSearchShardsResponse> searchShardsResponses = new ConcurrentHashMap<>();
        final AtomicReference<TransportException> transportException = new AtomicReference<>();
        for (Map.Entry<String, OriginalIndices> entry : remoteIndicesByCluster.entrySet()) {
            final String clusterName = entry.getKey();
            RemoteClusterConnection remoteClusterConnection = remoteClusters.get(clusterName);
            if (remoteClusterConnection == null) {
                throw new IllegalArgumentException("no such remote cluster: " + clusterName);
            }
            final String[] indices = entry.getValue().indices();
            ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest(indices)
                .indicesOptions(indicesOptions).local(true).preference(preference)
                .routing(routing);
            remoteClusterConnection.fetchSearchShards(searchShardsRequest,
                new ActionListener<ClusterSearchShardsResponse>() {
                    @Override
                    public void onResponse(ClusterSearchShardsResponse clusterSearchShardsResponse) {
                        searchShardsResponses.put(clusterName, clusterSearchShardsResponse);
                        if (responsesCountDown.countDown()) {
                            TransportException exception = transportException.get();
                            if (exception == null) {
                                listener.onResponse(searchShardsResponses);
                            } else {
                                listener.onFailure(transportException.get());
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        TransportException exception = new TransportException("unable to communicate with remote cluster [" +
                                clusterName + "]", e);
                        if (transportException.compareAndSet(null, exception) == false) {
                            exception = transportException.accumulateAndGet(exception, (previous, current) -> {
                                current.addSuppressed(previous);
                                return current;
                            });
                        }
                        if (responsesCountDown.countDown()) {
                            listener.onFailure(exception);
                        }
                    }
                });
        }
    }

    /**
     * Returns a connection to the given node on the given remote cluster
     * @throws IllegalArgumentException if the remote cluster is unknown
     */
    public Transport.Connection getConnection(DiscoveryNode node, String cluster) {
        RemoteClusterConnection connection = remoteClusters.get(cluster);
        if (connection == null) {
            throw new IllegalArgumentException("no such remote cluster: " + cluster);
        }
        return connection.getConnection(node);
    }

    /**
     * Ensures that the given cluster alias is connected. If the cluster is connected this operation
     * will invoke the listener immediately.
     */
    public void ensureConnected(String clusterAlias, ActionListener<Void> listener) {
        RemoteClusterConnection remoteClusterConnection = remoteClusters.get(clusterAlias);
        if (remoteClusterConnection == null) {
            throw new IllegalArgumentException("no such remote cluster: " + clusterAlias);
        }
        remoteClusterConnection.ensureConnected(listener);
    }

    public Transport.Connection getConnection(String cluster) {
        RemoteClusterConnection connection = remoteClusters.get(cluster);
        if (connection == null) {
            throw new IllegalArgumentException("no such remote cluster: " + cluster);
        }
        return connection.getConnection();
    }

    @Override
    protected Set<String> getRemoteClusterNames() {
        return this.remoteClusters.keySet();
    }

    @Override
    public void listenForUpdates(ClusterSettings clusterSettings) {
        super.listenForUpdates(clusterSettings);
        clusterSettings.addAffixUpdateConsumer(REMOTE_CLUSTER_SKIP_UNAVAILABLE, this::updateSkipUnavailable,
                (clusterAlias, value) -> {});
    }

    synchronized void updateSkipUnavailable(String clusterAlias, Boolean skipUnavailable) {
        RemoteClusterConnection remote = this.remoteClusters.get(clusterAlias);
        if (remote != null) {
            remote.updateSkipUnavailable(skipUnavailable);
        }
    }

    protected void updateRemoteCluster(String clusterAlias, List<InetSocketAddress> addresses) {
        updateRemoteCluster(clusterAlias, addresses, ActionListener.wrap((x) -> {}, (x) -> {}));
    }

    void updateRemoteCluster(
            final String clusterAlias,
            final List<InetSocketAddress> addresses,
            final ActionListener<Void> connectionListener) {
        final List<DiscoveryNode> nodes = addresses.stream().map(address -> {
            final TransportAddress transportAddress = new TransportAddress(address);
            final String id = clusterAlias + "#" + transportAddress.toString();
            final Version version = Version.CURRENT.minimumCompatibilityVersion();
            return new DiscoveryNode(id, transportAddress, version);
        }).collect(Collectors.toList());
        updateRemoteClusters(Collections.singletonMap(clusterAlias, nodes), connectionListener);
    }

    /**
     * Connects to all remote clusters in a blocking fashion. This should be called on node startup to establish an initial connection
     * to all configured seed nodes.
     */
    void initializeRemoteClusters() {
        final TimeValue timeValue = REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(settings);
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        Map<String, List<DiscoveryNode>> seeds = RemoteClusterAware.buildRemoteClustersSeeds(settings);
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

    @Override
    public void close() throws IOException {
        IOUtils.close(remoteClusters.values());
    }

    public void getRemoteConnectionInfos(ActionListener<Collection<RemoteConnectionInfo>> listener) {
        final Map<String, RemoteClusterConnection> remoteClusters = this.remoteClusters;
        if (remoteClusters.isEmpty()) {
            listener.onResponse(Collections.emptyList());
        } else {
            final GroupedActionListener<RemoteConnectionInfo> actionListener = new GroupedActionListener<>(listener,
                remoteClusters.size(), Collections.emptyList());
            for (RemoteClusterConnection connection : remoteClusters.values()) {
                connection.getConnectionInfo(actionListener);
            }
        }
    }

    /**
     * Collects all nodes of the given clusters and returns / passes a (clusterAlias, nodeId) to {@link DiscoveryNode}
     * function on success.
     */
    public void collectNodes(Set<String> clusters, ActionListener<BiFunction<String, String, DiscoveryNode>> listener) {
        Map<String, RemoteClusterConnection> remoteClusters = this.remoteClusters;
        for (String cluster : clusters) {
            if (remoteClusters.containsKey(cluster) == false) {
                listener.onFailure(new IllegalArgumentException("no such remote cluster: [" + cluster + "]"));
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
}
