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
package org.elasticsearch.action.search;

import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ClusterNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Basic service for accessing remote clusters via gateway nodes
 */
public final class RemoteClusterService extends AbstractComponent implements Closeable {

    static final String LOCAL_CLUSTER_GROUP_KEY = "";

    /**
     * A list of initial seed nodes to discover eligible nodes from the remote cluster
     */
    public static final Setting.AffixSetting<List<InetSocketAddress>> REMOTE_CLUSTERS_SEEDS = Setting.affixKeySetting("search.remote.",
        "seeds", (key) -> Setting.listSetting(key, Collections.emptyList(), RemoteClusterService::parseSeedAddress,
            Setting.Property.NodeScope, Setting.Property.Dynamic));
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

    private static final char REMOTE_CLUSTER_INDEX_SEPARATOR = ':';

    private final TransportService transportService;
    private final int numRemoteConnections;
    private final ClusterNameExpressionResolver clusterNameResolver;
    private volatile Map<String, RemoteClusterConnection> remoteClusters = Collections.emptyMap();

    RemoteClusterService(Settings settings, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
        this.clusterNameResolver = new ClusterNameExpressionResolver(settings);
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
    boolean isCrossClusterSearchEnabled() {
        return remoteClusters.isEmpty() == false;
    }

    boolean isRemoteNodeConnected(final String remoteCluster, final DiscoveryNode node) {
        return remoteClusters.get(remoteCluster).isNodeConnected(node);
    }

    /**
     * Groups indices per cluster by splitting remote cluster-alias, index-name pairs on {@link #REMOTE_CLUSTER_INDEX_SEPARATOR}. All
     * indices per cluster are collected as a list in the returned map keyed by the cluster alias. Local indices are grouped under
     * {@link #LOCAL_CLUSTER_GROUP_KEY}. The returned map is mutable.
     *
     * @param requestIndices the indices in the search request to filter
     * @param indexExists a predicate that can test if a certain index or alias exists
     *
     * @return a map of grouped remote and local indices
     */
    Map<String, List<String>> groupClusterIndices(String[] requestIndices, Predicate<String> indexExists) {
        Map<String, List<String>> perClusterIndices = new HashMap<>();
        Set<String> remoteClusterNames = this.remoteClusters.keySet();
        for (String index : requestIndices) {
            int i = index.indexOf(REMOTE_CLUSTER_INDEX_SEPARATOR);
            if (i >= 0) {
                String remoteClusterName = index.substring(0, i);
                List<String> clusters = clusterNameResolver.resolveClusterNames(remoteClusterNames, remoteClusterName);
                if (clusters.isEmpty() == false) {
                    if (indexExists.test(index)) {
                        // we use : as a separator for remote clusters. might conflict if there is an index that is actually named
                        // remote_cluster_alias:index_name - for this case we fail the request. the user can easily change the cluster alias
                        // if that happens
                        throw new IllegalArgumentException("Can not filter indices; index " + index +
                            " exists but there is also a remote cluster named: " + remoteClusterName);
                        }
                    String indexName = index.substring(i + 1);
                    for (String clusterName : clusters) {
                        perClusterIndices.computeIfAbsent(clusterName, k -> new ArrayList<>()).add(indexName);
                    }
                } else {
                    perClusterIndices.computeIfAbsent(LOCAL_CLUSTER_GROUP_KEY, k -> new ArrayList<>()).add(index);
                }
            } else {
                perClusterIndices.computeIfAbsent(LOCAL_CLUSTER_GROUP_KEY, k -> new ArrayList<>()).add(index);
            }
        }
        return perClusterIndices;
}

    /**
     * Returns <code>true</code> iff the given cluster is configured as a remote cluster. Otherwise <code>false</code>
     */
    boolean isRemoteClusterRegistered(String clusterName) {
        return remoteClusters.containsKey(clusterName);
    }

    void collectSearchShards(SearchRequest searchRequest, Map<String, OriginalIndices> remoteIndicesByCluster,
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
            remoteClusterConnection.fetchSearchShards(searchRequest, indices,
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

    Function<String, Transport.Connection> processRemoteShards(Map<String, ClusterSearchShardsResponse> searchShardsResponses,
                                                               Map<String, OriginalIndices> remoteIndicesByCluster,
                                                               List<SearchShardIterator> remoteShardIterators,
                                                               Map<String, AliasFilter> aliasFilterMap) {
        Map<String, Supplier<Transport.Connection>> nodeToCluster = new HashMap<>();
        for (Map.Entry<String, ClusterSearchShardsResponse> entry : searchShardsResponses.entrySet()) {
            String clusterAlias = entry.getKey();
            ClusterSearchShardsResponse searchShardsResponse = entry.getValue();
            for (DiscoveryNode remoteNode : searchShardsResponse.getNodes()) {
                nodeToCluster.put(remoteNode.getId(), () -> getConnection(remoteNode, clusterAlias));
            }
            Map<String, AliasFilter> indicesAndFilters = searchShardsResponse.getIndicesAndFilters();
            for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
                //add the cluster name to the remote index names for indices disambiguation
                //this ends up in the hits returned with the search response
                ShardId shardId = clusterSearchShardsGroup.getShardId();
                Index remoteIndex = shardId.getIndex();
                Index index = new Index(clusterAlias + REMOTE_CLUSTER_INDEX_SEPARATOR + remoteIndex.getName(), remoteIndex.getUUID());
                OriginalIndices originalIndices = remoteIndicesByCluster.get(clusterAlias);
                assert originalIndices != null;
                SearchShardIterator shardIterator = new SearchShardIterator(new ShardId(index, shardId.getId()),
                    Arrays.asList(clusterSearchShardsGroup.getShards()), originalIndices);
                remoteShardIterators.add(shardIterator);
                AliasFilter aliasFilter;
                if (indicesAndFilters == null) {
                    aliasFilter = new AliasFilter(null, Strings.EMPTY_ARRAY);
                } else {
                    aliasFilter = indicesAndFilters.get(shardId.getIndexName());
                    assert aliasFilter != null;
                }
                // here we have to map the filters to the UUID since from now on we use the uuid for the lookup
                aliasFilterMap.put(remoteIndex.getUUID(), aliasFilter);
            }
        }
        return (nodeId) -> {
            Supplier<Transport.Connection> supplier = nodeToCluster.get(nodeId);
            if (supplier == null) {
                throw new IllegalArgumentException("unknown remote node: " + nodeId);
            }
            return supplier.get();
        };
    }

    /**
     * Returns a connection to the given node on the given remote cluster
     * @throws IllegalArgumentException if the remote cluster is unknown
     */
    private Transport.Connection getConnection(DiscoveryNode node, String cluster) {
        RemoteClusterConnection connection = remoteClusters.get(cluster);
        if (connection == null) {
            throw new IllegalArgumentException("no such remote cluster: " + cluster);
        }
        return connection.getConnection(node);
    }

    void updateRemoteCluster(String clusterAlias, List<InetSocketAddress> addresses) {
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

    static Map<String, List<DiscoveryNode>> buildRemoteClustersSeeds(Settings settings) {
        Stream<Setting<List<InetSocketAddress>>> allConcreteSettings = REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(settings);
        return allConcreteSettings.collect(
            Collectors.toMap(REMOTE_CLUSTERS_SEEDS::getNamespace,  concreteSetting -> {
            String clusterName = REMOTE_CLUSTERS_SEEDS.getNamespace(concreteSetting);
            List<DiscoveryNode> nodes = new ArrayList<>();
            for (InetSocketAddress address : concreteSetting.get(settings)) {
                TransportAddress transportAddress = new TransportAddress(address);
                DiscoveryNode node = new DiscoveryNode(clusterName + "#" + transportAddress.toString(),
                    transportAddress,
                    Version.CURRENT.minimumCompatibilityVersion());
                nodes.add(node);
            }
            return nodes;
        }));
    }

    private static InetSocketAddress parseSeedAddress(String remoteHost) {
        int portSeparator = remoteHost.lastIndexOf(':'); // in case we have a IPv6 address ie. [::1]:9300
        if (portSeparator == -1 || portSeparator == remoteHost.length()) {
            throw new IllegalArgumentException("remote hosts need to be configured as [host:port], found [" + remoteHost + "] instead");
        }
        String host = remoteHost.substring(0, portSeparator);
        InetAddress hostAddress;
        try {
            hostAddress = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("unknown host [" + host + "]", e);
        }
        try {
            int port = Integer.valueOf(remoteHost.substring(portSeparator + 1));
            if (port <= 0) {
                throw new IllegalArgumentException("port number must be > 0 but was: [" + port + "]");
            }
            return new InetSocketAddress(hostAddress, port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("port must be a number", e);
        }

    }

    /**
     * Connects to all remote clusters in a blocking fashion. This should be called on node startup to establish an initial connection
     * to all configured seed nodes.
     */
    void initializeRemoteClusters() {
        final TimeValue timeValue = REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(settings);
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        Map<String, List<DiscoveryNode>> seeds = buildRemoteClustersSeeds(settings);
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
}
