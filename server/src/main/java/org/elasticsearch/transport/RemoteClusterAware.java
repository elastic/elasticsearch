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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.ClusterNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for all services and components that need up-to-date information about the registered remote clusters
 */
public abstract class RemoteClusterAware extends AbstractComponent {

    /**
     * A list of initial seed nodes to discover eligible nodes from the remote cluster
     */
    public static final Setting.AffixSetting<List<InetSocketAddress>> REMOTE_CLUSTERS_SEEDS = Setting.affixKeySetting("search.remote.",
        "seeds", (key) -> Setting.listSetting(key, Collections.emptyList(), RemoteClusterAware::parseSeedAddress,
            Setting.Property.NodeScope, Setting.Property.Dynamic));
    public static final char REMOTE_CLUSTER_INDEX_SEPARATOR = ':';
    public static final String LOCAL_CLUSTER_GROUP_KEY = "";

    protected final ClusterNameExpressionResolver clusterNameResolver;

    /**
     * Creates a new {@link RemoteClusterAware} instance
     * @param settings the nodes level settings
     */
    protected RemoteClusterAware(Settings settings) {
        super(settings);
        this.clusterNameResolver = new ClusterNameExpressionResolver(settings);
    }

    protected static Map<String, List<DiscoveryNode>> buildRemoteClustersSeeds(Settings settings) {
        Stream<Setting<List<InetSocketAddress>>> allConcreteSettings = REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(settings);
        return allConcreteSettings.collect(
            Collectors.toMap(REMOTE_CLUSTERS_SEEDS::getNamespace, concreteSetting -> {
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

    /**
     * Groups indices per cluster by splitting remote cluster-alias, index-name pairs on {@link #REMOTE_CLUSTER_INDEX_SEPARATOR}. All
     * indices per cluster are collected as a list in the returned map keyed by the cluster alias. Local indices are grouped under
     * {@link #LOCAL_CLUSTER_GROUP_KEY}. The returned map is mutable.
     *
     * @param requestIndices the indices in the search request to filter
     * @param indexExists a predicate that can test if a certain index or alias exists in the local cluster
     *
     * @return a map of grouped remote and local indices
     */
    public Map<String, List<String>> groupClusterIndices(String[] requestIndices, Predicate<String> indexExists) {
        Map<String, List<String>> perClusterIndices = new HashMap<>();
        Set<String> remoteClusterNames = getRemoteClusterNames();
        for (String index : requestIndices) {
            int i = index.indexOf(RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR);
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
                    perClusterIndices.computeIfAbsent(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, k -> new ArrayList<>()).add(index);
                }
            } else {
                perClusterIndices.computeIfAbsent(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, k -> new ArrayList<>()).add(index);
            }
        }
        return perClusterIndices;
    }

    protected abstract Set<String> getRemoteClusterNames();

    /**
     * Subclasses must implement this to receive information about updated cluster aliases. If the given address list is
     * empty the cluster alias is unregistered and should be removed.
     */
    protected abstract void updateRemoteCluster(String clusterAlias, List<InetSocketAddress> addresses);

    /**
     * Registers this instance to listen to updates on the cluster settings.
     */
    public void listenForUpdates(ClusterSettings clusterSettings) {
        clusterSettings.addAffixUpdateConsumer(RemoteClusterAware.REMOTE_CLUSTERS_SEEDS, this::updateRemoteCluster,
            (namespace, value) -> {});
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

    public static String buildRemoteIndexName(String clusterAlias, String indexName) {
        return clusterAlias != null ? clusterAlias + REMOTE_CLUSTER_INDEX_SEPARATOR + indexName : indexName;
    }
}
