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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingUpgrader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for all services and components that need up-to-date information about the registered remote clusters
 */
public abstract class RemoteClusterAware extends AbstractComponent {

    public static final Setting.AffixSetting<List<String>> SEARCH_REMOTE_CLUSTERS_SEEDS =
            Setting.affixKeySetting(
                    "search.remote.",
                    "seeds",
                    key -> Setting.listSetting(
                            key,
                            Collections.emptyList(),
                            s -> {
                                parsePort(s);
                                return s;
                            },
                            Setting.Property.Deprecated,
                            Setting.Property.Dynamic,
                            Setting.Property.NodeScope));

    public static final SettingUpgrader<List<String>> SEARCH_REMOTE_CLUSTER_SEEDS_UPGRADER = new SettingUpgrader<List<String>>() {

        @Override
        public Setting<List<String>> getSetting() {
            return SEARCH_REMOTE_CLUSTERS_SEEDS;
        }

        @Override
        public String getKey(final String key) {
            return key.replaceFirst("^search", "cluster");
        }

    };

    /**
     * A list of initial seed nodes to discover eligible nodes from the remote cluster
     */
    public static final Setting.AffixSetting<List<String>> REMOTE_CLUSTERS_SEEDS = Setting.affixKeySetting(
            "cluster.remote.",
            "seeds",
            key -> Setting.listSetting(
                    key,
                    // the default needs to be emptyList() when fallback is removed
                    "_na_".equals(key)
                            ? SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace(key)
                            : SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSetting(key.replaceAll("^cluster", "search")),
                    s -> {
                        // validate seed address
                        parsePort(s);
                        return s;
                    },
                    Setting.Property.Dynamic,
                    Setting.Property.NodeScope));

    public static final char REMOTE_CLUSTER_INDEX_SEPARATOR = ':';
    public static final String LOCAL_CLUSTER_GROUP_KEY = "";

    public static final Setting.AffixSetting<String> SEARCH_REMOTE_CLUSTERS_PROXY = Setting.affixKeySetting(
            "search.remote.",
            "proxy",
            key -> Setting.simpleString(
                    key,
                    s -> {
                        if (Strings.hasLength(s)) {
                            parsePort(s);
                        }
                        return s;
                    },
                    Setting.Property.Deprecated,
                    Setting.Property.Dynamic,
                    Setting.Property.NodeScope),
            REMOTE_CLUSTERS_SEEDS);

    public static final SettingUpgrader<String> SEARCH_REMOTE_CLUSTERS_PROXY_UPGRADER = new SettingUpgrader<String>() {

        @Override
        public Setting<String> getSetting() {
            return SEARCH_REMOTE_CLUSTERS_PROXY;
        }

        @Override
        public String getKey(final String key) {
            return key.replaceFirst("^search", "cluster");
        }

    };

    /**
     * A proxy address for the remote cluster.
     * NOTE: this settings is undocumented until we have at last one transport that supports passing
     * on the hostname via a mechanism like SNI.
     */
    public static final Setting.AffixSetting<String> REMOTE_CLUSTERS_PROXY = Setting.affixKeySetting(
            "cluster.remote.",
            "proxy",
            key -> Setting.simpleString(
                    key,
                    // no default is needed when fallback is removed, use simple string which gives empty
                    "_na_".equals(key)
                            ? SEARCH_REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(key)
                            : SEARCH_REMOTE_CLUSTERS_PROXY.getConcreteSetting(key.replaceAll("^cluster", "search")),
                    s -> {
                        if (Strings.hasLength(s)) {
                            parsePort(s);
                        }
                        return s;
                    },
                    Setting.Property.Dynamic,
                    Setting.Property.NodeScope),
            REMOTE_CLUSTERS_SEEDS);


    protected final ClusterNameExpressionResolver clusterNameResolver;

    /**
     * Creates a new {@link RemoteClusterAware} instance
     * @param settings the nodes level settings
     */
    protected RemoteClusterAware(Settings settings) {
        super(settings);
        this.clusterNameResolver = new ClusterNameExpressionResolver(settings);
    }

    /**
     * Builds the dynamic per-cluster config from the given settings. This is a map keyed by the cluster alias that points to a tuple
     * (ProxyAddresss, [SeedNodeSuppliers]). If a cluster is configured with a proxy address all seed nodes will point to
     * {@link TransportAddress#META_ADDRESS} and their configured address will be used as the hostname for the generated discovery node.
     */
    protected static Map<String, Tuple<String, List<Supplier<DiscoveryNode>>>> buildRemoteClustersDynamicConfig(Settings settings) {
        Stream<Setting<List<String>>> allConcreteSettings = REMOTE_CLUSTERS_SEEDS.getAllConcreteSettings(settings);
        return allConcreteSettings.collect(
                Collectors.toMap(REMOTE_CLUSTERS_SEEDS::getNamespace, concreteSetting -> {
                    String clusterName = REMOTE_CLUSTERS_SEEDS.getNamespace(concreteSetting);
                    List<String> addresses = concreteSetting.get(settings);
                    final boolean proxyMode = REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(clusterName).exists(settings);
                    List<Supplier<DiscoveryNode>> nodes = new ArrayList<>(addresses.size());
                    for (String address : addresses) {
                        nodes.add(() -> buildSeedNode(clusterName, address, proxyMode));
                    }
                    return new Tuple<>(REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(clusterName).get(settings), nodes);
                }));
    }

    static DiscoveryNode buildSeedNode(String clusterName, String address, boolean proxyMode) {
        if (proxyMode) {
            TransportAddress transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 0);
            String hostName = address.substring(0, indexOfPortSeparator(address));
            return new DiscoveryNode("", clusterName + "#" + address, UUIDs.randomBase64UUID(), hostName, address,
                    transportAddress, Collections
                    .emptyMap(), EnumSet.allOf(DiscoveryNode.Role.class),
                    Version.CURRENT.minimumCompatibilityVersion());
        } else {
            TransportAddress transportAddress = new TransportAddress(RemoteClusterAware.parseSeedAddress(address));
            return new DiscoveryNode(clusterName + "#" + transportAddress.toString(),
                    transportAddress,
                    Version.CURRENT.minimumCompatibilityVersion());
        }
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
    protected abstract void updateRemoteCluster(String clusterAlias, List<String> addresses, String proxy);

    /**
     * Registers this instance to listen to updates on the cluster settings.
     */
    public void listenForUpdates(ClusterSettings clusterSettings) {
        clusterSettings.addAffixUpdateConsumer(
                RemoteClusterAware.REMOTE_CLUSTERS_PROXY,
                RemoteClusterAware.REMOTE_CLUSTERS_SEEDS,
                (key, value) -> updateRemoteCluster(key, value.v2(), value.v1()),
                (namespace, value) -> {});
        clusterSettings.addAffixUpdateConsumer(
                RemoteClusterAware.SEARCH_REMOTE_CLUSTERS_PROXY,
                RemoteClusterAware.SEARCH_REMOTE_CLUSTERS_SEEDS,
                (key, value) -> updateRemoteCluster(key, value.v2(), value.v1()),
                (namespace, value) -> {});
    }


    protected static InetSocketAddress parseSeedAddress(String remoteHost) {
        String host = remoteHost.substring(0, indexOfPortSeparator(remoteHost));
        InetAddress hostAddress;
        try {
            hostAddress = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("unknown host [" + host + "]", e);
        }
        return new InetSocketAddress(hostAddress, parsePort(remoteHost));
    }

    private static int parsePort(String remoteHost) {
        try {
            int port = Integer.valueOf(remoteHost.substring(indexOfPortSeparator(remoteHost) + 1));
            if (port <= 0) {
                throw new IllegalArgumentException("port number must be > 0 but was: [" + port + "]");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse port", e);
        }
    }

    private static int indexOfPortSeparator(String remoteHost) {
        int portSeparator = remoteHost.lastIndexOf(':'); // in case we have a IPv6 address ie. [::1]:9300
        if (portSeparator == -1 || portSeparator == remoteHost.length()) {
            throw new IllegalArgumentException("remote hosts need to be configured as [host:port], found [" + remoteHost + "] instead");
        }
        return portSeparator;
    }

    public static String buildRemoteIndexName(String clusterAlias, String indexName) {
        return clusterAlias != null ? clusterAlias + REMOTE_CLUSTER_INDEX_SEPARATOR + indexName : indexName;
    }

}
