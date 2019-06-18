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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for all services and components that need up-to-date information about the registered remote clusters
 */
public abstract class RemoteClusterAware {

    /**
     * A list of initial seed nodes to discover eligible nodes from the remote cluster
     */
    public static final Setting.AffixSetting<List<String>> REMOTE_CLUSTERS_SEEDS = Setting.affixKeySetting(
            "cluster.remote.",
            "seeds",
            key -> Setting.listSetting(
                    key,
                    Collections.emptyList(),
                    s -> {
                        // validate seed address
                        parsePort(s);
                        return s;
                    },
                    Setting.Property.Dynamic,
                    Setting.Property.NodeScope));

    public static final char REMOTE_CLUSTER_INDEX_SEPARATOR = ':';
    public static final String LOCAL_CLUSTER_GROUP_KEY = "";

    /**
     * A proxy address for the remote cluster.
     */
    public static final Setting.AffixSetting<String> REMOTE_CLUSTERS_PROXY = Setting.affixKeySetting(
            "cluster.remote.",
            "proxy",
            key -> Setting.simpleString(
                    key,
                    s -> {
                        if (Strings.hasLength(s)) {
                            parsePort(s);
                        }
                    },
                    Setting.Property.Dynamic,
                    Setting.Property.NodeScope),
            REMOTE_CLUSTERS_SEEDS);

    protected final Settings settings;
    private final ClusterNameExpressionResolver clusterNameResolver;

    /**
     * Creates a new {@link RemoteClusterAware} instance
     * @param settings the nodes level settings
     */
    protected RemoteClusterAware(Settings settings) {
        this.settings = settings;
        this.clusterNameResolver = new ClusterNameExpressionResolver();
    }

    /**
     * Builds the dynamic per-cluster config from the given settings. This is a map keyed by the cluster alias that points to a tuple
     * (ProxyAddresss, [SeedNodeSuppliers]). If a cluster is configured with a proxy address all seed nodes will point to
     * {@link TransportAddress#META_ADDRESS} and their configured address will be used as the hostname for the generated discovery node.
     */
    protected static Map<String, Tuple<String, List<Tuple<String, Supplier<DiscoveryNode>>>>> buildRemoteClustersDynamicConfig(
            final Settings settings) {
        final Map<String, Tuple<String, List<Tuple<String, Supplier<DiscoveryNode>>>>> remoteSeeds =
                buildRemoteClustersDynamicConfig(settings, REMOTE_CLUSTERS_SEEDS);
        return remoteSeeds.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, Tuple<String, List<Tuple<String, Supplier<DiscoveryNode>>>>> buildRemoteClustersDynamicConfig(
            final Settings settings, final Setting.AffixSetting<List<String>> seedsSetting) {
        final Stream<Setting<List<String>>> allConcreteSettings = seedsSetting.getAllConcreteSettings(settings);
        return allConcreteSettings.collect(
                Collectors.toMap(seedsSetting::getNamespace, concreteSetting -> {
                    String clusterName = seedsSetting.getNamespace(concreteSetting);
                    List<String> addresses = concreteSetting.get(settings);
                    final boolean proxyMode =
                            REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(clusterName).existsOrFallbackExists(settings);
                    List<Tuple<String, Supplier<DiscoveryNode>>> nodes = new ArrayList<>(addresses.size());
                    for (String address : addresses) {
                        nodes.add(Tuple.tuple(address, () -> buildSeedNode(clusterName, address, proxyMode)));
                    }
                    return new Tuple<>(REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(clusterName).get(settings), nodes);
                }));
    }

    static DiscoveryNode buildSeedNode(String clusterName, String address, boolean proxyMode) {
        if (proxyMode) {
            TransportAddress transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 0);
            String hostName = address.substring(0, indexOfPortSeparator(address));
            return new DiscoveryNode("", clusterName + "#" + address, UUIDs.randomBase64UUID(), hostName, address,
                    transportAddress, Collections.singletonMap("server_name", hostName), DiscoveryNodeRole.BUILT_IN_ROLES,
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
     * @param remoteClusterNames the remote cluster names
     * @param requestIndices the indices in the search request to filter
     *
     * @return a map of grouped remote and local indices
     */
    protected Map<String, List<String>> groupClusterIndices(Set<String> remoteClusterNames, String[] requestIndices) {
        Map<String, List<String>> perClusterIndices = new HashMap<>();
        for (String index : requestIndices) {
            int i = index.indexOf(RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR);
            if (i >= 0) {
                String remoteClusterName = index.substring(0, i);
                List<String> clusters = clusterNameResolver.resolveClusterNames(remoteClusterNames, remoteClusterName);
                String indexName = index.substring(i + 1);
                for (String clusterName : clusters) {
                    perClusterIndices.computeIfAbsent(clusterName, k -> new ArrayList<>()).add(indexName);
                }
            } else {
                perClusterIndices.computeIfAbsent(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, k -> new ArrayList<>()).add(index);
            }
        }
        return perClusterIndices;
    }

    void updateRemoteCluster(String clusterAlias, List<String> addresses, String proxy) {
        Boolean compress = TransportSettings.TRANSPORT_COMPRESS.get(settings);
        TimeValue pingSchedule = TransportSettings.PING_SCHEDULE.get(settings);
        updateRemoteCluster(clusterAlias, addresses, proxy, compress, pingSchedule);
    }

    void updateRemoteCluster(String clusterAlias, Settings settings) {
        String proxy = REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(clusterAlias).get(settings);
        List<String> addresses = REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace(clusterAlias).get(settings);
        Boolean compress = RemoteClusterService.REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace(clusterAlias).get(settings);
        TimeValue pingSchedule = RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE
            .getConcreteSettingForNamespace(clusterAlias)
            .get(settings);

        updateRemoteCluster(clusterAlias, addresses, proxy, compress, pingSchedule);
    }

    /**
     * Subclasses must implement this to receive information about updated cluster aliases. If the given address list is
     * empty the cluster alias is unregistered and should be removed.
     */
    protected abstract void updateRemoteCluster(String clusterAlias, List<String> addresses, String proxy, boolean compressionEnabled,
                                                TimeValue pingSchedule);

    /**
     * Registers this instance to listen to updates on the cluster settings.
     */
    public void listenForUpdates(ClusterSettings clusterSettings) {
        List<Setting.AffixSetting<?>> remoteClusterSettings = Arrays.asList(RemoteClusterAware.REMOTE_CLUSTERS_PROXY,
            RemoteClusterAware.REMOTE_CLUSTERS_SEEDS, RemoteClusterService.REMOTE_CLUSTER_COMPRESS,
            RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE);
        clusterSettings.addAffixGroupUpdateConsumer(remoteClusterSettings, this::updateRemoteCluster);
    }

    static InetSocketAddress parseSeedAddress(String remoteHost) {
        final Tuple<String, Integer> hostPort = parseHostPort(remoteHost);
        final String host = hostPort.v1();
        assert hostPort.v2() != null : remoteHost;
        final int port = hostPort.v2();
        InetAddress hostAddress;
        try {
            hostAddress = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("unknown host [" + host + "]", e);
        }
        return new InetSocketAddress(hostAddress, port);
    }

    public static Tuple<String, Integer> parseHostPort(final String remoteHost) {
        final String host = remoteHost.substring(0, indexOfPortSeparator(remoteHost));
        final int port = parsePort(remoteHost);
        return Tuple.tuple(host, port);
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
        return clusterAlias == null || LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)
            ? indexName : clusterAlias + REMOTE_CLUSTER_INDEX_SEPARATOR + indexName;
    }
}
