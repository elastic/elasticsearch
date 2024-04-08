/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.metadata.ClusterNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.node.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for all services and components that need up-to-date information about the registered remote clusters
 */
public abstract class RemoteClusterAware {
    public static final char REMOTE_CLUSTER_INDEX_SEPARATOR = ':';
    public static final String LOCAL_CLUSTER_GROUP_KEY = "";

    protected final Settings settings;
    private final String nodeName;
    private final boolean isRemoteClusterClientEnabled;

    /**
     * Creates a new {@link RemoteClusterAware} instance
     * @param settings the nodes level settings
     */
    protected RemoteClusterAware(Settings settings) {
        this.settings = settings;
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.isRemoteClusterClientEnabled = DiscoveryNode.isRemoteClusterClient(settings);
    }

    /**
     * Returns remote clusters that are enabled in these settings
     */
    protected static Set<String> getEnabledRemoteClusters(final Settings settings) {
        return RemoteConnectionStrategy.getRemoteClusters(settings);
    }

    /**
     * Groups indices per cluster by splitting remote cluster-alias, index-name pairs on {@link #REMOTE_CLUSTER_INDEX_SEPARATOR}. All
     * indices per cluster are collected as a list in the returned map keyed by the cluster alias. Local indices are grouped under
     * {@link #LOCAL_CLUSTER_GROUP_KEY}. The returned map is mutable.
     *
     * This method supports excluding clusters by using the {@code -cluster:*} index expression.
     * For example, if requestIndices is [blogs, *:blogs, -remote1:*] and *:blogs resolves to "remote1:blogs, remote2:blogs"
     * the map returned by the function will not have the remote1 entry. It will have only {"":blogs, remote2:blogs}.
     * The index for the excluded cluster must be '*' to clarify that the entire cluster should be removed.
     * A wildcard in the "-" excludes notation is also allowed. For example, suppose there are three remote clusters,
     * remote1, remote2, remote3, and this index expression is provided: blogs,rem*:blogs,-rem*1:*. That would successfully
     * remove remote1 from the list of clusters to be included.
     *
     * @param remoteClusterNames the remote cluster names. If a clusterAlias is preceded by a minus sign that cluster will be excluded.
     * @param requestIndices the indices in the search request to filter
     *
     * @return a map of grouped remote and local indices
     */
    protected Map<String, List<String>> groupClusterIndices(Set<String> remoteClusterNames, String[] requestIndices) {
        Map<String, List<String>> perClusterIndices = new HashMap<>();
        Set<String> clustersToRemove = new HashSet<>();
        for (String index : requestIndices) {
            // ensure that `index` is a remote name and not a datemath expression which includes ':' symbol
            // since datemath expression after evaluation should not contain ':' symbol
            String probe = IndexNameExpressionResolver.resolveDateMathExpression(index);
            int i = probe.indexOf(RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR);
            if (i >= 0) {
                if (isRemoteClusterClientEnabled == false) {
                    assert remoteClusterNames.isEmpty() : remoteClusterNames;
                    throw new IllegalArgumentException("node [" + nodeName + "] does not have the remote cluster client role enabled");
                }
                int startIdx = index.charAt(0) == '-' ? 1 : 0;
                String remoteClusterName = index.substring(startIdx, i);
                List<String> clusters = ClusterNameExpressionResolver.resolveClusterNames(remoteClusterNames, remoteClusterName);
                String indexName = index.substring(i + 1);
                if (startIdx == 1) {
                    if (indexName.equals("*") == false) {
                        throw new IllegalArgumentException(
                            Strings.format(
                                "To exclude a cluster you must specify the '*' wildcard for " + "the index expression, but found: [%s]",
                                indexName
                            )
                        );
                    }
                    clustersToRemove.addAll(clusters);
                } else {
                    for (String clusterName : clusters) {
                        perClusterIndices.computeIfAbsent(clusterName, k -> new ArrayList<>()).add(indexName);
                    }
                }
            } else {
                perClusterIndices.computeIfAbsent(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, k -> new ArrayList<>()).add(index);
            }
        }
        List<String> excludeFailed = new ArrayList<>();
        for (String exclude : clustersToRemove) {
            List<String> removed = perClusterIndices.remove(exclude);
            if (removed == null) {
                excludeFailed.add(exclude);
            }
        }
        if (excludeFailed.size() > 0) {
            String warning = Strings.format(
                "Attempt to exclude cluster%s %s failed as %s not included in the list of clusters to be included: %s. Input: [%s]",
                excludeFailed.size() == 1 ? "" : "s",
                excludeFailed,
                excludeFailed.size() == 1 ? "it is" : "they are",
                perClusterIndices.keySet().stream().map(s -> s.equals("") ? "(local)" : s).collect(Collectors.toList()),
                String.join(",", requestIndices)
            );
            throw new IllegalArgumentException(warning);
        }
        if (clustersToRemove.size() > 0 && perClusterIndices.size() == 0) {
            throw new IllegalArgumentException(
                "The '-' exclusions in the index expression list excludes all indexes. Nothing to search. Input: ["
                    + String.join(",", requestIndices)
                    + "]"
            );
        }
        return perClusterIndices;
    }

    void validateAndUpdateRemoteCluster(String clusterAlias, Settings settings) {
        if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)) {
            throw new IllegalArgumentException("remote clusters must not have the empty string as its key");
        }
        updateRemoteCluster(clusterAlias, settings);
    }

    /**
     * Subclasses must implement this to receive information about updated cluster aliases.
     */
    protected abstract void updateRemoteCluster(String clusterAlias, Settings settings);

    /**
     * Registers this instance to listen to updates on the cluster settings.
     */
    public void listenForUpdates(ClusterSettings clusterSettings) {
        List<Setting.AffixSetting<?>> remoteClusterSettings = List.of(
            RemoteClusterService.REMOTE_CLUSTER_COMPRESS,
            RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE,
            RemoteConnectionStrategy.REMOTE_CONNECTION_MODE,
            SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY,
            SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS,
            SniffConnectionStrategy.REMOTE_NODE_CONNECTIONS,
            ProxyConnectionStrategy.PROXY_ADDRESS,
            ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS,
            ProxyConnectionStrategy.SERVER_NAME
        );
        clusterSettings.addAffixGroupUpdateConsumer(remoteClusterSettings, this::validateAndUpdateRemoteCluster);
    }

    public static String buildRemoteIndexName(String clusterAlias, String indexName) {
        return clusterAlias == null || LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)
            ? indexName
            : clusterAlias + REMOTE_CLUSTER_INDEX_SEPARATOR + indexName;
    }
}
