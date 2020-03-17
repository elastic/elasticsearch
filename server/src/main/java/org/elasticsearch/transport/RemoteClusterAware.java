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

import org.elasticsearch.cluster.metadata.ClusterNameExpressionResolver;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Base class for all services and components that need up-to-date information about the registered remote clusters
 */
public abstract class RemoteClusterAware {

    public static final char REMOTE_CLUSTER_INDEX_SEPARATOR = ':';
    public static final String LOCAL_CLUSTER_GROUP_KEY = "";

    protected final Settings settings;
    private final ClusterNameExpressionResolver clusterNameResolver;

    /**
     * Creates a new {@link RemoteClusterAware} instance
     *
     * @param settings the nodes level settings
     */
    protected RemoteClusterAware(Settings settings) {
        this.settings = settings;
        this.clusterNameResolver = new ClusterNameExpressionResolver();
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
     * @param remoteClusterNames the remote cluster names
     * @param requestIndices     the indices in the search request to filter
     * @param indexExists        a predicate that can test if a certain index or alias exists in the local cluster
     * @return a map of grouped remote and local indices
     */
    protected Map<String, List<String>> groupClusterIndices(Set<String> remoteClusterNames, String[] requestIndices,
                                                            Predicate<String> indexExists) {
        Map<String, List<String>> perClusterIndices = new HashMap<>();
        for (String index : requestIndices) {
            int i = index.indexOf(RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR);
            if (i >= 0) {
                String remoteClusterName = index.substring(0, i);
                List<String> clusters = clusterNameResolver.resolveClusterNames(remoteClusterNames, remoteClusterName);
                if (clusters.isEmpty() == false) {
                    if (indexExists.test(index)) {
                        //We use ":" as a separator for remote clusters. There may be a conflict if there is an index that is named
                        //remote_cluster_alias:index_name - for this case we fail the request. The user can easily change the cluster alias
                        //if that happens. Note that indices and aliases can be created with ":" in their names names up to 6.last, which
                        //means such names need to be supported until 7.last. It will be possible to remove this check from 8.0 on.
                        throw new IllegalArgumentException("Can not filter indices; index " + index +
                            " exists but there is also a remote cluster named: " + remoteClusterName);
                    }
                    String indexName = index.substring(i + 1);
                    for (String clusterName : clusters) {
                        perClusterIndices.computeIfAbsent(clusterName, k -> new ArrayList<>()).add(indexName);
                    }
                } else {
                    //Indices and aliases can be created with ":" in their names up to 6.last (although deprecated), and still be
                    //around in 7.x. That's why we need to be lenient here and treat the index as local although it contains ":".
                    //It will be possible to remove such leniency and assume that no local indices contain ":" only from 8.0 on.
                    perClusterIndices.computeIfAbsent(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, k -> new ArrayList<>()).add(index);
                }
            } else {
                perClusterIndices.computeIfAbsent(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, k -> new ArrayList<>()).add(index);
            }
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
        List<Setting.AffixSetting<?>> remoteClusterSettings = Arrays.asList(
            RemoteClusterService.REMOTE_CLUSTER_COMPRESS,
            RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE,
            RemoteConnectionStrategy.REMOTE_CONNECTION_MODE,
            SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_SEEDS,
            SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_PROXY,
            SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY,
            SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS,
            SniffConnectionStrategy.REMOTE_NODE_CONNECTIONS,
            ProxyConnectionStrategy.PROXY_ADDRESS,
            ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS,
            ProxyConnectionStrategy.SERVER_NAME);
        clusterSettings.addAffixGroupUpdateConsumer(remoteClusterSettings, this::validateAndUpdateRemoteCluster);
    }

    public static String buildRemoteIndexName(String clusterAlias, String indexName) {
        return clusterAlias == null || LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)
            ? indexName : clusterAlias + REMOTE_CLUSTER_INDEX_SEPARATOR + indexName;
    }
}
