/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Set;
import java.util.TreeSet;

public class RemoteClusterRegistry {

    private final RemoteClusterService remoteClusterService;
    private final IndicesOptions indicesOptions;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterService clusterService;

    public RemoteClusterRegistry(RemoteClusterService remoteClusterService, IndicesOptions indicesOptions, ClusterService clusterService,
                                 IndexNameExpressionResolver indexNameExpressionResolver) {
        this.remoteClusterService = remoteClusterService;
        this.indicesOptions = indicesOptions;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public Set<String> versionIncompatibleClusters(String indexPattern) {
        Set<String> incompatibleClusters = new TreeSet<>();
        for (String clusterAlias: clusterAliases(Strings.splitStringByCommaToArray(indexPattern), true)) {
            Version clusterVersion = remoteClusterService.getConnection(clusterAlias).getVersion();
            if (clusterVersion.equals(Version.CURRENT) == false) { // TODO: should newer clusters be eventually allowed?
                incompatibleClusters.add(clusterAlias);
            }
        }
        return incompatibleClusters;
    }

    public Set<String> clusterAliases(String[] indices, boolean discardLocal) {
        Set<String> clusters = remoteClusterService.groupIndices(indicesOptions, indices,
            idx -> indexNameExpressionResolver.hasIndexAbstraction(idx, clusterService.state())).keySet();
        if (discardLocal) {
            clusters.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        }
        return clusters;
    }
}
