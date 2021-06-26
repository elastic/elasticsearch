/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class RemoteClusterRegistry {

    private final RemoteClusterService remoteClusterService;
    private final IndicesOptions indicesOptions;

    public RemoteClusterRegistry(RemoteClusterService remoteClusterService, IndicesOptions indicesOptions) {
        this.remoteClusterService = remoteClusterService;
        this.indicesOptions = indicesOptions;
    }

    public Set<String> versionIncompatibleClusters(String indexPattern) {
        Set<String> incompatibleClusters = new TreeSet<>();
        for (String clusterAlias: indicesPerRemoteCluster(indexPattern).keySet()) {
            Version clusterVersion = remoteClusterService.getConnection(clusterAlias).getVersion();
            if (clusterVersion.equals(Version.CURRENT) == false) { // TODO: should newer clusters be eventually allowed?
                incompatibleClusters.add(clusterAlias);
            }
        }
        return incompatibleClusters;
    }

    private Map<String, OriginalIndices> indicesPerRemoteCluster(String indexPattern) {
        Map<String, OriginalIndices> indicesMap = remoteClusterService.groupIndices(indicesOptions,
            Strings.splitStringByCommaToArray(indexPattern));
        indicesMap.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        return indicesMap;
    }
}
