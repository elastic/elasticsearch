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

// Mostly a wrapper for the (final, unmockable) RemoteClusterService.
public class RemoteClusterRegistry {

    private final RemoteClusterService remoteClusterService;
    private final IndicesOptions indicesOptions;

    public RemoteClusterRegistry(RemoteClusterService remoteClusterService, IndicesOptions indicesOptions) {
        this.remoteClusterService = remoteClusterService;
        this.indicesOptions = indicesOptions;
    }

    public Map<String, OriginalIndices> indicesPerRemoteCluster(String indexPattern) {
        Map<String, OriginalIndices> indicesMap = remoteClusterService.groupIndices(indicesOptions,
            Strings.splitStringByCommaToArray(indexPattern));
        indicesMap.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        return indicesMap;
    }

    public Version remoteVersion(String clusterAlias) {
        return remoteClusterService.getConnection(clusterAlias).getVersion();
    }
}
