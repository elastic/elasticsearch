/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Set;

public class RemoteClusterRegistry {

    private final RemoteClusterService remoteClusterService;
    private final IndicesOptions indicesOptions;

    public RemoteClusterRegistry(RemoteClusterService remoteClusterService, IndicesOptions indicesOptions) {
        this.remoteClusterService = remoteClusterService;
        this.indicesOptions = indicesOptions;
    }

    public Set<String> clusterAliases(String[] indices, boolean discardLocal) {
        Set<String> clusters = remoteClusterService.groupIndices(indicesOptions, indices).keySet();
        if (discardLocal) {
            clusters.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        }
        return clusters;
    }
}
