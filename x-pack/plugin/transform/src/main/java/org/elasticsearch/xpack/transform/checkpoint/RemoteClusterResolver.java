/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteConnectionStrategy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

class RemoteClusterResolver extends RemoteClusterAware {

    private final CopyOnWriteArraySet<String> clusters;

    class ResolvedIndices {
        private final Map<String, List<String>> remoteIndicesPerClusterAlias;
        private final List<String> localIndices;

        ResolvedIndices(Map<String, List<String>> groupClusterIndices) {
            this.localIndices = groupClusterIndices.getOrDefault(LOCAL_CLUSTER_GROUP_KEY, Collections.emptyList());
            groupClusterIndices.remove(LOCAL_CLUSTER_GROUP_KEY);
            this.remoteIndicesPerClusterAlias = groupClusterIndices;
        }

        public Map<String, List<String>> getRemoteIndicesPerClusterAlias() {
            return remoteIndicesPerClusterAlias;
        }

        public List<String> getLocalIndices() {
            return localIndices;
        }

        public int size() {
            return remoteIndicesPerClusterAlias.size() + (localIndices.isEmpty() ? 0 : 1);
        }
    }

    RemoteClusterResolver(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        clusters = new CopyOnWriteArraySet<>(getEnabledRemoteClusters(settings));
        listenForUpdates(clusterSettings);
    }

    @Override
    protected void updateRemoteCluster(String clusterAlias, Settings settings) {
        if (RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, settings)) {
            clusters.add(clusterAlias);
        } else {
            clusters.remove(clusterAlias);
        }
    }

    ResolvedIndices resolve(String... indices) {
        return new ResolvedIndices(groupClusterIndices(clusters, indices));
    }
}
