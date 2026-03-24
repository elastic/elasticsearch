/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.LinkedProjectConfig;
import org.elasticsearch.transport.LinkedProjectConfigService;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Maintain a list of remote clusters (aliases) and provide the ability to resolve.
 */
class RemoteClusterResolver extends RemoteClusterAware {

    private final CopyOnWriteArraySet<String> clusters;

    static class ResolvedIndices {
        private final Map<String, List<String>> remoteIndicesPerClusterAlias;
        private final List<String> localIndices;

        ResolvedIndices(Map<String, List<String>> remoteIndicesPerClusterAlias, List<String> localIndices) {
            this.localIndices = localIndices;
            this.remoteIndicesPerClusterAlias = remoteIndicesPerClusterAlias;
        }

        Map<String, List<String>> getRemoteIndicesPerClusterAlias() {
            return remoteIndicesPerClusterAlias;
        }

        List<String> getLocalIndices() {
            return localIndices;
        }

        int numClusters() {
            return remoteIndicesPerClusterAlias.size() + (localIndices.isEmpty() ? 0 : 1);
        }
    }

    RemoteClusterResolver(Settings settings, LinkedProjectConfigService linkedProjectConfigService) {
        super(settings);
        clusters = new CopyOnWriteArraySet<>(
            linkedProjectConfigService.getInitialLinkedProjectConfigs().stream().map(LinkedProjectConfig::linkedProjectAlias).toList()
        );
        linkedProjectConfigService.register(this);
    }

    @Override
    public void updateLinkedProject(LinkedProjectConfig config) {
        clusters.add(config.linkedProjectAlias());
    }

    @Override
    public void remove(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
        clusters.remove(linkedProjectAlias);
    }

    ResolvedIndices resolve(String... indices) {
        Map<String, List<String>> resolvedClusterIndices = groupClusterIndices(clusters, indices);
        List<String> localIndices = resolvedClusterIndices.getOrDefault(LOCAL_CLUSTER_GROUP_KEY, Collections.emptyList());
        resolvedClusterIndices.remove(LOCAL_CLUSTER_GROUP_KEY);
        return new ResolvedIndices(resolvedClusterIndices, localIndices);
    }
}
