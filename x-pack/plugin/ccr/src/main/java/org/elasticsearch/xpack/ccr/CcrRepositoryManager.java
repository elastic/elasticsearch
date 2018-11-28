/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;

import java.util.List;
import java.util.Set;

class CcrRepositoryManager extends RemoteClusterAware {

    private final RepositoriesService repositoriesService;
    private final Set<String> clusters = ConcurrentCollections.newConcurrentSet();

    CcrRepositoryManager(Settings settings, ClusterService clusterService, RepositoriesService repositoriesService) {
        super(settings);
        this.repositoriesService = repositoriesService;
        clusters.addAll(buildRemoteClustersDynamicConfig(settings).keySet());
        listenForUpdates(clusterService.getClusterSettings());
    }

    @Override
    protected Set<String> getRemoteClusterNames() {
        return clusters;
    }

    @Override
    protected void updateRemoteCluster(String clusterAlias, List<String> addresses, String proxyAddress) {
        if (addresses.isEmpty()) {
            if (clusters.remove(clusterAlias)) {
                repositoriesService.unregisterInternalRepository(clusterAlias);
            }
        } else {
            if (clusters.add(clusterAlias)) {
                RepositoryMetaData metaData = new RepositoryMetaData(clusterAlias, CcrRepository.TYPE, settings);
                repositoriesService.registerInternalRepository(clusterAlias, new CcrRepository(metaData, settings));
            }
        }
    }
}
