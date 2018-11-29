/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteInternalRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutInternalRepositoryAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;

import java.util.List;
import java.util.Set;

class CcrRepositoryManager extends RemoteClusterAware {

    private final NodeClient client;
    private final Set<String> clusters = ConcurrentCollections.newConcurrentSet();

    CcrRepositoryManager(Settings settings, ClusterService clusterService, NodeClient client) {
        super(settings);
        this.client = client;
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
                DeleteInternalRepositoryAction.DeleteInternalRepositoryRequest request =
                    new DeleteInternalRepositoryAction.DeleteInternalRepositoryRequest(clusterAlias);
                PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();
                client.executeLocally(DeleteInternalRepositoryAction.INSTANCE, request, future);
                assert future.isDone() : "Should be completed as it is executed synchronously";
            }
        } else {
            if (clusters.add(clusterAlias)) {
                ActionRequest request = new PutInternalRepositoryAction.PutInternalRepositoryRequest(clusterAlias, CcrRepository.TYPE);
                PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();
                client.executeLocally(PutInternalRepositoryAction.INSTANCE, request, future);
                assert future.isDone() : "Should be completed as it is executed synchronously";
            }
        }
    }
}
