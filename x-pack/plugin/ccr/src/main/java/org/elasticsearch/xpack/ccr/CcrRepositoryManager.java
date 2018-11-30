/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteInternalRepositoryAction;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteInternalRepositoryRequest;
import org.elasticsearch.xpack.ccr.action.repositories.PutInternalRepositoryAction;
import org.elasticsearch.xpack.ccr.action.repositories.PutInternalRepositoryRequest;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;

import java.util.List;

class CcrRepositoryManager extends RemoteClusterAware {

    private final NodeClient client;

    CcrRepositoryManager(Settings settings, ClusterService clusterService, NodeClient client) {
        super(settings);
        this.client = client;
        listenForUpdates(clusterService.getClusterSettings());
    }

    @Override
    protected void updateRemoteCluster(String clusterAlias, List<String> addresses, String proxyAddress) {
        if (addresses.isEmpty()) {
            DeleteInternalRepositoryRequest request = new DeleteInternalRepositoryRequest(clusterAlias);
            PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();
            client.executeLocally(DeleteInternalRepositoryAction.INSTANCE, request, future);
            assert future.isDone() : "Should be completed as it is executed synchronously";
        } else {
            ActionRequest request = new PutInternalRepositoryRequest(clusterAlias, CcrRepository.TYPE);
            PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();
            client.executeLocally(PutInternalRepositoryAction.INSTANCE, request, future);
            assert future.isDone() : "Should be completed as it is executed synchronously";
        }
    }
}
