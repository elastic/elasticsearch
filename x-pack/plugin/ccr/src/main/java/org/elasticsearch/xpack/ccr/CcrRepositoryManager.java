/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteInternalCcrRepositoryAction;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteInternalCcrRepositoryRequest;
import org.elasticsearch.xpack.ccr.action.repositories.PutInternalCcrRepositoryAction;
import org.elasticsearch.xpack.ccr.action.repositories.PutInternalCcrRepositoryRequest;
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
            DeleteInternalCcrRepositoryRequest request = new DeleteInternalCcrRepositoryRequest(clusterAlias);
            PlainActionFuture<ActionResponse> future = PlainActionFuture.newFuture();
            client.executeLocally(DeleteInternalCcrRepositoryAction.INSTANCE, request, future);
            assert future.isDone() : "Should be completed as it is executed synchronously";
        } else {
            ActionRequest request = new PutInternalCcrRepositoryRequest(clusterAlias, CcrRepository.TYPE);
            PlainActionFuture<ActionResponse> future = PlainActionFuture.newFuture();
            client.executeLocally(PutInternalCcrRepositoryAction.INSTANCE, request, future);
            assert future.isDone() : "Should be completed as it is executed synchronously";
        }
    }
}
