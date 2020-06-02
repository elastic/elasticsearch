/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteInternalCcrRepositoryAction;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteInternalCcrRepositoryRequest;
import org.elasticsearch.xpack.ccr.action.repositories.PutInternalCcrRepositoryAction;
import org.elasticsearch.xpack.ccr.action.repositories.PutInternalCcrRepositoryRequest;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;

import java.io.IOException;
import java.util.Set;

class CcrRepositoryManager extends AbstractLifecycleComponent {

    private final Client client;
    private final RemoteSettingsUpdateListener updateListener;

    CcrRepositoryManager(Settings settings, ClusterService clusterService, Client client) {
        this.client = client;
        updateListener = new RemoteSettingsUpdateListener(settings);
        updateListener.listenForUpdates(clusterService.getClusterSettings());
    }

    @Override
    protected void doStart() {
        updateListener.init();
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
    }

    private void putRepository(String repositoryName) {
        ActionRequest request = new PutInternalCcrRepositoryRequest(repositoryName, CcrRepository.TYPE);
        PlainActionFuture<PutInternalCcrRepositoryAction.PutInternalCcrRepositoryResponse> f = PlainActionFuture.newFuture();
        client.execute(PutInternalCcrRepositoryAction.INSTANCE, request, f);
        assert f.isDone() : "Should be completed as it is executed synchronously";
    }

    private void deleteRepository(String repositoryName) {
        DeleteInternalCcrRepositoryRequest request = new DeleteInternalCcrRepositoryRequest(repositoryName);
        PlainActionFuture<DeleteInternalCcrRepositoryAction.DeleteInternalCcrRepositoryResponse> f = PlainActionFuture.newFuture();
        client.execute(DeleteInternalCcrRepositoryAction.INSTANCE, request, f);
        assert f.isDone() : "Should be completed as it is executed synchronously";
    }

    private class RemoteSettingsUpdateListener extends RemoteClusterAware {

        private RemoteSettingsUpdateListener(Settings settings) {
            super(settings);
        }

        void init() {
            Set<String> clusterAliases = getEnabledRemoteClusters(settings);
            for (String clusterAlias : clusterAliases) {
                putRepository(CcrRepository.NAME_PREFIX + clusterAlias);
            }
        }

        @Override
        protected void updateRemoteCluster(String clusterAlias, Settings settings) {
            String repositoryName = CcrRepository.NAME_PREFIX + clusterAlias;
            if (RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, settings)) {
                putRepository(repositoryName);
            } else {
                deleteRepository(repositoryName);
            }
        }
    }
}
