/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.LinkedProjectConfig;
import org.elasticsearch.transport.LinkedProjectConfigService;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteInternalCcrRepositoryAction;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteInternalCcrRepositoryRequest;
import org.elasticsearch.xpack.ccr.action.repositories.PutInternalCcrRepositoryAction;
import org.elasticsearch.xpack.ccr.action.repositories.PutInternalCcrRepositoryRequest;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;

class CcrRepositoryManager extends AbstractLifecycleComponent {

    private final Client client;
    private final RemoteSettingsUpdateListener updateListener;
    private final LinkedProjectConfigService linkedProjectConfigService;

    CcrRepositoryManager(Settings settings, LinkedProjectConfigService linkedProjectConfigService, Client client) {
        this.client = client;
        updateListener = new RemoteSettingsUpdateListener(settings);
        linkedProjectConfigService.register(updateListener);
        this.linkedProjectConfigService = linkedProjectConfigService;
    }

    @Override
    protected void doStart() {
        updateListener.init();
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

    private void putRepository(String repositoryName) {
        ActionRequest request = new PutInternalCcrRepositoryRequest(repositoryName, CcrRepository.TYPE);
        PlainActionFuture<ActionResponse.Empty> f = new PlainActionFuture<>();
        client.execute(PutInternalCcrRepositoryAction.INSTANCE, request, f);
        assert f.isDone() : "Should be completed as it is executed synchronously";
    }

    private void deleteRepository(String repositoryName) {
        DeleteInternalCcrRepositoryRequest request = new DeleteInternalCcrRepositoryRequest(repositoryName);
        PlainActionFuture<ActionResponse.Empty> f = new PlainActionFuture<>();
        client.execute(DeleteInternalCcrRepositoryAction.INSTANCE, request, f);
        assert f.isDone() : "Should be completed as it is executed synchronously";
    }

    private class RemoteSettingsUpdateListener extends RemoteClusterAware {

        private RemoteSettingsUpdateListener(Settings settings) {
            super(settings);
        }

        void init() {
            for (var config : linkedProjectConfigService.getInitialLinkedProjectConfigs()) {
                putRepository(CcrRepository.NAME_PREFIX + config.linkedProjectAlias());
            }
        }

        @Override
        public void updateLinkedProject(LinkedProjectConfig config) {
            String repositoryName = CcrRepository.NAME_PREFIX + config.linkedProjectAlias();
            if (config.isConnectionEnabled()) {
                putRepository(repositoryName);
            } else {
                deleteRepository(repositoryName);
            }
        }
    }
}
