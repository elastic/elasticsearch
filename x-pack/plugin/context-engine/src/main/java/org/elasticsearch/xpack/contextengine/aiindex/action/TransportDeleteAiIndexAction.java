/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.contextengine.aiindex.AiIndexRegistryService;

public class TransportDeleteAiIndexAction extends HandledTransportAction<DeleteAiIndexAction.Request, AcknowledgedResponse> {

    private final AiIndexRegistryService registryService;

    @Inject
    public TransportDeleteAiIndexAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            DeleteAiIndexAction.NAME,
            transportService,
            actionFilters,
            DeleteAiIndexAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.registryService = new AiIndexRegistryService(client);
    }

    @Override
    protected void doExecute(Task task, DeleteAiIndexAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        registryService.delete(request.space(), request.id(), listener.map(ignored -> AcknowledgedResponse.TRUE));
    }
}
