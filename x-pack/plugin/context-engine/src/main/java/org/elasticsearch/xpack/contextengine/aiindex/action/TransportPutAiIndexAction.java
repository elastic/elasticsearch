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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.contextengine.aiindex.AiIndexRegistryService;

public class TransportPutAiIndexAction extends HandledTransportAction<PutAiIndexAction.Request, PutAiIndexAction.Response> {

    private final AiIndexRegistryService registryService;

    @Inject
    public TransportPutAiIndexAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(PutAiIndexAction.NAME, transportService, actionFilters, PutAiIndexAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.registryService = new AiIndexRegistryService(client);
    }

    @Override
    protected void doExecute(Task task, PutAiIndexAction.Request request, ActionListener<PutAiIndexAction.Response> listener) {
        registryService.put(
            request.id(),
            request.aiIndex(),
            request.create(),
            listener.map(stored -> new PutAiIndexAction.Response(stored.v1(), stored.v2()))
        );
    }
}
