/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportGetEngineAction extends HandledTransportAction<GetEngineAction.Request, GetEngineAction.Response> {

    private final Client client;

    @Inject
    public TransportGetEngineAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetEngineAction.NAME, transportService, actionFilters, GetEngineAction.Request::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetEngineAction.Request request, ActionListener<GetEngineAction.Response> listener) {
        // Hi Carlos
    }

}
