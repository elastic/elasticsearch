/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.ConnectorIndexService;

public class TransportGetConnectorAction extends TransportAction<GetConnectorAction.Request, GetConnectorAction.Response> {
    protected final ConnectorIndexService connectorIndexService;

    @Inject
    public TransportGetConnectorAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetConnectorAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.connectorIndexService = new ConnectorIndexService(client);
    }

    @Override
    protected void doExecute(Task task, GetConnectorAction.Request request, ActionListener<GetConnectorAction.Response> listener) {
        connectorIndexService.getConnector(
            request.getConnectorId(),
            request.getIncludeDeleted(),
            listener.map(GetConnectorAction.Response::new)
        );
    }
}
