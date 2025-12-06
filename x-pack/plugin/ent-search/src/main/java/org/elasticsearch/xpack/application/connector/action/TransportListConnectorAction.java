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
import org.elasticsearch.xpack.core.action.util.PageParams;

public class TransportListConnectorAction extends TransportAction<ListConnectorAction.Request, ListConnectorAction.Response> {
    protected final ConnectorIndexService connectorIndexService;

    @Inject
    public TransportListConnectorAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(ListConnectorAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.connectorIndexService = new ConnectorIndexService(client);
    }

    @Override
    protected void doExecute(Task task, ListConnectorAction.Request request, ActionListener<ListConnectorAction.Response> listener) {
        final PageParams pageParams = request.getPageParams();

        connectorIndexService.listConnectors(
            pageParams.getFrom(),
            pageParams.getSize(),
            request.getIndexNames(),
            request.getConnectorNames(),
            request.getConnectorServiceTypes(),
            request.getConnectorSearchQuery(),
            request.getIncludeDeleted(),

            listener.map(r -> new ListConnectorAction.Response(r.connectors(), r.totalResults()))
        );
    }

}
